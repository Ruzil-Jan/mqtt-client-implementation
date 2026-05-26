# Приложение Б — Исходный код MQTT-клиента, WebSocket-обработчиков и JavaScript-клиента

## Содержание

1. [MQTT-клиент Flask-приложения (`app.py`)](#1-mqtt-клиент-flask-приложения-apppy)
2. [WebSocket-обработчики (`app.py`)](#2-websocket-обработчики-apppy)
3. [JavaScript-клиент (`static/js/main.js`)](#3-javascript-клиент-staticjsmainjs)
4. [Модуль базы данных (`db.py`)](#4-модуль-базы-данных-dbpy)

---

## 1. MQTT-клиент Flask-приложения (`app.py`)

### Инициализация и конфигурация

```python
import paho.mqtt.client as mqtt
import os, json, datetime, threading

MQTT_BROKER = os.getenv('MQTT_BROKER', 'localhost')
MQTT_PORT   = int(os.getenv('MQTT_PORT', 1883))
MQTT_KEEPALIVE = 60

mqtt_client    = mqtt.Client(mqtt.CallbackAPIVersion.V1)
mqtt_connected = False
```

Адрес брокера берётся из переменных окружения — при отсутствии `.env` используются значения по умолчанию `localhost:1883`.

---

### Callback: подключение к брокеру

```python
def on_mqtt_connect(client, userdata, flags, rc):
    global mqtt_connected
    if rc == 0:
        mqtt_connected = True
        print("[MQTT] ✓ Подключено к брокеру")
        # Подписка на статусные сообщения от всех устройств
        mqtt_client.subscribe("home/devices/+/status", qos=1)
        # Уведомить все браузеры об успешном подключении
        socketio.emit('mqtt_status', {'connected': True}, broadcast=True)
    else:
        print(f"[MQTT] ✗ Ошибка подключения: {rc}")
```

Код возврата `rc == 0` означает успешное подключение. Подстановочный символ `+` в топике `home/devices/+/status` подписывает клиент на сообщения от всех устройств одновременно.

---

### Callback: разрыв соединения

```python
def on_mqtt_disconnect(client, userdata, rc):
    global mqtt_connected
    mqtt_connected = False
    if rc != 0:
        print(f"[MQTT] Неожиданное отключение: {rc}")
    # Уведомить браузеры о потере MQTT-соединения
    socketio.emit('mqtt_status', {'connected': False}, broadcast=True)
```

При ненулевом `rc` разрыв произошёл неожиданно (сбой сети, перезапуск брокера). Paho-MQTT автоматически пытается переподключиться при использовании `loop_forever()`.

---

### Callback: входящее сообщение от устройства

```python
def on_mqtt_message(client, userdata, msg):
    """Получение обновления состояния от IoT-устройства."""
    try:
        # Разбор топика: home/devices/{id}/status
        parts = msg.topic.split('/')
        if len(parts) < 4 or parts[3] != 'status':
            return

        device_id = int(parts[2])
        payload   = json.loads(msg.payload.decode())

        # Нормализация состояния: принимаем "on"/"true"/"1"/True как включено
        state  = payload.get('state', 'off')
        status = 1 if state in ['on', 'true', '1', True] else 0

        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT id FROM bulbs WHERE id = ?", (device_id,))
        if c.fetchone():
            # Сохранить весь payload как extra-данные (температура и т.д.)
            extra_json = json.dumps(payload)
            c.execute(
                "UPDATE bulbs SET status = ?, extra = ? WHERE id = ?",
                (status, extra_json, device_id)
            )
            conn.commit()

            # Разослать обновление всем подключённым браузерам через WebSocket
            socketio.emit('device_update', {
                'device_id': device_id,
                'status':    status,
                'data':      payload
            }, broadcast=True)

        conn.close()
    except Exception as e:
        print(f"[MQTT] Ошибка обработки сообщения: {e}")
```

---

### Отправка команды устройству

Вызывается внутри маршрута `/toggle/<id>` после обновления состояния в БД:

```python
if mqtt_connected:
    command = {
        "action":    "on" if new_status else "off",
        "timestamp": datetime.datetime.now().isoformat()
    }
    mqtt_client.publish(
        f"home/devices/{device_id}/command",
        json.dumps(command),
        qos=1
    )
```

QoS=1 гарантирует доставку команды как минимум один раз — сообщение не потеряется при кратковременном разрыве.

---

### Привязка callbacks и запуск в отдельном потоке

```python
mqtt_client.on_connect    = on_mqtt_connect
mqtt_client.on_disconnect = on_mqtt_disconnect
mqtt_client.on_message    = on_mqtt_message

def mqtt_thread_function():
    """Фоновый поток MQTT-клиента."""
    global mqtt_connected
    try:
        mqtt_client.connect(MQTT_BROKER, MQTT_PORT, MQTT_KEEPALIVE)
        mqtt_client.loop_forever()   # блокирующий цикл с авто-переподключением
    except Exception as e:
        print(f"[MQTT] Ошибка подключения: {e}")
        mqtt_connected = False

# Точка входа
if __name__ == "__main__":
    init_db()

    mqtt_thread = threading.Thread(target=mqtt_thread_function, daemon=True)
    mqtt_thread.start()
    print("[APP] MQTT поток запущен")

    time.sleep(1)  # дать время MQTT-потоку установить соединение

    socketio.run(
        app,
        host="0.0.0.0",
        port=5000,
        debug=False,
        use_reloader=False,
        allow_unsafe_werkzeug=True
    )
```

`daemon=True` гарантирует завершение потока при остановке основного процесса. `loop_forever()` содержит встроенный механизм переподключения.

---

## 2. WebSocket-обработчики (`app.py`)

Flask-SocketIO инициализируется с поддержкой многопоточности:

```python
from flask_socketio import SocketIO, emit

socketio = SocketIO(
    app,
    cors_allowed_origins="*",
    async_mode='threading',
    ping_timeout=60,
    ping_interval=25
)

connected_clients = set()
```

---

### Событие: клиент подключился

```python
@socketio.on('connect')
def handle_connect():
    connected_clients.add(request.sid)
    print("[WebSocket] ✓ Клиент подключился")
    # Сообщить браузеру текущий статус MQTT-соединения
    emit('connection_response', {
        'status':         'connected',
        'mqtt_connected': mqtt_connected
    })
```

При подключении браузер сразу узнаёт, есть ли активное соединение с MQTT-брокером.

---

### Событие: клиент отключился

```python
@socketio.on('disconnect')
def handle_disconnect():
    connected_clients.discard(request.sid)
    print("[WebSocket] Клиент отключился")
```

---

### Событие: запрос полного списка устройств

```python
@socketio.on('request_status')
def handle_request_status():
    """Клиент запрашивает актуальное состояние всех устройств."""
    devices = load_devices()
    emit('full_status', {
        'devices':        devices,
        'mqtt_connected': mqtt_connected
    })
```

Используется при первой загрузке страницы или после восстановления соединения.

---

### Событие: переключение устройства через WebSocket

```python
@socketio.on('toggle_device')
def handle_toggle_device(data):
    """Переключение состояния устройства без HTTP-запроса."""
    device_id = data.get('device_id')
    if not device_id:
        return

    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
        "SELECT id, status, device_type FROM bulbs WHERE id = ?",
        (device_id,)
    )
    row = c.fetchone()

    if row:
        status      = row["status"]
        device_type = row["device_type"]
        cfg         = DEVICE_CONFIG.get(device_type, {})

        if cfg.get("has_toggle", False):
            new_status = 1 - status
            c.execute(
                "UPDATE bulbs SET status = ? WHERE id = ?",
                (new_status, device_id)
            )
            conn.commit()

            # Отправить команду IoT-устройству через MQTT
            if mqtt_connected:
                command = {
                    "action":    "on" if new_status else "off",
                    "timestamp": datetime.datetime.now().isoformat()
                }
                mqtt_client.publish(
                    f"home/devices/{device_id}/command",
                    json.dumps(command),
                    qos=1
                )

            # Уведомить все браузеры об изменении состояния
            emit('device_toggled', {
                'device_id':  device_id,
                'new_status': new_status
            }, broadcast=True)

    conn.close()
```

Событие `broadcast=True` отправляет обновление всем подключённым клиентам — если один пользователь переключает устройство, остальные видят изменение мгновенно.

---

## 3. JavaScript-клиент (`static/js/main.js`)

### Переключение состояния устройства (HTTP)

```javascript
function toggleDevice(id, button) {
    const card   = button.closest('.bulb-card');
    const img    = card.querySelector('.bulb-img');
    const status = card.querySelector('.status');

    fetch(`/toggle/${id}`)
        .then(r => r.json())
        .then(data => {
            if (data.error) {
                alert(data.error);
                return;
            }
            // Обновить текст статуса и цвет
            status.textContent = data.status_text || (data.status ? 'Включено' : 'Выключено');
            status.style.color = data.status ? '#4CAF50' : '#000000';

            // Обновить иконку устройства
            if (data.icon) {
                img.src = '/static/assets/' + data.icon;
            }

            // Обновить текст кнопки
            if (data.toggle_text) {
                button.textContent = data.toggle_text;
            }
        });
}
```

Запрос к `/toggle/{id}` — GET-запрос, возвращающий JSON с новым состоянием. Страница не перезагружается.

---

### Открытие модального окна с информацией об устройстве

```javascript
function openDeviceModal(id) {
    currentDeviceId = id;

    fetch(`/device/${id}`)
        .then(r => r.json())
        .then(data => {
            const device = data.device;
            const cfg    = data.config || {};
            const fields = cfg.fields || [];

            document.getElementById('device-modal-title').textContent = device.name;

            let html = `
                <p><strong>Тип:</strong> ${cfg.name || device.device_type}</p>
                <p><strong>IP:</strong> ${device.ip}</p>
                <div class="device-fields">
            `;

            fields.forEach(field => {
                if (field.key === 'location') return;

                const key   = field.key;
                const label = field.label || key;
                const type  = field.type || 'text';
                const value = (device.extra && device.extra[key] !== undefined)
                    ? device.extra[key]
                    : (field.default || '');

                if (type === 'textarea') {
                    html += `
                        <label class="field-label">${label}</label>
                        <textarea class="field-input" data-extra-key="${key}" disabled>${value}</textarea>
                    `;
                } else {
                    html += `
                        <label class="field-label">${label}</label>
                        <input class="field-input" type="${type}" value="${value}"
                               data-extra-key="${key}" disabled>
                    `;
                }
            });

            html += `</div>`;
            document.getElementById('device-modal-body').innerHTML = html;
            document.getElementById('modal-device').classList.add('show');
        });
}

function closeDeviceModal() {
    currentDeviceId = null;
    document.getElementById('modal-device').classList.remove('show');
}
```

Дополнительные поля (`fields` из `devices.json`) рендерятся динамически — каждый тип устройства отображает свои параметры (температура, зона полива, URL потока и т.д.).

---

### Добавление нового устройства

```javascript
function addDeviceManual() {
    const name = document.getElementById('new-name').value.trim();
    const ip   = document.getElementById('new-ip').value.trim();
    const type = document.getElementById('new-type').value;

    if (!name || !ip || !type) {
        return alert("Заполните все поля и выберите тип устройства!");
    }

    fetch('/add', {
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body: `name=${encodeURIComponent(name)}&ip=${encodeURIComponent(ip)}&device_type=${encodeURIComponent(type)}`
    })
    .then(r => r.json())
    .then(data => {
        if (data.success) {
            location.reload();
        } else {
            alert(data.message || "Ошибка при добавлении устройства");
        }
    });
}
```

При дублировании IP сервер возвращает `{"success": false, "message": "Устройство с таким IP уже существует!"}` — ограничение обеспечивается `UNIQUE` на поле `ip` в SQLite.

---

### Удаление устройства

```javascript
function deleteCurrentDevice() {
    if (!currentDeviceId) return;
    if (!confirm("Удалить это устройство?")) return;

    fetch(`/delete/${currentDeviceId}`, { method: 'POST' })
        .then(r => r.json())
        .then(data => {
            if (data.success) {
                location.reload();
            } else {
                alert("Не удалось удалить устройство");
            }
        });
}
```

---

### Управление панелью и модальными окнами

```javascript
let currentDeviceId = null;

function toggleManagePanel(show) {
    const panel   = document.getElementById('manage-panel');
    const mainBtn = document.getElementById('manage-toggle-btn');
    if (!panel || !mainBtn) return;

    const isVisible = panel.style.display === 'flex';
    const next      = (show !== undefined) ? show : !isVisible;

    if (next) {
        panel.style.display = 'flex';
        mainBtn.style.display = 'none';
    } else {
        panel.style.display = 'none';
        mainBtn.style.display = 'inline-block';
    }
}

function openManualModal() {
    document.getElementById('modal-add').classList.add('show');
}

function closeManualModal() {
    document.getElementById('modal-add').classList.remove('show');
}

// Закрытие модальных окон по клику на фон
window.onclick = function(e) {
    const modalAdd    = document.getElementById('modal-add');
    const modalDevice = document.getElementById('modal-device');
    if (modalAdd    && e.target === modalAdd)    closeManualModal();
    if (modalDevice && e.target === modalDevice) closeDeviceModal();
};
```

---

## 4. Модуль базы данных (`db.py`)

```python
import sqlite3

DB_NAME = "db.sqlite"


def get_db_connection():
    conn = sqlite3.connect(DB_NAME, timeout=1.0)
    conn.row_factory = sqlite3.Row   # доступ к полям по имени: row["name"]

    # WAL-режим: защита от потери данных при внезапном отключении питания.
    # Критично при работе на SD-карте или eMMC Android-устройства.
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")

    return conn


def init_db():
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS bulbs (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            name        TEXT    NOT NULL,
            device_type TEXT    NOT NULL,
            ip          TEXT    NOT NULL UNIQUE,
            status      INTEGER DEFAULT 0,
            extra       TEXT
        )
    """)
    conn.commit()
    conn.close()
```

`PRAGMA synchronous=NORMAL` — компромисс между скоростью и надёжностью: данные не теряются при сбое приложения, но могут потеряться при внезапном отключении питания без WAL. В связке с `journal_mode=WAL` обеспечивает полную защиту данных.
