import asyncio
import sqlite3
import threading
from contextlib import asynccontextmanager
from typing import List

import snap7
from snap7.util import get_real, get_bool, set_real, set_bool

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse


# ============================================================
# CONFIGURATION
# ============================================================

PLC_IP = "192.168.0.1"
PLC_RACK = 0
PLC_SLOT = 1

DB_NUMBER = 100
DB_SIZE = 53
POLL_INTERVAL = 0.5          # seconds

SQLITE_DATABASE = "pid_history.db"


# ============================================================
# DB100 MEMORY LAYOUT  (ADJUST THESE OFFSETS IF NEEDED)
# ============================================================
# Real values = 4 bytes each
OFFSETS = {
    "xv101": 0,
    "xv102": 4,
    "xv103": 8,
    "xv104": 12,
    "xv101_sp": 16,
    "xv102_sp": 20,
    "xv103_sp": 24,
    "xv104_sp": 28,
    "level": 32,
    "level_sp": 36,
    "mixer": 40,
    "mixer_sp": 44,
    "temp": 48,
}

# Byte 52 → bits for digital signals
BOOL_BYTE = 52
BIT_P101 = 0
BIT_P102 = 1
BIT_S101 = 2


# ============================================================
# PLC CLIENT
# ============================================================

plc = snap7.client.Client()
plc_lock = threading.Lock()


# ============================================================
# DATABASE
# ============================================================

def init_database():
    connection = sqlite3.connect(SQLITE_DATABASE)
    cursor = connection.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS process_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            xv101 REAL, xv102 REAL, xv103 REAL, xv104 REAL,
            xv101_sp REAL, xv102_sp REAL, xv103_sp REAL, xv104_sp REAL,
            level REAL, level_sp REAL,
            mixer_speed REAL, mixer_speed_sp REAL,
            temperature REAL,
            mixer_sns INTEGER, p101 INTEGER, p102 INTEGER
        )
    """)
    connection.commit()
    connection.close()


def log_process_data(data: dict):
    connection = sqlite3.connect(SQLITE_DATABASE)
    cursor = connection.cursor()
    cursor.execute("""
        INSERT INTO process_history (
            xv101, xv102, xv103, xv104,
            xv101_sp, xv102_sp, xv103_sp, xv104_sp,
            level, level_sp, mixer_speed, mixer_speed_sp,
            temperature, mixer_sns, p101, p102
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        data["xv101"], data["xv102"], data["xv103"], data["xv104"],
        data["xv101_sp"], data["xv102_sp"], data["xv103_sp"], data["xv104_sp"],
        data["level"], data["level_sp"],
        data["mixer"], data["mixer_sp"],
        data["temp"],
        int(data["s101"]), int(data["p101"]), int(data["p102"]),
    ))
    connection.commit()
    connection.close()


# ============================================================
# PLC CONNECTION HELPERS
# ============================================================

def connect_plc():
    with plc_lock:
        if plc.get_connected():
            return
        print(f"Connecting to PLC {PLC_IP}...")
        plc.connect(PLC_IP, PLC_RACK, PLC_SLOT)
        if not plc.get_connected():
            raise ConnectionError("Could not connect to PLC.")
        print("PLC connected.")


def disconnect_plc():
    with plc_lock:
        if plc.get_connected():
            plc.disconnect()
            print("PLC disconnected.")


# ============================================================
# READ / WRITE DB100
# ============================================================

def read_process_data() -> dict:
    with plc_lock:
        if not plc.get_connected():
            plc.connect(PLC_IP, PLC_RACK, PLC_SLOT)

        raw = plc.db_read(DB_NUMBER, 0, DB_SIZE)

        data = {
            "xv101": round(get_real(raw, OFFSETS["xv101"]), 1),
            "xv102": round(get_real(raw, OFFSETS["xv102"]), 1),
            "xv103": round(get_real(raw, OFFSETS["xv103"]), 1),
            "xv104": round(get_real(raw, OFFSETS["xv104"]), 1),

            "xv101_sp": round(get_real(raw, OFFSETS["xv101_sp"]), 1),
            "xv102_sp": round(get_real(raw, OFFSETS["xv102_sp"]), 1),
            "xv103_sp": round(get_real(raw, OFFSETS["xv103_sp"]), 1),
            "xv104_sp": round(get_real(raw, OFFSETS["xv104_sp"]), 1),

            "level": round(get_real(raw, OFFSETS["level"]), 1),
            "level_sp": round(get_real(raw, OFFSETS["level_sp"]), 1),

            "mixer": round(get_real(raw, OFFSETS["mixer"]), 0),
            "mixer_sp": round(get_real(raw, OFFSETS["mixer_sp"]), 0),

            "temp": round(get_real(raw, OFFSETS["temp"]), 1),

            "p101": get_bool(raw, BOOL_BYTE, BIT_P101),
            "p102": get_bool(raw, BOOL_BYTE, BIT_P102),
            "s101": get_bool(raw, BOOL_BYTE, BIT_S101),
        }
        return data


def write_real(offset: int, value: float):
    with plc_lock:
        if not plc.get_connected():
            plc.connect(PLC_IP, PLC_RACK, PLC_SLOT)
        raw = bytearray(4)
        set_real(raw, 0, value)
        plc.db_write(DB_NUMBER, offset, raw)


def write_bool(byte_offset: int, bit: int, value: bool):
    with plc_lock:
        if not plc.get_connected():
            plc.connect(PLC_IP, PLC_RACK, PLC_SLOT)
        raw = plc.db_read(DB_NUMBER, byte_offset, 1)
        set_bool(raw, 0, bit, value)
        plc.db_write(DB_NUMBER, byte_offset, raw)


# ============================================================
# WEBSOCKET MANAGER
# ============================================================

class ConnectionManager:
    def __init__(self):
        self.active: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active:
            self.active.remove(websocket)

    async def broadcast(self, data: dict):
        dead = []
        for ws in self.active:
            try:
                await ws.send_json(data)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws)


manager = ConnectionManager()


# ============================================================
# BACKGROUND POLLING TASK
# ============================================================

async def poll_plc():
    while True:
        try:
            data = read_process_data()
            log_process_data(data)
            await manager.broadcast(data)
        except Exception as e:
            print(f"PLC read error: {e}")
        await asyncio.sleep(POLL_INTERVAL)


# ============================================================
# FASTAPI APP
# ============================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_database()
    connect_plc()
    print("PLC connection ready.")
    print(f"PLC IP : {PLC_IP}")
    print(f"DB     : {DB_NUMBER}")
    task = asyncio.create_task(poll_plc())
    yield
    task.cancel()
    disconnect_plc()


app = FastAPI(title="PID 3D SCADA BACKEND", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def root():
    return {
        "status": "online",
        "application": "PID 3D SCADA HMI",
        "plc": PLC_IP,
        "db": DB_NUMBER,
    }


@app.get("/api/live")
def get_live():
    try:
        return read_process_data()
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            # Keep the connection alive. We only send data from the poll task.
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)


# ---------- COMMANDS FROM 3D HMI ----------

@app.post("/api/pump/{pump_id}")
async def set_pump(pump_id: str, on: bool):
    if pump_id == "p101":
        write_bool(BOOL_BYTE, BIT_P101, on)
    elif pump_id == "p102":
        write_bool(BOOL_BYTE, BIT_P102, on)
    else:
        return JSONResponse(status_code=400, content={"error": "Unknown pump"})
    return {"status": "ok", "pump": pump_id, "on": on}


@app.post("/api/valve/{valve_id}")
async def set_valve(valve_id: str, value: float):
    key = f"{valve_id}_sp"
    if key not in OFFSETS:
        return JSONResponse(status_code=400, content={"error": "Unknown valve"})
    write_real(OFFSETS[key], value)
    return {"status": "ok", "valve": valve_id, "sp": value}


@app.post("/api/mixer")
async def set_mixer(value: float):
    write_real(OFFSETS["mixer_sp"], value)
    return {"status": "ok", "mixer_sp": value}


@app.post("/api/level")
async def set_level(value: float):
    write_real(OFFSETS["level_sp"], value)
    return {"status": "ok", "level_sp": value}
