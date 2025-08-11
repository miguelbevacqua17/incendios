# server.py
# Backend FIRMS ARG con FastAPI + SQLite + CORS (listo para Codespaces)
# - /refresh: descarga CSV FIRMS (por sensor), normaliza y guarda en SQLite
# - /fires?window=today|24h|3d|7d: devuelve GeoJSON unificado desde SQLite
# Requisitos: fastapi, uvicorn, requests, python-dotenv (opcional)

import os
import io
import csv
import time
import json
import sqlite3
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

# -----------------------------
# Configuración (env)
# -----------------------------
FIRMS_MAP_KEY = os.getenv("FIRMS_MAP_KEY", "").strip()
if not FIRMS_MAP_KEY:
    raise RuntimeError(
        "FIRMS_MAP_KEY no está definido. En Codespaces, creá un Secret "
        "de Codespaces llamado FIRMS_MAP_KEY con tu MAP KEY de FIRMS."
    )

FRONTEND_ORIGIN = os.getenv("FRONTEND_ORIGIN")  # ej: https://<codespace>-8080.app.github.dev
COUNTRY = os.getenv("FIRMS_COUNTRY", "ARG").strip()
DAY_RANGE = int(os.getenv("FIRMS_DAY_RANGE", "7"))  # 1..10
CACHE_TTL_MIN = int(os.getenv("CACHE_TTL_MIN", "5"))
DB_PATH = os.getenv("DB_PATH", "fires.db")

DEFAULT_SENSORS = "VIIRS_SNPP_NRT,VIIRS_NOAA20_NRT,VIIRS_NOAA21_NRT"
SENSORS = [s.strip() for s in os.getenv("FIRMS_SENSORS", DEFAULT_SENSORS).split(",") if s.strip()]

TZ_AR = ZoneInfo("America/Argentina/Buenos_Aires")

# -----------------------------
# App + CORS
# -----------------------------
app = FastAPI(title="FIRMS ARG Fires API (SQLite)")

# CORS: si FRONTEND_ORIGIN está definido, permítelo explícitamente; si no, abre para pruebas
if FRONTEND_ORIGIN:
    allow_origins = [FRONTEND_ORIGIN]
    allow_credentials = False
else:
    # modo abierto de prueba (no usar con credenciales)
    allow_origins = ["*"]
    allow_credentials = False

app.add_middleware(
    CORSMiddleware,
    allow_origins=allow_origins,
    allow_credentials=allow_credentials,
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["*"],
)

# -----------------------------
# SQLite (esquema e utilidades)
# -----------------------------
CREATE_SQL = """
CREATE TABLE IF NOT EXISTS fires (
    id INTEGER PRIMARY KEY,
    source TEXT NOT NULL,
    latitude REAL NOT NULL,
    longitude REAL NOT NULL,
    acq_date TEXT NOT NULL,       -- YYYY-MM-DD
    acq_time TEXT NOT NULL,       -- HHMM (zfill 4)
    timestamp_utc TEXT NOT NULL,  -- ISO 8601 (Z)
    timestamp_local TEXT NOT NULL,-- ISO 8601 AR
    satellite TEXT,
    instrument TEXT,
    confidence TEXT,
    confidence_n REAL,
    frp REAL,
    version TEXT,
    daynight TEXT,
    extra_json TEXT,
    UNIQUE (source, latitude, longitude, acq_date, acq_time) ON CONFLICT IGNORE
);
CREATE INDEX IF NOT EXISTS idx_fires_utc ON fires(timestamp_utc);
CREATE INDEX IF NOT EXISTS idx_fires_local ON fires(timestamp_local);
CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT);
"""

def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn

@app.on_event("startup")
def startup():
    conn = get_conn()
    with conn:
        for stmt in CREATE_SQL.split(";"):
            if stmt.strip():
                conn.execute(stmt)

# -----------------------------
# Helpers
# -----------------------------
def to_timestamp(acq_date: str, acq_time: str) -> Tuple[str, str]:
    acq_time = (acq_time or "").zfill(4)
    hh, mm = acq_time[:2], acq_time[2:]
    dt_utc = datetime.strptime(f"{acq_date} {hh}:{mm}", "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
    dt_local = dt_utc.astimezone(TZ_AR)
    return dt_utc.isoformat().replace("+00:00", "Z"), dt_local.isoformat()

def confidence_to_float(val: Optional[str]) -> Optional[float]:
    if val is None:
        return None
    try:
        return float(val)
    except Exception:
        lookup = {
            "l": 20.0, "low": 20.0,
            "n": 60.0, "nominal": 60.0,
            "h": 90.0, "high": 90.0
        }
        return lookup.get(str(val).strip().lower())

def csv_to_rows(csv_text: str, source: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    reader = csv.DictReader(io.StringIO(csv_text))
    for r in reader:
        lat = r.get("latitude"); lon = r.get("longitude")
        acq_date = r.get("acq_date"); acq_time = r.get("acq_time")
        if not (lat and lon and acq_date and acq_time):
            continue
        try:
            latf = float(lat); lonf = float(lon)
        except Exception:
            continue

        ts_utc, ts_local = to_timestamp(acq_date, str(acq_time))
        conf = r.get("confidence")
        conf_n = confidence_to_float(conf)

        # Campos opcionales
        frp_val = None
        if r.get("frp") not in (None, "", "null"):
            try:
                frp_val = float(r.get("frp"))
            except Exception:
                frp_val = None

        extra = {}
        # Conservá cualquier campo adicional que quieras inspeccionar
        for k in ("bright_ti4", "bright_ti5", "scan", "track", "bright_t31"):
            if k in r and r[k] not in (None, ""):
                extra[k] = r[k]

        rows.append({
            "source": source,
            "latitude": latf,
            "longitude": lonf,
            "acq_date": acq_date,
            "acq_time": str(acq_time).zfill(4),
            "timestamp_utc": ts_utc,
            "timestamp_local": ts_local,
            "satellite": r.get("satellite"),
            "instrument": r.get("instrument"),
            "confidence": conf,
            "confidence_n": conf_n,
            "frp": frp_val,
            "version": r.get("version"),
            "daynight": r.get("daynight"),
            "extra_json": json.dumps(extra) if extra else None
        })
    return rows

def fetch_csv(source: str, country: str, day_range: int) -> str:
    url = f"https://firms.modaps.eosdis.nasa.gov/api/country/csv/{FIRMS_MAP_KEY}/{source}/{country}/{day_range}"
    resp = requests.get(url, timeout=60)
    if resp.status_code in (403, 429):
        # 403 puede ser limite excedido; 429 too many
        raise HTTPException(status_code=resp.status_code, detail=f"FIRMS error {resp.status_code}: {resp.text[:200]}")
    if resp.status_code >= 400:
        raise HTTPException(status_code=resp.status_code, detail=f"FIRMS error {resp.status_code}: {resp.text[:200]}")
    return resp.text

def upsert_rows(conn: sqlite3.Connection, rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    sql = """
    INSERT OR IGNORE INTO fires
    (source, latitude, longitude, acq_date, acq_time, timestamp_utc, timestamp_local,
     satellite, instrument, confidence, confidence_n, frp, version, daynight, extra_json)
    VALUES (:source, :latitude, :longitude, :acq_date, :acq_time, :timestamp_utc, :timestamp_local,
            :satellite, :instrument, :confidence, :confidence_n, :frp, :version, :daynight, :extra_json)
    """
    with conn:
        conn.executemany(sql, rows)
    return len(rows)

def set_meta(conn: sqlite3.Connection, k: str, v: str):
    with conn:
        conn.execute("INSERT OR REPLACE INTO meta(k, v) VALUES (?, ?)", (k, v))

def get_meta(conn: sqlite3.Connection, k: str) -> Optional[str]:
    row = conn.execute("SELECT v FROM meta WHERE k=?", (k,)).fetchone()
    return row[0] if row else None

def last_refresh_dt(conn: sqlite3.Connection) -> Optional[datetime]:
    v = get_meta(conn, "last_refresh_utc")
    if not v:
        return None
    try:
        return datetime.fromisoformat(v.replace("Z", "+00:00"))
    except Exception:
        return None

def window_to_bounds(window: str) -> Tuple[str, str, str]:
    now_utc = datetime.now(timezone.utc)
    if window == "24h":
        start = (now_utc - timedelta(hours=24)).isoformat().replace("+00:00", "Z")
        end = now_utc.isoformat().replace("+00:00", "Z")
        return ("timestamp_utc", start, end)
    elif window == "3d":
        start = (now_utc - timedelta(days=3)).isoformat().replace("+00:00", "Z")
        end = now_utc.isoformat().replace("+00:00", "Z")
        return ("timestamp_utc", start, end)
    elif window == "7d":
        start = (now_utc - timedelta(days=7)).isoformat().replace("+00:00", "Z")
        end = now_utc.isoformat().replace("+00:00", "Z")
        return ("timestamp_utc", start, end)
    elif window == "today":
        now_local = datetime.now(TZ_AR)
        start_local = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
        end_local = now_local
        return ("timestamp_local", start_local.isoformat(), end_local.isoformat())
    else:
        raise HTTPException(400, "window inválida. Usa: today | 24h | 3d | 7d")

# -----------------------------
# Endpoints
# -----------------------------
@app.get("/health")
def health():
    return {"ok": True, "time_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")}

@app.get("/refresh")
def refresh_data(
    sensors: Optional[str] = None,
    day_range: Optional[int] = None,
    pause_sec: int = 10
) -> Dict[str, Any]:
    """
    Descarga desde FIRMS para los sensores indicados (o los por defecto),
    normaliza y guarda en SQLite. Evita refrescar si se hizo hace < CACHE_TTL_MIN.
    """
    used_sensors = [s.strip() for s in (sensors or ",".join(SENSORS)).split(",") if s.strip()]
    dr = int(day_range or DAY_RANGE)
    if dr < 1 or dr > 10:
        raise HTTPException(400, "day_range debe ser entre 1 y 10.")

    conn = get_conn()
    # TTL lógico para evitar abusar del API
    lr = last_refresh_dt(conn)
    if lr and (datetime.now(timezone.utc) - lr) < timedelta(minutes=CACHE_TTL_MIN):
        return {
            "status": "skip",
            "reason": f"ya refrescado hace < {CACHE_TTL_MIN} min",
            "last_refresh_utc": lr.isoformat().replace("+00:00", "Z")
        }

    added_total = 0
    detail = []
    partial_error = None

    for i, src in enumerate(used_sensors):
        try:
            csv_text = fetch_csv(src, COUNTRY, dr)
            rows = csv_to_rows(csv_text, src)
            n = upsert_rows(conn, rows)
            added_total += n
            detail.append({"sensor": src, "rows_parsed": len(rows), "rows_inserted": n})
        except HTTPException as e:
            partial_error = {"sensor": src, "status": e.status_code, "detail": str(e.detail)}
            # Si es límite (403/429), cortamos para no agravar
            if e.status_code in (403, 429):
                break
            else:
                detail.append({"sensor": src, "error": partial_error})
        if i < len(used_sensors) - 1:
            time.sleep(max(0, pause_sec))

    now_iso = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    if added_total > 0:
        set_meta(conn, "last_refresh_utc", now_iso)

    resp = {
        "status": "ok" if added_total > 0 else "no_data",
        "inserted": added_total,
        "detail": detail,
        "last_refresh_utc": now_iso
    }
    if partial_error:
        resp["partial_error"] = partial_error
    return resp

@app.get("/fires")
def get_fires(
    window: str = Query("24h", pattern="^(today|24h|3d|7d)$"),
    limit: int = Query(50000, ge=1, le=200000)
) -> Dict[str, Any]:
    """
    Devuelve GeoJSON unificado desde SQLite según ventana temporal.
    """
    col, start_iso, end_iso = window_to_bounds(window)
    conn = get_conn()
    # ¿existe la tabla y hay datos?
    exists = conn.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='fires'").fetchone()[0]
    if not exists:
        raise HTTPException(503, "La base está vacía. Ejecutá /refresh primero.")

    sql = f"""
    SELECT source, latitude, longitude, acq_date, acq_time, timestamp_utc, timestamp_local,
           satellite, instrument, confidence, confidence_n, frp, version, daynight
    FROM fires
    WHERE {col} >= ? AND {col} <= ?
    ORDER BY timestamp_utc DESC
    LIMIT ?
    """
    rows = list(conn.execute(sql, (start_iso, end_iso, limit)))

    features = []
    last_update = None
    for r in rows:
        (source, lat, lon, acq_date, acq_time, ts_utc, ts_local,
         satellite, instrument, confidence, confidence_n, frp, version, daynight) = r
        if not last_update or (ts_local and ts_local > last_update):
            last_update = ts_local
        props = {
            "source": source,
            "acq_date": acq_date,
            "acq_time": acq_time,
            "timestamp_utc": ts_utc,
            "timestamp_local": ts_local,
            "satellite": satellite,
            "instrument": instrument,
            "confidence": confidence,
            "confidence_n": confidence_n,
            "frp": frp,
            "version": version,
            "daynight": daynight
        }
        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [lon, lat]},
            "properties": props
        })

    return {
        "type": "FeatureCollection",
        "features": features,
        "meta": {
            "count": len(features),
            "window": window,
            "last_update": last_update,
            "source": "NASA FIRMS (LANCE), NASA EOSDIS",
            "attribution_url": "https://firms.modaps.eosdis.nasa.gov/"
        }
    }
