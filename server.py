# server.py
# ETL + API FIRMS (Argentina) con FastAPI + SQLite + GeoJSON estático
# Fuentes: VIIRS_SNPP_NRT, VIIRS_NOAA20_NRT, VIIRS_NOAA21_NRT, MODIS_NRT
# Endpoints:
#   - GET /health
#   - GET /refresh?sensors=...&day_range=1..10
#   - GET /fires?window=today|24h|3d|7d
# Requisitos: fastapi, uvicorn, requests, python-dotenv (opcional)

import os
import io
import csv
import json
import time
import sqlite3
import asyncio
import logging
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

# =========================
# Logging
# =========================
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("firms-api")

# =========================
# Configuración (ENV)
# =========================
FIRMS_MAP_KEY = os.getenv("FIRMS_MAP_KEY", "").strip()
if not FIRMS_MAP_KEY:
    raise RuntimeError("FIRMS_MAP_KEY no está definido (agregá tu MAP KEY de FIRMS como variable de entorno).")

# Si servís el front en Codespaces (puerto 8080), setealo para CORS:
#   export FRONTEND_ORIGIN="https://<tu-codespace>-8080.app.github.dev"
FRONTEND_ORIGIN = os.getenv("FRONTEND_ORIGIN")  # opcional

COUNTRY = os.getenv("FIRMS_COUNTRY", "ARG").strip()
DAY_RANGE = int(os.getenv("FIRMS_DAY_RANGE", "3"))        # 1..10 (por defecto 3 días)
CACHE_TTL_MIN = int(os.getenv("CACHE_TTL_MIN", "5"))      # TTL lógico entre refresh
DB_PATH = os.getenv("DB_PATH", "fires.db")
OUT_DIR = os.getenv("OUT_DIR", ".")                       # donde escribir fires_*.json

# TODOS los satélites disponibles (MODIS incluido)
DEFAULT_SENSORS = "VIIRS_SNPP_NRT,VIIRS_NOAA20_NRT,VIIRS_NOAA21_NRT,MODIS_NRT"
SENSORS = [s.strip() for s in os.getenv("FIRMS_SENSORS", DEFAULT_SENSORS).split(",") if s.strip()]

TZ_AR = ZoneInfo("America/Argentina/Buenos_Aires")

# =========================
# HTTP Session con reintentos
# =========================
_session = requests.Session()
_retry = Retry(
    total=5,
    connect=3,
    read=3,
    status=5,
    status_forcelist=(403, 429, 500, 502, 503, 504),
    allowed_methods=frozenset(["GET"]),
    backoff_factor=2,  # 0, 2, 4, 8, 16s...
    respect_retry_after_header=True,
)
_adapter = HTTPAdapter(max_retries=_retry, pool_connections=10, pool_maxsize=20)
_session.mount("https://", _adapter)
_session.mount("http://", _adapter)
_session.headers.update({"User-Agent": "FIRMS-ARG-ETL/1.0 (+FastAPI)"})


# =========================
# SQLite (esquema)
# =========================
CREATE_SQL = """
CREATE TABLE IF NOT EXISTS fires (
  id INTEGER PRIMARY KEY,
  source TEXT NOT NULL,
  latitude REAL NOT NULL,
  longitude REAL NOT NULL,
  acq_date TEXT NOT NULL,
  acq_time TEXT NOT NULL,          -- HHMM
  timestamp_utc TEXT NOT NULL,     -- ISO 8601 Z
  timestamp_local TEXT NOT NULL,   -- ISO 8601 AR
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
CREATE INDEX IF NOT EXISTS idx_fires_utc   ON fires(timestamp_utc);
CREATE INDEX IF NOT EXISTS idx_fires_local ON fires(timestamp_local);
CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT);
"""

def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn

def ensure_schema(conn: sqlite3.Connection):
    with conn:
        for stmt in CREATE_SQL.split(";"):
            if stmt.strip():
                conn.execute(stmt)

# =========================
# Utilidades de tiempo / parsing
# =========================
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
        lookup = {"l": 20.0, "low": 20.0, "n": 60.0, "nominal": 60.0, "h": 90.0, "high": 90.0}
        return lookup.get(str(val).strip().lower())

def window_to_bounds(window: str) -> Tuple[str, str, str]:
    now_utc = datetime.now(timezone.utc)
    if window == "24h":
        return ("timestamp_utc",
                (now_utc - timedelta(hours=24)).isoformat().replace("+00:00","Z"),
                now_utc.isoformat().replace("+00:00","Z"))
    elif window == "3d":
        return ("timestamp_utc",
                (now_utc - timedelta(days=3)).isoformat().replace("+00:00","Z"),
                now_utc.isoformat().replace("+00:00","Z"))
    elif window == "7d":
        return ("timestamp_utc",
                (now_utc - timedelta(days=7)).isoformat().replace("+00:00","Z"),
                now_utc.isoformat().replace("+00:00","Z"))
    elif window == "today":
        now_local = datetime.now(TZ_AR)
        start_local = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
        return ("timestamp_local", start_local.isoformat(), now_local.isoformat())
    else:
        raise ValueError("window inválida (usa: today | 24h | 3d | 7d)")

# =========================
# Meta (último refresh)
# =========================
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

# =========================
# ETL: descarga FIRMS → normaliza → SQLite
# =========================
def fetch_csv(source: str, country: str, day_range: int, retries: int = 4, backoff_sec: int = 20) -> str:
    url = f"https://firms.modaps.eosdis.nasa.gov/api/country/csv/{FIRMS_MAP_KEY}/{source}/{country}/{day_range}"
    attempt = 0
    while True:
        try:
            # timeout=(connect, read)
            r = _session.get(url, timeout=(10, 60))
        except requests.exceptions.Timeout as e:
            attempt += 1
            if attempt > retries:
                raise RuntimeError(f"FIRMS timeout tras {retries} reintentos: {e}")
            wait = backoff_sec * attempt
            log.warning(f"{source} timeout. Reintentando en {wait}s (intento {attempt}/{retries})")
            time.sleep(wait)
            continue
        except requests.exceptions.RequestException as e:
            attempt += 1
            if attempt > retries:
                raise RuntimeError(f"FIRMS error de red tras {retries} reintentos: {e}")
            wait = backoff_sec * attempt
            log.warning(f"{source} error de red. Reintentando en {wait}s (intento {attempt}/{retries})")
            time.sleep(wait)
            continue

        if r.status_code in (403, 429, 500, 502, 503, 504):
            attempt += 1
            if attempt > retries:
                raise RuntimeError(f"FIRMS {r.status_code}: {r.text[:200]}")
            wait = backoff_sec * attempt
            log.warning(f"{source} {r.status_code}. Reintentando en {wait}s (intento {attempt}/{retries})")
            time.sleep(wait)
            continue

        if r.status_code >= 400:
            # otros 4xx: no reintentar
            raise RuntimeError(f"FIRMS {r.status_code}: {r.text[:200]}")

        return r.text

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
        frp_val = None
        if r.get("frp") not in (None, "", "null"):
            try:
                frp_val = float(r.get("frp"))
            except Exception:
                frp_val = None
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
            "extra_json": None
        })
    return rows

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

def refresh_all(
    conn: sqlite3.Connection,
    sensors: List[str],
    day_range: int,
    pause_sec: int = 10,
    dump_after_each: bool = False,
    log_progress: bool = False,
    prioritize_modis: bool = True,
) -> Dict[str, Any]:
    ensure_schema(conn)
    # TTL (para no abusar)
    lr = last_refresh_dt(conn)
    now_utc_dt = datetime.now(timezone.utc)
    now_iso = now_utc_dt.isoformat().replace("+00:00","Z")
    set_meta(conn, "last_attempt_utc", now_iso)

    if lr and (now_utc_dt - lr) < timedelta(minutes=CACHE_TTL_MIN):
        return {"status": "skip", "reason": f"ya refrescado hace < {CACHE_TTL_MIN} min", "last_refresh_utc": lr.isoformat().replace("+00:00","Z")}

    # Prioridad: MODIS primero si está en la lista
    ordered = sensors[:]
    if prioritize_modis and "MODIS_NRT" in ordered:
        ordered = ["MODIS_NRT"] + [s for s in ordered if s != "MODIS_NRT"]

    total = len(ordered)
    completed: List[str] = []
    added_total = 0
    detail = []

    log.info(f"[ETL] Comenzando refresh: sensores={ordered}, day_range={day_range}")

    for i, s in enumerate(ordered, start=1):
        pending = ordered[i:]  # lo que queda a futuro
        if log_progress:
            log.info(f"[ETL][{i}/{total}] {s} → descargando (day_range={day_range}) | pendientes: {pending or 'ninguno'}")

        try:
            txt = fetch_csv(s, COUNTRY, day_range)
            rows = csv_to_rows(txt, s)
            n = upsert_rows(conn, rows)
            added_total += n
            completed.append(s)
            detail.append({"sensor": s, "rows_parsed": len(rows), "rows_inserted": n})
            log.info(f"[ETL][{s}] ok parsed={len(rows)} inserted={n}")

            # Dump parcial: si insertó algo, actualizamos archivos para que el front vea progreso
            if dump_after_each and n > 0:
                try:
                    dump_all_geojson(conn, OUT_DIR)
                    log.info(f"[ETL][{s}] GeoJSON actualizado (dump parcial).")
                except Exception as e:
                    log.warning(f"[dump-parcial][{s}] {e}")

        except RuntimeError as e:
            detail.append({"sensor": s, "error": str(e)})
            log.warning(f"[ETL][{s}] {e}")
        except Exception as e:
            detail.append({"sensor": s, "error": f"unexpected: {e}"})
            log.exception(f"[ETL][{s}] unexpected error")

        # Informe de progreso luego de cada sensor
        if log_progress:
            log.info(f"[ETL] completados: {completed or ['ninguno']} | pendientes: {ordered[i:] or ['ninguno']}")

        if i < total:
            time.sleep(max(0, pause_sec))

    # Sólo sellamos last_refresh_utc si hubo inserciones
    if added_total > 0:
        set_meta(conn, "last_refresh_utc", now_iso)

    return {
        "status": "ok" if added_total > 0 else "no_data",
        "inserted": added_total,
        "detail": detail,
        "last_refresh_utc": get_meta(conn, "last_refresh_utc"),
        "completed": completed,
        "pending": [s for s in ordered if s not in completed],
    }

# =========================
# Export a GeoJSON estático
# =========================
def dump_geojson(conn: sqlite3.Connection, window: str, out_path: str) -> Dict[str, Any]:
    col, start_iso, end_iso = window_to_bounds(window)
    sql = f"""
    SELECT source, latitude, longitude, acq_date, acq_time, timestamp_utc, timestamp_local,
           satellite, instrument, confidence, confidence_n, frp, version, daynight
    FROM fires
    WHERE {col} >= ? AND {col} <= ?
    ORDER BY timestamp_utc DESC
    """
    rows = list(conn.execute(sql, (start_iso, end_iso)))
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

    geo = {
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
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(geo, f, ensure_ascii=False)
    return geo

def dump_all_geojson(conn: sqlite3.Connection, out_dir: str = OUT_DIR) -> Dict[str, Dict[str, Any]]:
    outputs = {}
    outputs["today"] = dump_geojson(conn, "today", os.path.join(out_dir, "fires_today.json"))
    outputs["24h"]  = dump_geojson(conn, "24h",  os.path.join(out_dir, "fires_24h.json"))
    outputs["3d"]   = dump_geojson(conn, "3d",   os.path.join(out_dir, "fires_3d.json"))
    outputs["7d"]   = dump_geojson(conn, "7d",   os.path.join(out_dir, "fires_7d.json"))
    return outputs

# --- Tarea en background para reintentar MODIS cada 10 minutos ---
async def retry_modis_background():
    while True:
        try:
            conn = get_conn()
            res = await asyncio.to_thread(
                refresh_all,
                conn,
                ["MODIS_NRT"],
                day_range=1,
                pause_sec=0,
                dump_after_each=True,
                log_progress=True,
                prioritize_modis=False,
            )
            if (res or {}).get("inserted", 0) > 0:
                await asyncio.to_thread(dump_all_geojson, conn, OUT_DIR)
                log.info("[MODIS] Ingresaron datos. Deteniendo reintentos.")
                return
        except Exception as e:
            log.warning(f"[MODIS][retry] {e}")
        await asyncio.sleep(600)  # 10 min

# =========================
# FastAPI (API)
# =========================
app = FastAPI(title="FIRMS ARG Fires API (SQLite)")

# CORS (abierto para pruebas, o restringí al FRONTEND_ORIGIN si lo definís)
if FRONTEND_ORIGIN:
    allow_origins = [FRONTEND_ORIGIN]; allow_credentials = False
else:
    allow_origins = ["*"]; allow_credentials = False

app.add_middleware(
    CORSMiddleware,
    allow_origins=allow_origins,
    allow_credentials=allow_credentials,
    allow_methods=["GET","OPTIONS"],
    allow_headers=["*"],
)

@app.on_event("startup")
async def startup_event():
    """
    Arranca con un seed rápido (MODIS, 1 día) para tener puntos cuanto antes,
    y luego corre el ETL completo con el resto de sensores. Ambos en background.
    """
    log.info("[INIT] ETL inicial en background…")
    conn = get_conn()
    ensure_schema(conn)

    async def seed_fast():
        try:
            # Seed: MODIS día 1 (rápido) + dump parcial
            await asyncio.to_thread(
                refresh_all,
                conn,
                ["MODIS_NRT"],
                day_range=1,
                pause_sec=0,
                dump_after_each=True,
                log_progress=True,
                prioritize_modis=False,
            )
            await asyncio.to_thread(dump_all_geojson, conn, OUT_DIR)
            log.info("[INIT][SEED] Listo.")
        except Exception as e:
            log.warning(f"[INIT][SEED] {e}")

    async def full_etl():
        try:
            # ETL completo con la config real
            await asyncio.to_thread(
                refresh_all,
                conn,
                SENSORS,
                DAY_RANGE,
                pause_sec=10,
                dump_after_each=True,
                log_progress=True,
                prioritize_modis=True,
            )
            await asyncio.to_thread(dump_all_geojson, conn, OUT_DIR)
            log.info("[INIT][FULL] ETL inicial OK.")
        except Exception as e:
            log.warning(f"[INIT][FULL] {e}")

    # Correr ambas tareas en paralelo (seed rápido + full)
    asyncio.create_task(seed_fast())
    asyncio.create_task(full_etl())

    # Reintento de MODIS (se conserva)
    asyncio.create_task(retry_modis_background())


@app.get("/health")
def health():
    with get_conn() as conn:
        last_refresh = get_meta(conn, "last_refresh_utc")
        last_attempt = get_meta(conn, "last_attempt_utc")
        total = conn.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='fires'").fetchone()[0]
        rows = conn.execute("SELECT COUNT(*) FROM fires").fetchone()[0] if total else 0
    return {
        "ok": True,
        "time_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "last_refresh_utc": last_refresh,
        "last_attempt_utc": last_attempt,
        "rows": rows
    }

@app.get("/refresh")
def refresh_endpoint(sensors: Optional[str] = None, day_range: Optional[int] = None, pause_sec: int = 10):
    """
    Re-corre el ETL bajo demanda y regenera los JSON.
    Usa ?sensors=A,B,C y &day_range=1..10 para ajustes finos.
    """
    used = [s.strip() for s in (sensors or ",".join(SENSORS)).split(",") if s.strip()]
    dr = int(day_range or DAY_RANGE)
    if dr < 1 or dr > 10:
        raise HTTPException(400, "day_range debe ser entre 1 y 10.")
    conn = get_conn()
    res = refresh_all(conn, used, dr, pause_sec=pause_sec)
    try:
        dump_all_geojson(conn, OUT_DIR)
    except Exception as e:
        # No romper aunque fallara el volcado
        log.warning(f"[dump] {e}")
    return res

@app.get("/fires")
def get_fires(
    window: str = Query("24h", pattern="^(today|24h|3d|7d)$"),
    limit: int = Query(50000, ge=1, le=200000)
) -> Dict[str, Any]:
    """
    Devuelve GeoJSON unificado desde SQLite según ventana temporal.
    (Sigue generando archivos estáticos por si querés servirlos directo.)
    """
    col, start_iso, end_iso = window_to_bounds(window)
    with get_conn() as conn:
        exists = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='fires'"
        ).fetchone()[0]
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