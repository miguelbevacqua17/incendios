#!/usr/bin/env python3
import os
import io
import csv
import json
import sqlite3
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Optional, Dict, Any, List, Tuple
import time
import requests

# ===== Config =====
FIRMS_MAP_KEY = os.getenv("FIRMS_MAP_KEY", "").strip()
if not FIRMS_MAP_KEY:
    raise RuntimeError("FIRMS_MAP_KEY no está definido.")

COUNTRY = os.getenv("FIRMS_COUNTRY", "ARG").strip()
DAY_RANGE = int(os.getenv("FIRMS_DAY_RANGE", "1"))  # 1..10
OUT_DIR = os.getenv("OUT_DIR", ".")  # dónde dejar los JSON
DB_PATH = os.getenv("DB_PATH", "fires.db")

DEFAULT_SENSORS = "VIIRS_SNPP_NRT,VIIRS_NOAA20_NRT,VIIRS_NOAA21_NRT"
SENSORS = [s.strip() for s in os.getenv("FIRMS_SENSORS", DEFAULT_SENSORS).split(",") if s.strip()]

TZ_AR = ZoneInfo("America/Argentina/Buenos_Aires")

CREATE_SQL = """
CREATE TABLE IF NOT EXISTS fires (
  id INTEGER PRIMARY KEY,
  source TEXT NOT NULL,
  latitude REAL NOT NULL,
  longitude REAL NOT NULL,
  acq_date TEXT NOT NULL,
  acq_time TEXT NOT NULL,  -- HHMM
  timestamp_utc TEXT NOT NULL,
  timestamp_local TEXT NOT NULL,
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
"""

def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn

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

def fetch_csv(source: str, country: str, day_range: int) -> str:
    url = f"https://firms.modaps.eosdis.nasa.gov/api/country/csv/{FIRMS_MAP_KEY}/{source}/{country}/{day_range}"
    r = requests.get(url, timeout=60)
    if r.status_code in (403, 429):
        raise RuntimeError(f"FIRMS {r.status_code}: {r.text[:200]}")
    if r.status_code >= 400:
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
            try: frp_val = float(r.get("frp"))
            except Exception: frp_val = None

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
    if not rows: return 0
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

def refresh_all(conn: sqlite3.Connection, sensors: List[str], day_range: int, pause_sec: int = 10):
    for s in sensors:
        text = fetch_csv(s, COUNTRY, day_range)
        rows = csv_to_rows(text, s)
        n = upsert_rows(conn, rows)
        print(f"[{s}] parsed={len(rows)} inserted={n}")
        time.sleep(max(0, pause_sec))

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
        raise ValueError("window inválida")

def dump_geojson(conn: sqlite3.Connection, window: str, out_path: str):
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
            "type":"Feature",
            "geometry":{"type":"Point","coordinates":[lon,lat]},
            "properties": props
        })
    geo = {
        "type":"FeatureCollection",
        "features": features,
        "meta": {
            "count": len(features),
            "window": window,
            "last_update": last_update,
            "source": "NASA FIRMS (LANCE), NASA EOSDIS",
            "attribution_url": "https://firms.modaps.eosdis.nasa.gov/"
        }
    }
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(geo, f, ensure_ascii=False)
    print(f"[write] {out_path}  (features={len(features)})")

def main():
    # 1) preparar DB
    conn = get_conn()
    with conn:
        for stmt in CREATE_SQL.split(";"):
            if stmt.strip():
                conn.execute(stmt)

    # 2) refrescar desde FIRMS (respetar rate limit)
    refresh_all(conn, SENSORS, DAY_RANGE, pause_sec=10)

    # 3) volcar 4 ventanas a archivos estáticos
    os.makedirs(OUT_DIR, exist_ok=True)
    dump_geojson(conn, "today", os.path.join(OUT_DIR, "fires_today.json"))
    dump_geojson(conn, "24h",  os.path.join(OUT_DIR, "fires_24h.json"))
    dump_geojson(conn, "3d",   os.path.join(OUT_DIR, "fires_3d.json"))
    dump_geojson(conn, "7d",   os.path.join(OUT_DIR, "fires_7d.json"))
    print("OK.")

if __name__ == "__main__":
    main()
