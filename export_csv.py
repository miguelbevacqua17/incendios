# export_csv.py
# Exporta a CSV los focos obtenidos desde /fires del backend FastAPI (con reintentos robustos).
# Uso típico:
#   python export_csv.py --backend-url "https://<TU-CS>-8000.app.github.dev" --window 3d --excel --retries 10 --wait 6

import argparse
import csv
import time
from typing import Any, Dict, List
from urllib.parse import urljoin

import requests
from requests.exceptions import JSONDecodeError


def fetch_geojson(backend_url: str, window: str, timeout: int, retries: int, wait: float) -> Dict[str, Any]:
    base = backend_url.rstrip("/") + "/"
    url = urljoin(base, "fires")
    params = {"window": window}

    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, params=params, timeout=timeout)
            ct = (resp.headers.get("content-type") or "").lower()
            status = resp.status_code

            # Errores temporales típicos
            if status in (502, 503, 504):
                print(f"[WARN] {status} {resp.reason} desde backend. Intento {attempt}/{retries}. Reintento en {wait}s…")
                time.sleep(wait)
                continue

            if status != 200:
                snippet = (resp.text or "")[:300].replace("\n", " ")
                raise RuntimeError(f"HTTP {status} {resp.reason}. Content-Type={ct}. Body[:300]={snippet!r}")

            # Debe ser JSON/GeoJSON
            if "application/json" not in ct and "geo+json" not in ct:
                snippet = (resp.text or "")[:300].replace("\n", " ")
                print(f"[WARN] Respuesta no-JSON (CT={ct}). Intento {attempt}/{retries}. Snippet: {snippet!r}")
                time.sleep(wait)
                continue

            try:
                return resp.json()
            except JSONDecodeError:
                snippet = (resp.text or "")[:300].replace("\n", " ")
                print(f"[WARN] JSON inválido. Intento {attempt}/{retries}. Snippet: {snippet!r}")
                time.sleep(wait)
                continue

        except requests.RequestException as e:
            print(f"[WARN] Error de red: {e}. Intento {attempt}/{retries}. Reintento en {wait}s…")
            time.sleep(wait)

    raise SystemExit("[ERROR] No se pudo obtener JSON válido desde el backend tras varios intentos.")


def flatten_features(geojson: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for ft in geojson.get("features", []):
        props = ft.get("properties", {}) or {}
        geom = ft.get("geometry", {}) or {}
        lon = lat = None
        if isinstance(geom, dict) and geom.get("type") == "Point":
            coords = geom.get("coordinates", [])
            if isinstance(coords, (list, tuple)) and len(coords) >= 2:
                lon, lat = coords[0], coords[1]
        row = dict(props)
        row["lon"] = lon
        row["lat"] = lat
        rows.append(row)
    return rows


def choose_columns(rows: List[Dict[str, Any]]) -> List[str]:
    if not rows:
        return ["lon", "lat"]
    all_keys = set().union(*(r.keys() for r in rows))
    prefer = [
        "datetime", "acq_datetime", "acq_date", "acq_time",
        "sensor", "satellite", "source",
        "confidence", "confidence_text",
        "frp", "brightness", "bright_ti4", "bright_ti5",
        "daynight", "lon", "lat",
    ]
    ordered = [k for k in prefer if k in all_keys]
    rest = sorted(k for k in all_keys if k not in ordered)
    return ordered + rest


def write_csv(rows: List[Dict[str, Any]], out_path: str, columns: List[str], excel_hint: bool = False) -> None:
    encoding = "utf-8-sig" if excel_hint else "utf-8"  # BOM para Excel si --excel
    with open(out_path, "w", newline="", encoding=encoding) as f:
        if excel_hint:
            f.write("sep=,\n")  # ayuda a Excel a usar coma como separador
        w = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow({c: r.get(c, "") for c in columns})


def main():
    ap = argparse.ArgumentParser(description="Exporta a CSV los focos de /fires del backend (con reintentos).")
    ap.add_argument("--backend-url", required=True, help="URL base del backend (ej. https://<CS>-8000.app.github.dev)")
    ap.add_argument("--window", choices=["today", "24h", "3d", "7d"], default="24h", help="Ventana temporal")
    ap.add_argument("--out", default=None, help="Ruta del CSV de salida (opcional)")
    ap.add_argument("--timeout", type=int, default=60, help="Timeout HTTP por intento (s)")
    ap.add_argument("--retries", type=int, default=8, help="Cantidad de reintentos si la respuesta no es JSON")
    ap.add_argument("--wait", type=float, default=5.0, help="Espera entre reintentos (s)")
    ap.add_argument("--excel", action="store_true", help="Optimiza CSV para Excel (BOM + 'sep=,')")
    args = ap.parse_args()

    print(f"[INFO] Leyendo GeoJSON de {args.backend_url}/fires?window={args.window} …")
    gj = fetch_geojson(args.backend_url, args.window, timeout=args.timeout, retries=args.retries, wait=args.wait)

    meta = gj.get("meta", {})
    count = meta.get("count", "N/D")
    generated_at = meta.get("generated_at") or meta.get("generated") or "N/D"
    print(f"[INFO] Meta: count={count} | generated_at={generated_at}")

    rows = flatten_features(gj)
    cols = choose_columns(rows)

    ts = time.strftime("%Y%m%d_%H%M%S")
    out_path = args.out or f"export_fires_{args.window}_{ts}.csv"
    write_csv(rows, out_path, columns=cols, excel_hint=args.excel)
    print(f"[OK] Export listo → {out_path} | filas={len(rows)} | columnas={len(cols)}")


if __name__ == "__main__":
    main()
