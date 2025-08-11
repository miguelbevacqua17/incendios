# 🌎 Monitor de Incendios Argentina (NASA FIRMS + SQLite + MapLibre)

Visualiza incendios activos en Argentina usando **NASA FIRMS** (VIIRS y MODIS). Los datos se guardan en **SQLite** y se sirven con **FastAPI**. El frontend es un mapa **MapLibre** con estilo tipo Google Maps (Carto Voyager), **clusters**, **heatmap**, contador y “última actualización”.

---

## 📦 Requisitos

* GitHub **Codespaces** (recomendado) o Python 3.11/3.12 local
* Dependencias (ver `requirements.txt`): `fastapi`, `uvicorn`, `requests`, `python-dotenv` (opcional)
* **Clave FIRMS** (MAP KEY) de [https://firms.modaps.eosdis.nasa.gov/](https://firms.modaps.eosdis.nasa.gov/)

---

## 🚀 Uso en GitHub Codespaces

### 1) Abrir el Codespace

GitHub → **Code** → **Codespaces** → **Create Codespace on main**.

### 2) Configurar el Secret con la MAP KEY

1. En **Settings → Codespaces → Secrets** → **New Secret**.
2. **Name:** `FIRMS_MAP_KEY`
3. **Value:** tu MAP KEY de FIRMS.

> *Opcional (CORS)* Si vas a servir el front en el puerto 8080 del Codespace, definí también `FRONTEND_ORIGIN` antes de levantar el backend (o fijalo como variable de entorno en la terminal):
>
> ```bash
> export FRONTEND_ORIGIN="https://<TU-CODESPACE>-8080.app.github.dev"
> ```

### 3) Iniciar el backend (FastAPI)

```bash
pkill -f uvicorn 2>/dev/null || true
uvicorn server:app --host 0.0.0.0 --port 8000
```

Esto:

* Ejecuta el **ETL inicial en background** (VIIRS S‑NPP, NOAA‑20, NOAA‑21 y reintentos para **MODIS\_NRT**).
* Crea/actualiza la base `fires.db`.
* Genera `fires_today.json`, `fires_24h.json`, `fires_3d.json`, `fires_7d.json`.
* Expone la API en `:8000`.

### 4) Iniciar el frontend (mapa)

```bash
python -m http.server 8080
```

Luego abrí el puerto **8080** (pestaña **Ports** → Open in Browser) y cargá `index.html`.

---

## 🌐 Endpoints del backend

* **GET `/fires?window=24h`** → GeoJSON unificado de las últimas 24h.
  Valores de `window`: `today` (día calendario local AR), `24h`, `3d`, `7d`.

  **Ejemplo**:

  ```
  https://<TU-CODESPACE>-8000.app.github.dev/fires?window=24h
  ```

* **GET `/refresh`** → fuerza re-ejecución del ETL y regenera JSON estáticos.
  Parámetros opcionales: `sensors=VIIRS_SNPP_NRT,VIIRS_NOAA20_NRT,VIIRS_NOAA21_NRT,MODIS_NRT`, `day_range=1..10`.

* **GET `/health`** → ping rápido de salud.

---

## 🗺️ Frontend (MapLibre)

* Basemap **Carto Voyager** (raster, sin API key), similar a Google Maps “light”.
* Capa de **clusters** y de **puntos** con popups.
* **Heatmap** con toggle.
* Selector de ventana temporal (`today`, `24h`, `3d`, `7d`).
* **Contador total** y **última actualización** (desde `meta` del GeoJSON).

> El `index.html` trae los datos de `BACKEND_URL/fires?window=...` (ajustá `BACKEND_URL` dentro del archivo si cambia tu dominio del Codespace).

---

## ⚙️ Estructura del proyecto

```
.
├── server.py              # API FastAPI + ETL FIRMS → SQLite (+ JSON estáticos)
├── index.html             # Frontend MapLibre (Carto Voyager, clusters, heatmap)
├── requirements.txt       # Dependencias Python
├── fires.db               # SQLite con focos (se genera al iniciar)
└── README.md
```

---

## 🛠️ Ejecución local (opcional)

1. **Clonar el repo**

```bash
git clone <URL_DEL_REPO>
cd <NOMBRE_DEL_REPO>
```

2. **Entorno virtual**

```bash
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
.venv\Scripts\activate     # Windows
```

3. **Instalar dependencias**

```bash
pip install -r requirements.txt
```

4. **Variables de entorno**

```bash
export FIRMS_MAP_KEY="TU_MAP_KEY"
# Opcional (CORS si usás server estático en 8080)
# export FRONTEND_ORIGIN="http://localhost:8080"
```

5. **Levantar backend y frontend**

```bash
uvicorn server:app --host 0.0.0.0 --port 8000
python -m http.server 8080
```

---

## ✅ Validación rápida

* Abrí `https://<TU-CODESPACE>-8000.app.github.dev/fires?window=24h` → debería responder un **FeatureCollection** con `meta.count` > 0 (si el ETL ya cargó datos).
* Mirá la consola del backend: verás logs de backoff si FIRMS responde **403/429**.
* En el mapa (8080) deberían verse clusters/puntos; cambiá la ventana temporal para refetch.

---

## 🧰 Troubleshooting

**CORS bloqueado** (front 8080 → back 8000):

* Definí `FRONTEND_ORIGIN` antes de levantar el backend.
* Reiniciá Uvicorn.

**403/429 de FIRMS (rate limit)**:

* El servidor reintenta con backoff; además corre una tarea en background que vuelve a intentar **MODIS\_NRT** cada 10 min.
* Probá refrescar manualmente con `day_range=1` y/o por sensor:

  ```
  /refresh?sensors=MODIS_NRT&day_range=1
  ```

**Base vacía** (`503` en `/fires`):

* Ejecutá `/refresh` y esperá a que termine el ETL.

**Claves**:

* Verificá que el secret se llame **`FIRMS_MAP_KEY`** exactamente.
* Reabrí la terminal del Codespace si recién lo creaste.

---

## 🔎 Atribución

**Source:** NASA FIRMS (LANCE), NASA EOSDIS — [https://firms.modaps.eosdis.nasa.gov/](https://firms.modaps.eosdis.nasa.gov/)

Mapa base: © [OpenStreetMap](https://www.openstreetmap.org/copyright) contributors, © [CARTO](https://carto.com/attributions).

---

## 📄 Licencia

Uso libre con atribución. Este proyecto no está afiliado a NASA. Los datos y marcas son de sus respectivos titulares.
