# Monitoreo de incendios en Argentina con NASA FIRMS

Este proyecto ofrece una solución completa para visualizar y analizar los
focos de incendios detectados en Argentina a partir de los servicios
FIRMS de la NASA. Se compone de un backend Python que descarga y
unifica los datos de los sensores VIIRS (S‑NPP, NOAA‑20 y NOAA‑21) y
opcionalmente MODIS, y de un frontend en HTML/JavaScript que
presenta la información en un mapa interactivo con clústeres y
heatmap. La solución respeta las restricciones de uso del API (límite
de 5000 transacciones cada 10 minutos firms.modaps.eosdis.nasa.gov) 
mediante una caché en memoria configurable.

# Contenido
# server.py: 
API REST construida con FastAPI. Expone el endpoint GET /fires?window=today|24h|3d|7d que devuelve un
FeatureCollection GeoJSON con todos los focos detectados para Argentina filtrados por ventana temporal. El servidor fusiona
automáticamente las descargas de los sensores VIIRS (configurables) y convierte las columnas acq_date y acq_time del CSV de NASA en
timestamps con zona horaria firms.modaps.eosdis.nasa.gov . Incluye caché en memoria y soporta CORS para permitir el acceso desde páginas estáticas.

# index.html: 
interfaz web basada en MapLibre GL. Permite seleccionar la ventana temporal, alternar la visualización en
heatmap, muestra el número total de focos y la hora de la última actualización, y despliega popups con información detallada (sensor,
confianza, FRP) al pulsar sobre cada punto.

# requirements.txt: 
lista de dependencias Python.

# Prerrequisitos

Obtener una clave MAP_KEY de FIRMS. Regístrate en https://firms.modaps.eosdis.nasa.gov/api/map_key para solicitar
gratuitamente tu clave. La clave es necesaria para consumir los endpoints y cada petición consume parte del cupo de 5000
transacciones cada 10 minutos firms.modaps.eosdis.nasa.gov
El código ya contempla una caché de 5 minutos para evitar excesos.

Python 3.9+ con el módulo estándar zoneinfo (incluido a partir de Python 3.9).

# Instalación y ejecución del servidor

1. Descarga o clona este repositorio en Codespaces, Vercel o tu equipo local.
2. Crea un entorno virtual (opcional pero recomendado):

>   python -m venv .venv
    source .venv/bin/activate

3. Instala las dependencias:

>   pip install -r requirements.txt


# NUEVO README