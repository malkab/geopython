#!/usr/bin/env python3
from __future__ import annotations

import geopandas as gpd
from sqlalchemy import create_engine

# Definir rutas y parametros de entrada de forma explicita.
poblacion_path = "../data/poblacion/poblacion.shp"

# Configuracion de entrada desde PostGIS.
# El esquema de la URL de conexión sigue el formato:
# postgresql://usuario:contraseña@host:puerto/base_de_datos
postgis_url = "postgresql://postgres:postgres@postgres:5432/geopython"
postgis_schema = "geopython"
postgis_table = "siose_ar_intersect"

# Leer la tabla siose_ar_intersect desde PostGIS.
engine = create_engine(postgis_url)
siose_ar_intersect = gpd.read_postgis(
    f"SELECT * FROM {postgis_schema}.{postgis_table}",
    con=engine,
    geom_col="geometry",
)

print()
print(siose_ar_intersect.head())

# Leer el shapefile de poblacion.
poblacion = gpd.read_file(poblacion_path)

print()
print(poblacion.shape)
print(poblacion.head())

# Comprobar el CRS de cada capa.
print()
print(f"SIOSE intersect CRS: {siose_ar_intersect.crs}")
print(f"Poblacion CRS: {poblacion.crs}")

# Alinear CRS si difieren, reproyectando poblacion al CRS de SIOSE.
if poblacion.crs != siose_ar_intersect.crs:
    poblacion = poblacion.to_crs(siose_ar_intersect.crs)

# Intersectar siose_ar_intersect con la capa de poblacion.
poblacion_intersect = gpd.overlay(
    siose_ar_intersect,
    poblacion,
    how="identity",
)

# Intersectamos aquellas zonas donde exclusivamente se da sólo
# población
poblacion_ex = gpd.overlay(
    siose_ar_intersect,
    poblacion,
    how="intersection",
)

# Calculamos el índice de dependencia de la población.
poblacion_intersect["indice_dependencia"] = round((
    poblacion_intersect["p_15"] + poblacion_intersect["p_65"]
) / poblacion_intersect["p_16_64"], 2)

poblacion_ex["indice_dependencia"] = round((
    poblacion_intersect["p_15"] + poblacion_intersect["p_65"]
) / poblacion_intersect["p_16_64"], 2)

# Reordenamos columnas.
poblacion_intersect = poblacion_intersect[
    [
        "municipio_nombre",
        "id_cobertura_max",
        "cobertura_desc",
        "area_m2",
        "p_total",
        "indice_dependencia",
        "geometry",
    ]
]

poblacion_ex = poblacion_ex[
    [
        "municipio_nombre",
        "id_cobertura_max",
        "cobertura_desc",
        "area_m2",
        "p_total",
        "indice_dependencia",
        "geometry",
    ]
]

print()
print(poblacion_intersect.shape)
print(poblacion_intersect.head())

# Enviar el resultado a PostGIS usando to_postgis.
# Se reemplaza la tabla si ya existe.
engine = create_engine(postgis_url)
poblacion_intersect.to_postgis(
    name="poblacion_intersect",
    con=engine,
    schema="geopython",
    if_exists="replace",
    index=True,
)

# Enviar el resultado a PostGIS usando to_postgis.
# Se reemplaza la tabla si ya existe.
engine = create_engine(postgis_url)
poblacion_ex.to_postgis(
    name="poblacion_ex",
    con=engine,
    schema="geopython",
    if_exists="replace",
    index=True,
)