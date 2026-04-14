#!/usr/bin/env python3
from __future__ import annotations

import geopandas as gpd
import pandas as pd
from sqlalchemy import create_engine

# Definir rutas y parametros de entrada de forma explicita.
zonas_path = "../data/zonas_inundables.gpkg"
zonas_layer = "zi_500"
siose_path = "../data/siose_ar_2017_combinada.geojson"

# Configuracion de salida a PostGIS.
# El esquema de la URL de conexión sigue el formato:
# postgresql://usuario:contraseña@host:puerto/base_de_datos
postgis_url = "postgresql://postgres:postgres@postgres:5432/geopython"

# Leer la capa de zonas inundables desde el GeoPackage.
# Se especifica el nombre de la capa para evitar ambigüedad.
zonas = gpd.read_file(zonas_path, layer=zonas_layer)

# Leer el GeoJSON de SIOSE con las categorias combinadas.
siose = gpd.read_file(siose_path)

# Ver el número de filas y columnas:
print(f"Zonas inundables: {zonas.shape}")
print(f"SIOSE: {siose.shape}")

# Ver la cabecera de cada capa para entender su estructura:
print()
print(zonas.head())
print()
print(siose.head())

# Comprobar que ambas capas tienen geometria y el tipo de geometria:
print()
print(f"Zonas geometria: {zonas.geometry.geom_type.unique()}")
print(f"SIOSE geometria: {siose.geometry.geom_type.unique()}")

# Comprobar el CRS de cada capa:
print()
print(f"Zonas CRS: {zonas.crs}")
print(f"SIOSE CRS: {siose.crs}")

# Alinear CRS si difieren, reproyectando SIOSE al CRS de zonas.
# Esto asegura que las geometrias esten en el mismo sistema.
if siose.crs != zonas.crs:
    siose = siose.to_crs(zonas.crs)

# Intersectar los poligonos de SIOSE con las zonas inundables.
# Posibles valores de "how" en overlay:
#    - "intersection": solo la parte comun entre ambas capas.
#    - "union": combina todas las geometrias (union espacial).
#    - "identity": conserva geometria de la primera capa, agregando atributos de la segunda.
#    - "difference": parte de la primera capa que no se superpone con la segunda.
#    - "symmetric_difference": partes no comunes de ambas capas.
# El resultado conserva los atributos de ambas capas segun el modo elegido.

# Filtramos las zonas que nos intersan.
zonas = zonas[zonas["rio"].isin([
    "Barranco Fraile",
    "Barranco La Vera",
    "Arroyo Valsequillo",
    "Caño La Culata",
    "Arroyo Regajo 5",
    "Arroyo Lepe"
])]

siose_ar_intersect = gpd.overlay(
    siose,
    zonas,
    how="intersection"
)

# 7) Normalizar el indice para facilitar cargas a PostGIS.
# siose_ar_intersect = siose_ar_intersect.reset_index(drop=True)


# Calcular area en la proyeccion actual.
# Si el CRS es geografico, el area sera en grados y no en metros.
siose_ar_intersect["area_m2"] = round(siose_ar_intersect.geometry.area)

print()
print(siose_ar_intersect.head())

# Leer el catalogo de coberturas y unir por id_cobertura_max.
# Esto agrega la descripcion legible de la cobertura.
codigo_cobertura_path = "../data/codigo_cobertura.csv"
codigo_cobertura = pd.read_csv(codigo_cobertura_path)

print()
print(codigo_cobertura.head())

# Unión. El lado "left" es el resultado del intersect, que
# queremos conservar completo, mientras que el lado "right" es el
# catalogo de coberturas, que solo aporta la descripcion.
siose_ar_intersect = siose_ar_intersect.merge(
    codigo_cobertura,
    how="left",
    left_on="id_cobertura_max",
    right_on="id_cobertura",
)

print()
print(siose_ar_intersect.head())

# Reordenamos primero un poco las columnas.
siose_ar_intersect = siose_ar_intersect[
    [
        "municipio_nombre",
        "id_cobertura_max",
        "cobertura_desc",
        "rio",
        "area_m2",
        "geometry",
    ]
]

print()
print(siose_ar_intersect.shape)
print(siose_ar_intersect.head())

# Generar resumen por cobertura_desc con suma de area.
resumen_cobertura = (
    siose_ar_intersect
        .groupby("cobertura_desc", dropna=False, as_index=False)["area_m2"]
        .sum()
        .rename(columns={"area_m2": "area_total"})
        .sort_values("area_total", ascending=False)
)

resumen_cobertura["area_total_ha"] = round(resumen_cobertura["area_total"] / 10000, 2)

print()
print(resumen_cobertura)

# Enviar el resultado a PostGIS usando to_postgis.
# Se reemplaza la tabla si ya existe.
engine = create_engine(postgis_url)
siose_ar_intersect.to_postgis(
    name="siose_ar_intersect",
    con=engine,
    schema="geopython",
    if_exists="replace",
    index=True,
)