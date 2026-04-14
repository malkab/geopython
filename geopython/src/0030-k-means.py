#!/usr/bin/env python3
from __future__ import annotations

import geopandas as gpd
import pandas as pd
from sqlalchemy import create_engine
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

# Parametros de entrada.
postgis_url = "postgresql://postgres:postgres@postgres:5432/geopython"
postgis_schema = "geopython"
postgis_table = "poblacion_ex"

# Filtrado por cobertura.
# Ajustar esta lista segun las coberturas que quieras analizar.
id_cobertura_max_filtrar = 101

# Parametro de clustering.
k = 3

# Leer la tabla desde PostGIS con filtro por id_cobertura_max.
engine = create_engine(postgis_url)

query = (
    f"SELECT * FROM {postgis_schema}.{postgis_table} "
    f"WHERE id_cobertura_max = {id_cobertura_max_filtrar}"
)

poblacion_ex = gpd.read_postgis(query, con=engine, geom_col="geometry")

print()
print(poblacion_ex.shape)
print(poblacion_ex.head())

# Definir el vector tematico y limpiar valores nulos.
campos = ["area_m2", "p_total", "indice_dependencia"]
poblacion_ex = poblacion_ex.dropna(subset=campos)

print()
print(poblacion_ex.head())

# Preparar la matriz para k-means.
x = poblacion_ex[campos].astype(float)

print()
print(x.head())

# Estandarizar variables para evitar sesgos por escala.
scaler = StandardScaler()
x_scaled = scaler.fit_transform(x)

print()
print(x_scaled[:5])

# Ejecutar k-means.
kmeans = KMeans(n_clusters=k, n_init=10)
poblacion_ex["cluster"] = kmeans.fit_predict(x_scaled)

print()
print(poblacion_ex["cluster"].value_counts().sort_index())

# Exportar a PostGIS la tabla con la columna de clusters y el vector tematico.
salida_table = "poblacion_ex_clusters"
cols_salida = ["area_m2", "p_total", "indice_dependencia", "cluster", "geometry"]
poblacion_ex[cols_salida].to_postgis(
    name=salida_table,
    con=engine,
    schema=postgis_schema,
    if_exists="replace",
    index=False,
)
