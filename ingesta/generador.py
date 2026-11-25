# Databricks notebook source
import json
import random
import re
from uuid import uuid4

dbutils.widgets.text("sufijo", "")
dbutils.widgets.text("lote", "")
dbutils.widgets.text("registros", "500")

sufijo = dbutils.widgets.get("sufijo").strip().lower()
lote = dbutils.widgets.get("lote").strip().lower()
if not re.fullmatch(r"[a-z]{3}", sufijo):
    raise ValueError("El parametro sufijo debe tener tres letras, por ejemplo dsh.")
if lote not in {"lote_01", "lote_02", "continuo"}:
    raise ValueError("El parametro lote debe ser lote_01, lote_02 o continuo.")

lotes = {
    "lote_01": [
        {"evento_id": "e1", "aeronave_id": "A", "pais_origen": "CR", "velocidad_kmh": 200, "persona_id": "DUMMY-ID-001", "persona_alias": "DUMMY_UCRANIA"},
        {"evento_id": "e2", "aeronave_id": "B", "pais_origen": "PA", "velocidad_kmh": 300, "persona_id": "DUMMY-ID-002", "persona_alias": "DUMMY_GRECIA"},
        {"evento_id": "e3", "aeronave_id": "A", "pais_origen": "CR", "velocidad_kmh": 250, "persona_id": "DUMMY-ID-003", "persona_alias": "DUMMY_FILIPINAS"},
    ],
    "lote_02": [
        {"evento_id": "e4", "aeronave_id": "C", "pais_origen": "GT", "velocidad_kmh": 400, "persona_id": "DUMMY-ID-004", "persona_alias": "DUMMY_UCRANIA"},
        {"evento_id": "e5", "aeronave_id": "B", "pais_origen": "PA", "velocidad_kmh": 350, "persona_id": "DUMMY-ID-005", "persona_alias": "DUMMY_GRECIA"},
    ],
}
if lote == "continuo":
    valor = dbutils.widgets.get("registros").strip()
    if not re.fullmatch(r"[0-9]{1,3}", valor) or not 1 <= int(valor) <= 500:
        raise ValueError("El parametro registros debe ser un entero entre 1 y 500.")
    cantidad = int(valor)
    token = uuid4().hex
    rng = random.Random()
    eventos = []
    for indice in range(cantidad):
        eventos.append({
            "evento_id": f"dummy_{token}_{indice:03d}",
            "aeronave_id": rng.choice(["A", "B", "C", "D", "E"]),
            "pais_origen": rng.choice(["CR", "PA", "GT"]),
            "velocidad_kmh": float(rng.randint(150, 850)),
            "persona_id": f"DUMMY-ID-{token}-{indice:03d}",
            "persona_alias": rng.choice(["DUMMY_UCRANIA", "DUMMY_GRECIA", "DUMMY_FILIPINAS"]),
        })
    archivo = f"lote_continuo_{token}.json"
else:
    eventos = lotes[lote]
    archivo = f"{lote}.json"

ruta = f"/Volumes/retos_sdp_{sufijo}/reto02_{sufijo}/volumen_sdp_{sufijo}/reto02_{sufijo}/entrada_{sufijo}"
dbutils.fs.mkdirs(ruta)
contenido = "\n".join(json.dumps(evento) for evento in eventos) + "\n"
if not dbutils.fs.put(f"{ruta}/{archivo}", contenido, False):
    raise RuntimeError(f"No se pudo escribir el archivo {archivo}.")
print(f"Lote {lote}: {len(eventos)} eventos escritos en {ruta}/{archivo}")
