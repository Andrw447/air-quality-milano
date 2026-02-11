#!/usr/bin/env python3
"""
fetch_official_db.py

Scarica i dataset ufficiali qualità dell'aria (Comune di Milano), normalizza i campi
e produce:
 - data/air_quality_official.csv
 - data/stations_official.csv
 - data/air_quality_official.db  (SQLite con tabelle: measurements, stations)

Posiziona questo file nella cartella `data/` del tuo repo e lancialo con:
$ python data/fetch_official_db.py
"""

import os
import sys
import tempfile
import sqlite3
import json
from typing import List
import requests
import pandas as pd

# -----------------------
# CONFIG: URL ufficiali (modifica se necessario)
# -----------------------
DS573_JSON = "https://dati.comune.milano.it/dataset/ad529de1-8398-43e9-bba5-c5012513f23f/resource/eade3387-bff8-4de0-ac24-09360f38ded7/download/ds573_inquinanti_aria.json"
DS407_2024_JSON = "https://dati.comune.milano.it/dataset/dba6b6ff-792b-471d-9a2c-a625f1398f5f/resource/bcef81c8-4011-4225-93ee-f284387e8834/download/qaria_datoariagiornostazione_2024-12-24.json"
STATIONS_CSV = "https://dati.comune.milano.it/dataset/d6960c75-0a02-4fda-a85f-3b1c4aa725d6/resource/b301f327-7504-4efc-8b4a-5f4a29f9d0ff/download/qaria_stazione.csv"

# -----------------------
# OUTPUT: salva nella stessa cartella di questo script (data/)
# -----------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # assuming script is in data/
CSV_OUT = os.path.join(BASE_DIR, "air_quality_official.csv")
STATIONS_OUT = os.path.join(BASE_DIR, "stations_official.csv")
DB_OUT = os.path.join(BASE_DIR, "air_quality_official.db")

# -----------------------
# HELPERS
# -----------------------
def download_to_temp(url: str, timeout: int = 60) -> str:
    print(f"Downloading: {url}")
    r = requests.get(url, stream=True, timeout=timeout)
    r.raise_for_status()
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(url)[1] or ".tmp")
    with open(tmp.name, "wb") as fh:
        for chunk in r.iter_content(chunk_size=32768):
            if chunk:
                fh.write(chunk)
    print(f"Saved temporary file: {tmp.name}")
    return tmp.name

def read_json_flexible(path_or_file: str) -> pd.DataFrame:
    """
    Prova a leggere il file JSON con pandas.read_json, poi con orient='records',
    poi normalizza oggetti annidati.
    """
    try:
        return pd.read_json(path_or_file)
    except Exception:
        try:
            return pd.read_json(path_or_file, orient="records")
        except Exception:
            with open(path_or_file, "r", encoding="utf-8") as fh:
                j = json.load(fh)
            try:
                return pd.json_normalize(j)
            except Exception:
                # ultima risorsa: converti dict in DataFrame se possibile
                if isinstance(j, list):
                    return pd.DataFrame(j)
                raise

def read_csv_flexible(path_or_file: str) -> pd.DataFrame:
    return pd.read_csv(path_or_file)

# Normalizzazione: adattiamo i nomi colonne alle aspettative di src/app.py
def normalize_measurements(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.lower() for c in df.columns]

    # date / datetime
    date_candidates = [c for c in df.columns if "date" in c or "data" in c or "giorno" in c or "day" in c]
    if date_candidates:
        dcol = date_candidates[0]
        df["datetime"] = pd.to_datetime(df[dcol], errors="coerce")
        df["date"] = df["datetime"].dt.date
    else:
        # fallback: if year/month/day present
        if all(x in df.columns for x in ("year","month","day")):
            df["datetime"] = pd.to_datetime(df[["year","month","day"]], errors="coerce")
            df["date"] = df["datetime"].dt.date
        else:
            df["datetime"] = pd.NaT
            df["date"] = pd.NA

    # value
    value_candidates = [c for c in df.columns if c in ("value","valore","concentrazione","concen","concentration","concentration_mean") or "val" in c]
    if value_candidates:
        df["value"] = pd.to_numeric(df[value_candidates[0]], errors="coerce")
    else:
        # try numeric columns heuristics
        numeric_cols = df.select_dtypes(["number"]).columns.tolist()
        if numeric_cols:
            df["value"] = df[numeric_cols[0]]
        else:
            df["value"] = pd.NA

    # pollutant / parameter
    pol_candidates = [c for c in df.columns if "inquin" in c or "param" in c or "pollut" in c or "parameter" in c or c in ("nome","name","indicator")]
    if pol_candidates:
        df["pollutant"] = df[pol_candidates[0]].astype(str)
    else:
        df["pollutant"] = pd.NA

    # station id and name
    st_id_candidates = [c for c in df.columns if "staz" in c or "station" in c or "stazione" in c or c == "id" or "cod" in c]
    if st_id_candidates:
        df["station_id"] = df[st_id_candidates[0]].astype(str)
    else:
        df["station_id"] = pd.NA

    name_candidates = [c for c in df.columns if "nome" in c or "name" in c or "description" in c]
    if name_candidates:
        df["station_name"] = df[name_candidates[0]].astype(str)
    else:
        df["station_name"] = df.get("station_id", pd.NA)

    # lat/lon
    latc = next((c for c in df.columns if c in ("lat","lat_y_4326","latitude")), None)
    lonc = next((c for c in df.columns if c in ("lon","long_x_4326","longitude","long")), None)
    if latc:
        df["lat"] = pd.to_numeric(df[latc], errors="coerce")
    else:
        df["lat"] = pd.NA
    if lonc:
        df["lon"] = pd.to_numeric(df[lonc], errors="coerce")
    else:
        df["lon"] = pd.NA

    # unit & qc_flag
    unit_candidates = [c for c in df.columns if "unit" in c or "unita" in c or "uom" in c]
    df["unit"] = df[unit_candidates[0]].astype(str) if unit_candidates else "µg/m3"
    df["qc_flag"] = df.get("qc_flag", 0)

    # station_type (from dataset if present)
    stype_candidates = [c for c in df.columns if "tipo" in c or "type" in c or "station_type" in c]
    df["station_type"] = df[stype_candidates[0]] if stype_candidates else pd.NA

    # drop rows without essential fields
    df = df.dropna(subset=["value","pollutant","station_id"], how="any").copy()

    # ensure canonical columns exist
    canonical = ["date","datetime","station_id","station_name","lat","lon","station_type","pollutant","unit","value","qc_flag"]
    for c in canonical:
        if c not in df.columns:
            df[c] = pd.NA

    # convert date to ISO string for CSV stability
    df["date"] = df["date"].astype(str)
    return df[canonical]

def normalize_stations(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.lower() for c in df.columns]

    # map common names
    if "id" in df.columns and "station_id" not in df.columns:
        df = df.rename(columns={"id":"station_id"})
    if "nome" in df.columns and "station_name" not in df.columns:
        df = df.rename(columns={"nome":"station_name"})
    if "lat_y_4326" in df.columns and "lat" not in df.columns:
        df = df.rename(columns={"lat_y_4326":"lat"})
    if "long_x_4326" in df.columns and "lon" not in df.columns:
        df = df.rename(columns={"long_x_4326":"lon"})

    canonical = ["station_id","station_name","id_arpa","inizio_operativita","fine_operativita","inquinanti","lon","lat","location"]
    for c in canonical:
        if c not in df.columns:
            df[c] = pd.NA

    return df[canonical]

# -----------------------
# MAIN
# -----------------------
def build_db():
    tmp_files: List[str] = []
    try:
        # DOWNLOAD
        f1 = download_to_temp(DS573_JSON)
        tmp_files.append(f1)
        f2 = download_to_temp(DS407_2024_JSON)
        tmp_files.append(f2)
        f3 = download_to_temp(STATIONS_CSV)
        tmp_files.append(f3)

        print("Lettura file JSON/CSV scaricati...")
        df1 = read_json_flexible(f1)
        df2 = read_json_flexible(f2)
        stations_raw = read_csv_flexible(f3)

        print("Normalizzazione dataset misure (serie storica)...")
        m1 = normalize_measurements(df1)
        print(f"Records validi ds573: {len(m1)}")

        print("Normalizzazione dataset misure (2024)...")
        m2 = normalize_measurements(df2)
        print(f"Records validi ds407: {len(m2)}")

        # concat e cleanup
        all_measures = pd.concat([m1, m2], ignore_index=True, sort=False)
        all_measures["datetime"] = pd.to_datetime(all_measures["datetime"], errors="coerce")
        all_measures["date"] = all_measures["date"].astype(str)

        # normalize stations
        stations_norm = normalize_stations(stations_raw)

        # save CSVs
        all_measures.to_csv(CSV_OUT, index=False)
        stations_norm.to_csv(STATIONS_OUT, index=False)
        print(f"Salvati: {CSV_OUT} ({len(all_measures)} righe), {STATIONS_OUT} ({len(stations_norm)} righe)")

        # create sqlite db
        if os.path.exists(DB_OUT):
            os.remove(DB_OUT)
        conn = sqlite3.connect(DB_OUT)
        all_measures.to_sql("measurements", conn, index=False)
        stations_norm.to_sql("stations", conn, index=False)
        conn.commit()
        conn.close()
        print("SQLite DB creato:", DB_OUT)

    finally:
        for t in tmp_files:
            try:
                os.remove(t)
            except Exception:
                pass

if __name__ == "__main__":
    build_db()
