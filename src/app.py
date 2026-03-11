# src/app.py
"""
Applicazione Streamlit: Analisi qualità aria - Milano
Requisiti: leggere tutti i file in src/data (JSON/CSV/GeoJSON) e produrre:
 - andamento 10 anni (media annuale)
 - classifica stazioni (media ultimi 10 anni) se disponibili
 - andamento ultimo anno per singola stazione (giornaliero / mensile)
 - spiegazioni in sidebar per inquinanti principali
"""

import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import json
import os
import glob
from datetime import datetime

st.set_page_config(page_title="Qualità aria - Milano (student)", layout="wide")

# ---------------------------
# Titolo + descrizione
# ---------------------------
st.title("Analisi della qualità dell'aria — Milano")
st.markdown(
    "App didattica che analizza dataset Open Data Comune di Milano. "
    "Carica tutti i file presenti nella cartella `src/data` e costruisce "
    "analisi annuali e per stazione."
)

# ---------------------------
# Percorso cartella dati (robusto)
# ---------------------------
# Se l'app è lanciata da src/, questa linea trova la cartella src/data
DATA_FOLDER = os.path.join(os.path.dirname(__file__), "data")
# ---------------------------
# Utils per lettura file
# ---------------------------
def try_load_json(path):
    """Legge un JSON; ritorna lista (records) o None."""
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        # se il file è un GeoJSON (has 'features'), ritorniamo features
        if isinstance(data, dict) and "features" in data:
            return data["features"]
        # se è un record dict con chiave 'result' o 'records' (Socrata-style)
        if isinstance(data, dict) and "result" in data and "records" in data["result"]:
            return data["result"]["records"]
        if isinstance(data, dict) and "records" in data:
            return data["records"]
        # se è già una lista
        if isinstance(data, list):
            return data
        # fallback: ritorna dict avvolto in lista
        return [data]
    except Exception:
        return None

def try_read_csv(path):
    """Legge un CSV con alcune strategie e ritorna DataFrame o None."""
    try:
        return pd.read_csv(path)
    except Exception:
        try:
            return pd.read_csv(path, sep=';')
        except Exception:
            try:
                return pd.read_json(path)
            except Exception:
                return None

# ---------------------------
# Caricamento e merging dei file
# ---------------------------
all_records = []
station_geo = None
csv_stations_df = None
files_found = []

if not os.path.isdir(DATA_FOLDER):
    st.error("Cartella dati non trovata. Metti i file JSON/CSV/geojson in src/data e riavvia.")
    st.stop()

# cerchiamo file json / csv / geojson
for ext in ("*.json", "*.geojson", "*.csv"):
    for p in glob.glob(os.path.join(DATA_FOLDER, ext)):
        files_found.append(p)

if not files_found:
    st.error("Non ci sono file nella cartella data. Carica i JSON/CSV/GeoJSON e riavvia.")
    st.stop()

# Leggiamo i file con heuristics
json_count = 0
csv_count = 0
for file_path in files_found:
    lower = file_path.lower()
    if lower.endswith(".geojson"):
        # carichiamo il geojson delle stazioni (utile)
        try:
            with open(file_path, encoding="utf-8") as f:
                station_geo = json.load(f)
        except Exception:
            station_geo = None
    elif lower.endswith(".csv"):
        # potenziale file anagrafica stazioni
        df = try_read_csv(file_path)
        if isinstance(df, pd.DataFrame):
            # riconosciamo file stazioni da colonne tipiche
            cols_lower = [c.lower() for c in df.columns]
            if any("staz" in c or "station" in c or "nome" in c for c in cols_lower):
                csv_stations_df = df
            else:
                # fallback: prova a convertire in records
                all_records.extend(df.to_dict(orient="records"))
        csv_count += 1
    else:
        # JSON generico
        recs = try_load_json(file_path)
        if recs:
            # Alcuni JSON contengono singoli oggetti non list; normalizziamo
            for r in recs:
                if isinstance(r, dict):
                    all_records.append(r)
                else:
                    # se è str o altro, skip
                    pass
        json_count += 1

# ---------------------------
# Creazione DataFrame principale
# ---------------------------
if not all_records:
    st.warning("Non sono stati letti record dai JSON. Verifica i file e ricarica.")
    st.stop()

df_raw = pd.DataFrame(all_records)

# ---------------------------
# Normalizzazione colonne utili
# Cerchiamo le colonne più comuni e creiamo colonne canoniche:
# - date / datetime -> 'date'
# - pollutant (inquinante) -> 'pollutant'
# - station id or name -> 'station_id', 'station_name'
# - value -> 'value'
# ---------------------------
def find_col(cols, keywords):
    """ritorna la prima colonna che contiene una qualsiasi delle keywords"""
    cols_low = [c.lower() for c in cols]
    for kw in keywords:
        for i, c in enumerate(cols_low):
            if kw in c:
                return cols[i]
    return None

cols = df_raw.columns.tolist()

date_col = find_col(cols, ["data", "date", "giorno", "timestamp"])
pollutant_col = find_col(cols, ["inquin", "param", "pollut", "nome", "indicator"])
value_col = find_col(cols, ["valore", "value", "concentrazione", "inquinanti_aria", "media"])
station_col = find_col(cols, ["staz", "station", "centralina", "id_stazione", "nomecentralina", "codice"])

# Se non troviamo, proviamo fallback
if date_col is None:
    # a volte la data è in 'anno' + 'mese' etc ; useremo 'anno' se presente
    if "anno" in [c.lower() for c in cols]:
        date_col = "anno"

# Creiamo nuovo DataFrame con colonne canoniche (quando possibile)
df = df_raw.copy()

# date -> date/datetime
if date_col and date_col in df.columns:
    try:
        df["date"] = pd.to_datetime(df[date_col], errors="coerce")
    except Exception:
        df["date"] = pd.to_datetime(df[date_col].astype(str), errors="coerce")
else:
    df["date"] = pd.NaT

# pollutant
if pollutant_col and pollutant_col in df.columns:
    df["pollutant"] = df[pollutant_col].astype(str)
else:
    # prova colonne specifiche usate prima
    if "inquinanti_aria_tipologia" in df.columns:
        df["pollutant"] = df["inquinanti_aria_tipologia"].astype(str)
    else:
        df["pollutant"] = df.get("parametro", df.get("nome", pd.NA)).astype(str)

# value
if value_col and value_col in df.columns:
    df["value"] = pd.to_numeric(df[value_col], errors="coerce")
else:
    # prova colonne numeriche: prendi la prima colonna numerica
    numcols = df.select_dtypes(include=[np.number]).columns.tolist()
    if numcols:
        df["value"] = pd.to_numeric(df[numcols[0]], errors="coerce")
    else:
        df["value"] = pd.NA

# station id / name
if station_col and station_col in df.columns:
    df["station_id"] = df[station_col].astype(str)
    # se esiste un nome centralina, usalo
    if "nomecentralina" in df.columns:
        df["station_name"] = df["nomecentralina"].astype(str)
else:
    # fallback: prova colonne comuni
    if "nomecentralina" in df.columns:
        df["station_name"] = df["nomecentralina"].astype(str)
        df["station_id"] = df["nomecentralina"].astype(str)
    else:
        df["station_id"] = df.get("id", pd.NA)
        df["station_name"] = df.get("nome", pd.NA)

# aggiungi anno per aggregazioni
df["year"] = pd.to_numeric(df["date"].dt.year, errors="coerce").astype("Int64")

# togli righe senza valore/pollutant
df = df.dropna(subset=["value", "pollutant"])

# ---------------------------
# Separa dataframe indicatori (serie storica di indicatori annuali) e dati per stazione
# Heuristics:
# - indicatori: esistono colonne 'indicator' o 'inquinanti_aria_indicatori'
# - dati per stazione: esiste 'station_id' significativo e 'date' pieno
# ---------------------------
df_indicators = pd.DataFrame()
df_station = pd.DataFrame()

if "inquinanti_aria_indicatori" in df.columns or "indicator" in [c.lower() for c in df.columns]:
    # proviamo a prendere indicatori annuali
    if "inquinanti_aria_indicatori" in df.columns:
        df_indicators = df.rename(columns={"inquinanti_aria_indicatori":"indicator"})
    elif "indicator" in df.columns:
        df_indicators = df.copy()
    # se indicatori hanno 'year' e 'value' allora ok
    if not set(["year","pollutant","value"]).issubset(df_indicators.columns):
        df_indicators = pd.DataFrame()
else:
    # fallback: alcuni dataset annuali hanno 'year' e 'pollutant' già
    if "year" in df.columns and "pollutant" in df.columns and df["date"].isna().all():
        df_indicators = df.copy()

# dati per stazione: dobbiamo avere date valide e station_id
if df["date"].notna().sum() > 0 and df["station_id"].notna().sum() > 0:
    df_station = df[df["station_id"].notna() & df["date"].notna()].copy()
    df_station["year"] = pd.to_numeric(df_station["date"].dt.year, errors="coerce").astype("Int64")

# Se non abbiamo df_indicators (es: ds573 non era presente) ma abbiamo df_station, possiamo costruire indicatori aggregando per year
if df_indicators.empty and not df_station.empty:
    st.info("Dataset indicatori annuali non trovato - verranno calcolate medie annuali aggregando i dati per stazione")
    df_indicators = df_station.groupby(["year","pollutant"], dropna=True)["value"].mean().reset_index()
    # segnaliamo che questi indicatori sono aggregati da misure per stazione

# ---------------------------
# Sidebar: spiegazioni inquinanti
# ---------------------------
st.sidebar.header("Informazioni sugli inquinanti")
pollutant_info = {
    "PM10": "Particelle con diametro ≤10µm. Possono penetrare nelle vie respiratorie.",
    "PM2.5": "Particelle con diametro ≤2.5µm. Penetrazione profonda e rischi cardiovascolari.",
    "NO2": "Biossido di azoto: da traffico e combustioni; irritante per le vie respiratorie.",
    "O3": "Ozono troposferico: inquinante secondario, misurato spesso con superamenti giornalieri."
}
for name, desc in pollutant_info.items():
    st.sidebar.markdown(f"**{name}** — {desc}")

# ---------------------------
# UI: selettori principali
# ---------------------------
# Pollutant options: union of both dfs
polls = sorted(pd.unique(pd.concat([
    df_indicators["pollutant"] if not df_indicators.empty else pd.Series([], dtype=str),
    df_station["pollutant"] if not df_station.empty else pd.Series([], dtype=str)
]).dropna()))

if not polls:
    st.error("Non ci sono valori di inquinante validi.")
    st.stop()

sel_pollutant = st.selectbox("Seleziona inquinante", polls)

# anni disponibili
if not df_indicators.empty:
    years = sorted(df_indicators["year"].dropna().astype(int).unique())
else:
    years = sorted(df_station["year"].dropna().astype(int).unique()) if not df_station.empty else []

last_year = max(years) if years else None
last_10_years = [y for y in years if last_year is not None and y >= (last_year - 9)]

# ---------------------------
# Analisi 1: andamento 10 anni (media annuale)
# ---------------------------
st.header("Andamento 10 anni — media annuale")

# preferiamo indicatori ufficiali, fallback a aggregazione su df_station
if not df_indicators.empty:
    df_10 = df_indicators[df_indicators["pollutant"] == sel_pollutant].copy()
    # cerchiamo righe con "media" nel nome dell'indicatore, altrimenti prendiamo ciò che c'è
    if "indicator" in df_10.columns:
        media_rows = df_10[df_10["indicator"].str.contains("media", case=False, na=False)]
        if not media_rows.empty:
            df_10 = media_rows
else:
    df_10 = pd.DataFrame()

# fallback se vuoto -> aggregazione dal df_station
if df_10.empty and not df_station.empty:
    agg = df_station[df_station["pollutant"] == sel_pollutant].groupby("year")["value"].mean().reset_index()
    agg = agg[agg["year"].isin(last_10_years)]
    if not agg.empty:
        df_plot = agg.set_index("year")["value"]
    else:
        df_plot = pd.Series(dtype=float)
else:
    # usa df_10 (assicurati abbia 'year' e 'value')
    if not df_10.empty and set(["year","value"]).issubset(df_10.columns):
        df_plot = df_10[df_10["year"].isin(last_10_years)].sort_values("year").set_index("year")["value"]
    else:
        df_plot = pd.Series(dtype=float)

if df_plot.empty:
    st.info("Nessun dato annuale disponibile per l'inquinante selezionato negli ultimi 10 anni.")
else:
    fig, ax = plt.subplots(figsize=(9,4))
    ax.plot(df_plot.index.astype(int), df_plot.values, marker="o", linestyle="-")
    ax.set_xlabel("Anno")
    ax.set_ylabel("Valore (unità misurate)")
    ax.set_title(f"Andamento medio annuale — {sel_pollutant}")
    ax.grid(True)
    st.pyplot(fig)

    # trend lineare semplice
    try:
        coeff = np.polyfit(df_plot.index.astype(int), df_plot.values, 1)[0]
        if coeff < -0.01:
            trend_text = "in diminuzione"
        elif coeff > 0.01:
            trend_text = "in aumento"
        else:
            trend_text = "stabile"
        st.write(f"Tendenza stimata (semplice fit lineare): **{trend_text}**")
    except Exception:
        pass

# ---------------------------
# Analisi 2: classifica stazioni (media 10 anni)
# ---------------------------
st.header("Classifica stazioni — media ultimi 10 anni")

if not df_station.empty:
    df_last10 = df_station[df_station["year"].isin(last_10_years)]
    agg_station = df_last10[df_last10["pollutant"] == sel_pollutant].groupby("station_id")["value"].mean().dropna().sort_values(ascending=False)
    if agg_station.empty:
        st.info("Non ci sono dati per le stazioni negli ultimi 10 anni per questo inquinante.")
    else:
        top5 = agg_station.head(5)
        st.subheader("Top 5 stazioni (media ultimi 10 anni)")
        st.bar_chart(top5)
        st.write("Tabella completa (prime 50):")
        st.dataframe(agg_station.head(50).reset_index().rename(columns={"value":"media_10y"}))
        # se abbiamo csv_stations_df o geojson, mostriamo nomi vicini
        if csv_stations_df is not None:
            merged_top = top5.reset_index().merge(csv_stations_df.rename(columns={csv_stations_df.columns[0]:"station_id"}), on="station_id", how="left")
            st.write("Top5 con informazioni stazioni (se disponibili):")
            st.dataframe(merged_top)
else:
    st.info("Dataset misure per stazione non disponibile: non è possibile calcolare la classifica per stazioni.")

# ---------------------------
# Analisi 3: andamento ultimo anno per stazione
# ---------------------------
st.header("Andamento ultimo anno — per singola stazione")

if not df_station.empty:
    stations_list = sorted(df_station["station_id"].dropna().unique())
    selected_station = st.selectbox("Seleziona stazione", stations_list)
    if selected_station:
        df_s = df_station[(df_station["station_id"]==selected_station) & (df_station["pollutant"]==sel_pollutant)].copy()
        if df_s.empty:
            st.info("Nessun dato per questa stazione e inquinante.")
        else:
            # ultimo anno disponibile per questa stazione
            ly = int(df_s["year"].max())
            df_s_last = df_s[df_s["year"]==ly].set_index("date").sort_index()
            agg_mode = st.radio("Aggregazione per grafico", ("Mensile", "Giornaliera"))
            if agg_mode == "Mensile":
                ts = df_s_last.resample("M")["value"].mean()
            else:
                ts = df_s_last.resample("D")["value"].mean()

            fig2, ax2 = plt.subplots(figsize=(10,3))
            ax2.plot(ts.index, ts.values, marker="o", linestyle='-')
            ax2.set_title(f"Andamento {sel_pollutant} — Stazione {selected_station} ({ly})")
            ax2.set_ylabel("Valore")
            ax2.set_xlabel("Data")
            ax2.grid(True)
            st.pyplot(fig2)

            # evidenzia picchi
            mean_ts = ts.mean()
            std_ts = ts.std()
            peaks = ts[ts > (mean_ts + 2*std_ts)]
            if not peaks.empty:
                st.markdown("**Picchi (valori > mean + 2*std):**")
                st.write(peaks.reset_index().rename(columns={0:"valore"}))
else:
    st.info("Dataset per stazione non disponibile: la sezione dell'andamento per stazione non può essere mostrata.")
