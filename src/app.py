# src/app.py
import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime
from urllib.error import URLError

st.set_page_config(page_title="Analisi qualità aria - Milano", layout="wide")

# ---------------------------
# CONFIG: inserisci gli URL JSON del PDF qui (o usa file locale in data/)
# ---------------------------
# NOTE: sostituisci con gli URL esatti forniti nel PDF (vedi file caricato).
AIR_DATA_URL = "https://dati.comune.milano.it/dataset/ad529de1-8398-43e9-bba5-c5012513f23f/resource/eade3387-bff8-4de0-ac24-09360f38ded7/download/ds573_inquinanti_aria.json"

STATIONS_URL = "https://dati.comune.milano.it/dataset/d6960c75-0a02-4fda-a85f-3b1c4aa725d6/resource/b301f327-7504-4efc-8b4a-5f4a29f9d0ff/download/qaria_stazione.csv"

# Fallback su file locale (se presenti)
LOCAL_AIR_JSON = "data/air_data.json"
LOCAL_STATIONS_JSON = "data/stations.json"

# ---------------------------
# HELPERS: caricamento e pulizia
# ---------------------------
@st.cache_data(ttl=3600)
def load_json_from_url_or_file(url, local_path=None):
    try:
        if url and "http" in url:
            df = pd.read_json(url)
            # Se la struttura JSON è annidata, potresti dover fare pd.json_normalize
            return df
    except Exception as e:
        st.write(f"Errore caricamento da URL: {e}")
    if local_path:
        try:
            return pd.read_json(local_path)
        except Exception as e:
            st.warning(f"Impossibile caricare {local_path}: {e}")
    raise URLError("Impossibile caricare il dataset (URL e file locale falliti).")

def preprocess_air_data(df):
    df = df.copy()

    # Rinomina colonne reali del dataset ufficiale
    df.rename(columns={
        "data": "date",
        "nomecentralina": "station_name",
        "inquinante": "pollutant",
        "valore": "value"
    }, inplace=True)

    # Converte data
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["year"] = df["date"].dt.year

    return df

@st.cache_data(ttl=3600)
def load_and_prepare(aurl=AIR_DATA_URL, surl=STATIONS_URL):
    air_df = None
    try:
        air_df = load_json_from_url_or_file(aurl, LOCAL_AIR_JSON)
    except Exception as e:
        st.warning("Non è stato possibile caricare il dataset aria da URL. Prova a fornire un file locale nella cartella data/")
        raise e
    stations_df = None
    try:
        stations_df = load_json_from_url_or_file(surl, LOCAL_STATIONS_JSON)
    except Exception:
        stations_df = pd.DataFrame()  # ok, user may not have stations dataset

    air_df_clean = preprocess_air_data(air_df)
    # se stations_df contiene info utili, unirla
    if not stations_df.empty:
        # tenta di trovare id di stazione e nome
        stations_df.columns = [c.lower() for c in stations_df.columns]
        if 'id' in stations_df.columns and 'name' in stations_df.columns:
            stations_df = stations_df.rename(columns={'id': 'station_id', 'name': 'station_name'})
        # join
        merged = air_df_clean.merge(stations_df, on='station_id', how='left')
    else:
        merged = air_df_clean
    return merged

# ---------------------------
# VISUALS / ANALISI
# ---------------------------

def plot_10y_trend(df, pollutant):
    last_10y = sorted(df['year'].unique())[-10:]
    df10 = df[df['year'].isin(last_10y) & (df['pollutant'] == pollutant)]
    if df10.empty:
        st.warning("Nessun dato per questo inquinante negli ultimi 10 anni.")
        return
    annual = df10.groupby('year')['value'].mean().reset_index()
    fig, ax = plt.subplots(figsize=(8,3))
    ax.plot(annual['year'], annual['value'], marker='o')
    ax.set_title(f"Andamento medio annuale ({pollutant})")
    ax.set_xlabel("Anno")
    ax.set_ylabel("Valore medio")
    st.pyplot(fig)

def plot_station_ranking(df, pollutant, top_n=5):
    dfp = df[df['pollutant'] == pollutant]
    mean_station = dfp.groupby('station_id')['value'].mean().reset_index().sort_values('value', ascending=False)
    top = mean_station.head(top_n)
    fig, ax = plt.subplots(figsize=(8,3))
    ax.bar(top['station_id'].astype(str), top['value'])
    ax.set_title(f"Top {top_n} stazioni per media 10 anni ({pollutant})")
    ax.set_xlabel("Station ID")
    ax.set_ylabel("Valore medio")
    st.pyplot(fig)
    st.dataframe(mean_station.head(50).rename(columns={'value':'media_valore'}))

def plot_last_year(df, pollutant, station_id, freq='M'):
    last_year = df['year'].max()
    dfy = df[(df['year'] == last_year) & (df['pollutant'] == pollutant) & (df['station_id'] == station_id)]
    if dfy.empty:
        st.warning("Nessun dato per la combinazione selezionata nell'ultimo anno disponibile.")
        return
    ts = dfy.set_index('date').resample(freq)['value'].mean()
    fig, ax = plt.subplots(figsize=(10,3))
    ax.plot(ts.index, ts.values, marker='o')
    ax.set_title(f"Andamento {pollutant} - Stazione {station_id} ({last_year})")
    ax.set_xlabel("Data")
    ax.set_ylabel("Valore medio")
    st.pyplot(fig)

# ---------------------------
# MAIN STREAMLIT APP
# ---------------------------

st.title("Analisi della qualità dell'aria a Milano")
st.markdown("""
Applicazione per analizzare i dataset di qualità dell'aria (10 anni), confrontare stazioni,
vedere l'andamento dell'ultimo anno, e comprendere il significato degli inquinanti.
""")

with st.expander("I dataset (sorgenti) / Nota"):
    st.write("Usare gli URL JSON del Comune di Milano come indicato nel brief / PDF caricato.")
    st.write("Referenza al brief caricato: il PDF contiene i link ai dataset JSON. :contentReference[oaicite:2]{index=2}")
    st.write("- Se i dataset non sono accessibili via rete, salvarli in `data/` e riprovare.")

# caricamento dati (senza chiedere: uso tentativo con gestione error)
try:
    df_all = load_and_prepare()
except Exception as e:
    st.error("Errore nel caricamento/preparazione dati: verifica gli URL o i file locali nella cartella data/.")
    st.stop()

# select pollutant
pollutants = sorted(df_all['pollutant'].unique())
pollutant = st.sidebar.selectbox("Seleziona inquinante", pollutants)

# pollutant descriptions richieste nel progetto (esempio minimo)
st.sidebar.markdown("### Cos'è e perché è pericoloso (esempi):")
pollutant_info = {
    "NO2": ("Biossido di azoto (NO₂)", "Gas prodotto da combustione fossile: irritante per vie respiratorie, aumenta rischio cardiovascolare."),
    "PM10": ("Particolato PM10", "Particelle ≤10µm: penetrazione nelle vie respiratorie, problemi respiratori e cardiaci."),
    "PM2.5": ("Particolato PM2.5", "Particelle ≤2.5µm: penetrano in profondità nei polmoni e nel sangue; correlate a mortalità prematura.")
}
if pollutant in pollutant_info:
    st.sidebar.write(f"**{pollutant_info[pollutant][0]}**")
    st.sidebar.write(pollutant_info[pollutant][1])
else:
    # mostra descrizione generica
    st.sidebar.write("Informazioni sull'inquinante selezionato. (Aggiungi testo personalizzato nel codice.)")

# mostra andamento 10 anni
st.header("Andamento últimos 10 anni")
plot_10y_trend(df_all, pollutant)

# ranking stazioni (media 10 anni)
st.header("Classifica stazioni (media 10 anni)")
plot_station_ranking(df_all, pollutant, top_n=5)

# selezione stazione per andamento ultimo anno
st.header("Andamento ultimo anno per stazione")
stations = sorted(df_all['station_id'].astype(str).unique())
selected_station = st.selectbox("Seleziona stazione", stations)
freq = st.radio("Frequenza aggregazione", options=['D', 'M'], format_func=lambda x: "Giornaliera" if x=='D' else "Mensile")
plot_last_year(df_all, pollutant, selected_station, freq=freq)

st.markdown("### Domande guida (da mostrare nell'app):")
st.markdown("""
- L'inquinamento è aumentato o diminuito? (controlla la pendenza nell'andamento 10 anni)
- Ci sono anni con valori anomali? (ricerca picchi o outlier)
- L'andamento ultimo anno è regolare o variabile? (vedi grafico mensile/giornaliero)
- Questo inquinante è stagionale? (confronta mesi)
""")

st.info("Consegna: cartella progetto con app.py e file di supporto; app Streamlit funzionante; codice commentato come quello qui.")
