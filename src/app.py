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
AIR_DATA_URL = "https://dati.comune.milano.it/your-air-data-endpoint.json"
STATIONS_URL = "https://dati.comune.milano.it/your-stations-endpoint.json"

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
    # Esempio robusto: cerco colonne tipiche e normalizzo
    # ATTENZIONE: adattare alle colonne reali del JSON del Comune di Milano
    df = df.copy()
    # Normalizza nomi colonne se necessario
    lower_cols = {c: c.lower() for c in df.columns}
    df.rename(columns=lower_cols, inplace=True)

    # ipotesi: colonne 'data', 'valore', 'inquinante', 'id_stazione'
    # tentativo di trovare colonne equivalenti
    possible_date_cols = [c for c in df.columns if 'date' in c or 'data' in c or 'giorno' in c]
    if possible_date_cols:
        date_col = possible_date_cols[0]
        df['date'] = pd.to_datetime(df[date_col], errors='coerce')
    else:
        st.error("Colonna data non trovata nel dataset: adattare preprocess_air_data.")
        return pd.DataFrame()

    # value column
    possible_value_cols = [c for c in df.columns if 'val' in c or 'value' in c or 'concent' in c or 'misur' in c]
    if possible_value_cols:
        df['value'] = pd.to_numeric(df[possible_value_cols[0]], errors='coerce')
    else:
        st.error("Colonna valore non trovata nel dataset: adattare preprocess_air_data.")
        return pd.DataFrame()

    # pollutant
    possible_pollutant_cols = [c for c in df.columns if 'inquin' in c or 'pollut' in c or 'param' in c]
    if possible_pollutant_cols:
        df['pollutant'] = df[possible_pollutant_cols[0]].astype(str)
    else:
        # prova a derivare dal nome colonna o impostare 'PM10' se unico
        df['pollutant'] = df.get('parameter', np.nan).astype(str)

    # station id
    possible_station_cols = [c for c in df.columns if 'staz' in c or 'station' in c or 'id' == c]
    if possible_station_cols:
        df['station_id'] = df[possible_station_cols[0]]
    else:
        df['station_id'] = df.get('id_stazione', np.nan)

    # year column
    df['year'] = df['date'].dt.year

    # drop missing dates or values
    df = df.dropna(subset=['date', 'value', 'pollutant', 'station_id'])

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
