# src/app.py
import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import sqlite3
from urllib.error import URLError

st.set_page_config(page_title="Analisi qualità aria - Milano", layout="wide")

st.title("Analisi della qualità dell'aria a Milano")
st.markdown("Applicazione per analizzare i dataset ufficiali del Comune di Milano — media 10 anni, classifica stazioni, andamento ultimo anno, e spiegazioni per i cittadini. (Fonti dati: Comune di Milano).")

# ----------------------------
# URL ufficiali (se vuoi cambiare, modifica qui)
# ----------------------------
MEASURES_JSON_1 = "https://dati.comune.milano.it/dataset/ad529de1-8398-43e9-bba5-c5012513f23f/resource/eade3387-bff8-4de0-ac24-09360f38ded7/download/ds573_inquinanti_aria.json"
MEASURES_JSON_2 = "https://dati.comune.milano.it/dataset/dba6b6ff-792b-471d-9a2c-a625f1398f5f/resource/bcef81c8-4011-4225-93ee-f284387e8834/download/qaria_datoariagiornostazione_2024-12-24.json"
STATIONS_CSV = "https://dati.comune.milano.it/dataset/d6960c75-0a02-4fda-a85f-3b1c4aa725d6/resource/b301f327-7504-4efc-8b4a-5f4a29f9d0ff/download/qaria_stazione.csv"

# ----------------------------
# Helper: lettura flessibile JSON/CSV
# ----------------------------
@st.cache_data(ttl=3600)
def read_json_flexible(url):
    try:
        df = pd.read_json(url)
        return df
    except Exception:
        try:
            return pd.read_json(url, orient="records")
        except Exception as e:
            raise URLError(f"Impossibile leggere JSON da {url}: {e}")

@st.cache_data(ttl=3600)
def read_csv_flexible(url):
    return pd.read_csv(url)

# ----------------------------
# Normalizzazione (semplice ma robusta)
# ----------------------------
def normalize_measurements(df):
    df = df.copy()
    df.columns = [c.lower() for c in df.columns]

    # Cerca colonne possibili per date, valore, pollutante e stazione
    date_cols = [c for c in df.columns if "date" in c or "data" in c or "giorno" in c]
    val_cols = [c for c in df.columns if c in ("valore","value","concentrazione","inquinanti_aria")]
    poll_cols = [c for c in df.columns if "inquin" in c or "param" in c or "indicator" in c]
    st_cols = [c for c in df.columns if "staz" in c or "station" in c or "cod" in c]

    # map
    if date_cols:
        df["datetime"] = pd.to_datetime(df[date_cols[0]], errors="coerce")
    else:
        df["datetime"] = pd.NaT

    if val_cols:
        df["value"] = pd.to_numeric(df[val_cols[0]], errors="coerce")
    else:
        # try numeric first numeric col
        num = df.select_dtypes(include=[np.number]).columns
        df["value"] = df[num[0]] if len(num)>0 else pd.NA

    if poll_cols:
        df["pollutant"] = df[poll_cols[0]].astype(str)
    else:
        df["pollutant"] = df.get("inquinanti_aria_tipologia", df.get("nome", pd.NA)).astype(str)

    if st_cols:
        df["station_id"] = df[st_cols[0]].astype(str)
    else:
        # fallback: sometimes there is 'centralina' o 'nomecentralina'
        if "nomecentralina" in df.columns:
            df["station_name"] = df["nomecentralina"].astype(str)
            df["station_id"] = df["nomecentralina"].astype(str)
        else:
            df["station_id"] = df.get("id", pd.NA)

    # create year
    df["date"] = pd.to_datetime(df["datetime"], errors="coerce")
    df["year"] = df["date"].dt.year

    # keep essential
    df = df.dropna(subset=["value","pollutant"], how="any")
    wanted = ["date","datetime","year","station_id","station_name","pollutant","value"]
    for c in wanted:
        if c not in df.columns:
            df[c] = pd.NA
    return df[wanted]

def normalize_stations(df):
    df = df.copy()
    df.columns = [c.lower() for c in df.columns]
    if "id" in df.columns and "station_id" not in df.columns:
        df = df.rename(columns={"id":"station_id"})
    if "nome" in df.columns and "station_name" not in df.columns:
        df = df.rename(columns={"nome":"station_name"})
    # ensure canonical
    for c in ["station_id","station_name","lat","lon","station_type"]:
        if c not in df.columns:
            df[c] = pd.NA
    return df[["station_id","station_name","lat","lon","station_type"]]

# ----------------------------
# Caricamento & preparazione dati (merge)
# ----------------------------
@st.cache_data(ttl=3600)
def load_and_prepare():
    # Provo a leggere prima il dataset di misure giornaliere (più ricco)
    try:
        m2 = read_json_flexible(MEASURES_JSON_2)
        meas2 = normalize_measurements(m2)
    except Exception:
        meas2 = pd.DataFrame()

    # Provo serie storica / indicatori
    try:
        m1 = read_json_flexible(MEASURES_JSON_1)
        meas1 = normalize_measurements(m1)
    except Exception:
        meas1 = pd.DataFrame()

    # preferri usare il dataset più dettagliato (m2), altrimenti m1
    if not meas2.empty:
        measures = pd.concat([meas2, meas1], ignore_index=True, sort=False)
    else:
        measures = meas1.copy()

    # Stations
    stations = read_csv_flexible(STATIONS_CSV)
    stations = normalize_stations(stations)

    # join measures <-> stations se station_id esiste
    if "station_id" in measures.columns and not stations.empty:
        merged = measures.merge(stations, on="station_id", how="left")
    else:
        merged = measures

    # pulizie finali
    merged["year"] = pd.to_numeric(merged["year"], errors="coerce").astype('Int64')
    merged["value"] = pd.to_numeric(merged["value"], errors="coerce")
    merged = merged.dropna(subset=["value","pollutant"])
    return merged, stations

# ----------------------------
# Carico dati
# ----------------------------
try:
    df_all, stations_df = load_and_prepare()
    st.success("Dataset caricato correttamente ✅")
except Exception as e:
    st.error(f"Errore caricamento dati: {e}")
    st.stop()

# ----------------------------
# Sezione: spiegazione inquinanti (consegna)
# ----------------------------
st.sidebar.header("Informazioni sugli inquinanti")
pollutant_info = {
    "Polveri sottili - PM10": ("PM10", "Particelle con diametro ≤10µm. Entrano nelle vie respiratorie; aumentano malattie respiratorie e cardiovascolari."),
    "Particolato - PM2.5": ("PM2.5", "Particelle ≤2.5µm. Penetrano profondamente nei polmoni e nel sangue; correlate a mortalità prematura."),
    "Biossido di azoto - NO2": ("NO2", "Gas prodotto dalle combustioni; irrita le vie respiratorie, peggiora asma e problemi cardiovascolari."),
    "Ozono - O3": ("O3", "Formazione secondaria in atmosfera; irritante e dannoso per respirazione e vegetazione.")
}
# mostra descrizioni
for k, v in pollutant_info.items():
    st.sidebar.markdown(f"**{k}** — {v[1]}")

# ----------------------------
# UI: selettori principali
# ----------------------------
pollutants = sorted(df_all['pollutant'].unique())
sel_pollutant = st.selectbox("Seleziona inquinante", pollutants)

# filtro anni disponibili e ultimi 10 anni
years = df_all['year'].dropna().unique()
years = np.sort(years)
last_year = int(np.nanmax(years)) if len(years)>0 else None
last_10 = [y for y in years if y >= (last_year - 9)] if last_year else years

# ----------------------------
# Analisi 1: andamento 10 anni (media annuale)
# ----------------------------
st.header("Andamento 10 anni (media annuale)")

df_poll = df_all[df_all['pollutant'] == sel_pollutant].copy()
if df_poll.empty:
    st.warning("Nessun dato per l'inquinante selezionato.")
else:
    # media annuale
    annual = df_poll.groupby('year')['value'].mean().reindex(sorted(last_10)).dropna()
    if annual.empty:
        st.info("Nessun dato annuale disponibile per gli ultimi 10 anni.")
    else:
        fig, ax = plt.subplots(figsize=(9,4))
        ax.plot(annual.index.astype(int), annual.values, marker='o', linestyle='-')
        ax.set_xlabel("Anno")
        ax.set_ylabel("Valore (unità misurate)")
        ax.set_title(f"Andamento medio annuale — {sel_pollutant}")
        ax.grid(True)
        st.pyplot(fig)

        # Risposte brevi automatiche
        trend = np.polyfit(annual.index.astype(int), annual.values, 1)[0]
        if trend < -0.01:
            st.write("Tendenza: **in diminuzione** negli ultimi 10 anni.")
        elif trend > 0.01:
            st.write("Tendenza: **in aumento** negli ultimi 10 anni.")
        else:
            st.write("Tendenza: **stabile** negli ultimi 10 anni.")
        st.write("- Anni con possibili valori anomali:", list(annual[annual > (annual.mean()+2*annual.std())].index))

# ----------------------------
# Analisi 2: classifica stazioni (media 10 anni)
# ----------------------------
st.header("Classifica stazioni — media 10 anni")

# calcolo media 10 anni per stazione (filtro su ultimi 10)
df_last10 = df_all[df_all['year'].isin(last_10)]
agg_station = df_last10[df_last10['pollutant'] == sel_pollutant].groupby('station_id')['value'].mean().dropna().sort_values(ascending=False)
if agg_station.empty:
    st.info("Nessun dato stazioni per l'inquinante selezionato negli ultimi 10 anni.")
else:
    top5 = agg_station.head(5)
    st.bar_chart(top5)  # grafico rapido
    st.write("Tabella media 10 anni (prime 50):")
    st.dataframe(agg_station.head(50).reset_index().rename(columns={"value":"media_10y"}))

# ----------------------------
# Analisi 3: andamento ultimo anno per stazione
# ----------------------------
st.header("Andamento dell'ultimo anno per stazione")
station_options = sorted(df_all['station_id'].dropna().unique())
sel_station = st.selectbox("Seleziona stazione", station_options)

if sel_station:
    df_station = df_all[(df_all['station_id']==sel_station) & (df_all['pollutant']==sel_pollutant)].copy()
    if df_station.empty:
        st.warning("Nessun dato per questa stazione + inquinante.")
    else:
        # ultimo anno disponibile per la stazione
        ly = int(df_station['year'].max())
        df_ly = df_station[df_station['year']==ly].set_index('date').sort_index()
        # prova aggregazione mensile se troppi punti
        resample_mode = st.radio("Aggregazione", options=['Mensile','Giornaliera'], index=0)
        if resample_mode == 'Mensile':
            ts = df_ly.resample('M')['value'].mean()
        else:
            ts = df_ly.resample('D')['value'].mean()

        fig2, ax2 = plt.subplots(figsize=(10,3))
        ax2.plot(ts.index, ts.values, marker='o', linestyle='-')
        ax2.set_title(f"Andamento {sel_pollutant} — Stazione {sel_station} ({ly})")
        ax2.set_ylabel("Valore")
        ax2.set_xlabel("Data")
        ax2.grid(True)
        st.pyplot(fig2)

        # evidenzia picchi: valori > mean + 2*std
        m = ts.mean()
        s = ts.std()
        peaks = ts[ts > (m + 2*s)]
        if not peaks.empty:
            st.markdown("**Picchi evidenziati (valori molto alti):**")
            st.write(peaks.reset_index().rename(columns={0:'valore','index':'data'}))

# ----------------------------
# Info e note finali
# ----------------------------
st.markdown("---")
st.markdown("**Note:** i dataset ufficiali possono avere formati diversi; il codice include heuristics per normalizzare le colonne più comuni. Se il portale cambia struttura, è sufficiente adattare le mappe di normalizzazione.")
st.markdown("Fonte dati: portale Open Data Comune di Milano. :contentReference[oaicite:4]{index=4}")
