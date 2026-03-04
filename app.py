
import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt

st.set_page_config(page_title="Qualita dell'Aria Milano", layout="wide")

st.title("Analisi della qualita dell'aria a Milano")
st.write("Applicazione semplice per analizzare gli inquinanti negli ultimi 10 anni.")

# Dataset URLs (open data Milano)
AIR_DATA_URL = "https://dati.comune.milano.it/dataset?res_format=JSON&tags=qualita+aria&_tags_limit=0"
STATIONS_URL = "https://dati.comune.milano.it/dataset/d6960c75-0a02-4fda-a85f-3b1c4aa725d6/resource/635c6508-b335-48b1-b3c8-0d6c3b2a7d0c/download/stazioni.json"

@st.cache_data
def load_data():
    try:
        air = pd.read_json(AIR_DATA_URL)
    except:
        air = pd.DataFrame()

    try:
        stations = pd.read_json(STATIONS_URL)
    except:
        stations = pd.DataFrame()

    return air, stations

air_df, stations_df = load_data()

if air_df.empty:
    st.warning("Dataset non caricato correttamente. Controlla i link dei dataset.")
else:
    # Example cleaning
    if "data" in air_df.columns:
        air_df["data"] = pd.to_datetime(air_df["data"], errors="coerce")
        air_df["anno"] = air_df["data"].dt.year

    st.subheader("Dataset caricato")
    st.write(air_df.head())

    # Select pollutant
    if "inquinante" in air_df.columns:
        pollutant = st.selectbox("Seleziona inquinante", air_df["inquinante"].unique())
        df_pollutant = air_df[air_df["inquinante"] == pollutant]
    else:
        pollutant = None
        df_pollutant = air_df

    # 10 year trend
    if "anno" in df_pollutant.columns and "valore" in df_pollutant.columns:
        st.subheader("Andamento negli anni")

        yearly = df_pollutant.groupby("anno")["valore"].mean().reset_index()

        fig, ax = plt.subplots()
        ax.plot(yearly["anno"], yearly["valore"])
        ax.set_xlabel("Anno")
        ax.set_ylabel("Media")
        ax.set_title("Media annuale inquinante")

        st.pyplot(fig)

    # Station ranking
    if "stazione_id" in df_pollutant.columns and "valore" in df_pollutant.columns:
        st.subheader("Classifica stazioni")

        ranking = (
            df_pollutant.groupby("stazione_id")["valore"]
            .mean()
            .sort_values(ascending=False)
            .head(5)
            .reset_index()
        )

        st.write(ranking)

    st.subheader("Spiegazione inquinanti")

    st.markdown("""
**NO2 (biossido di azoto)**  
Gas prodotto soprattutto dal traffico. Può irritare i polmoni.

**PM10**  
Particelle di polvere molto piccole presenti nell'aria.

**PM2.5**  
Particelle ancora più piccole del PM10 e piu pericolose per la salute.
""")
