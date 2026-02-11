import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt

st.set_page_config(page_title="Analisi qualità aria - Milano", layout="wide")

st.title("Analisi della qualità dell'aria a Milano")

DATA_URL = "https://dati.comune.milano.it/dataset/ad529de1-8398-43e9-bba5-c5012513f23f/resource/eade3387-bff8-4de0-ac24-09360f38ded7/download/ds573_inquinanti_aria.csv"

@st.cache_data
def load_data():
    df = pd.read_csv(DATA_URL)
    return df

try:
    df = load_data()
except:
    st.error("Errore nel caricamento dei dati dal sito del Comune.")
    st.stop()

st.write("Dataset caricato correttamente ✅")
st.write(df.head())

# Selezione inquinante
if "Inquinante" in df.columns:
    pollutant = st.selectbox("Seleziona inquinante", df["Inquinante"].unique())
    df_filtered = df[df["Inquinante"] == pollutant]

    if "Valore" in df.columns:
        st.line_chart(df_filtered["Valore"])
else:
    st.warning("Colonne attese non trovate nel dataset.")
