import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt

st.set_page_config(page_title="Analisi qualità aria - Milano", layout="wide")

st.title("Analisi della qualità dell'aria a Milano")

DATA_URL = "https://dati.comune.milano.it/dataset/ad529de1-8398-43e9-bba5-c5012513f23f/resource/eade3387-bff8-4de0-ac24-09360f38ded7/download/ds573_inquinanti_aria.csv"

@st.cache_data
def load_data():
    df = pd.read_json(DATA_URL)
    return df

try:
    df = load_data()
except:
    st.error("Errore nel caricamento dei dati dal sito del Comune.")
    st.stop()

st.write("Dataset caricato correttamente ✅")
st.write(df.head())

# Selezione inquinante
# Mostra colonne disponibili
st.write("Colonne trovate nel dataset:")
st.write(df.columns)

# Selezione inquinante
# Selezione inquinante
pollutant = st.selectbox(
    "Seleziona inquinante",
    df["inquinanti_aria_tipologia"].unique()
)

df_filtered = df[df["inquinanti_aria_tipologia"] == pollutant]

# Filtra solo media annua (evita giorni di superamento)
df_filtered = df_filtered[df_filtered["inquinanti_aria_indicatori"] == "Media annua"]

# Ordina per anno
df_filtered["anno_rilevamento_inquinanti_aria"] = pd.to_numeric(
    df_filtered["anno_rilevamento_inquinanti_aria"], errors="coerce"
)

df_filtered["inquinanti_aria"] = pd.to_numeric(
    df_filtered["inquinanti_aria"], errors="coerce"
)

df_filtered = df_filtered.sort_values("anno_rilevamento_inquinanti_aria")


df_filtered["inquinanti_aria"] = pd.to_numeric(df_filtered["inquinanti_aria"], errors="coerce")

import matplotlib.pyplot as plt

st.subheader("Andamento nel tempo")

fig, ax = plt.subplots()

ax.plot(
    df_filtered["anno_rilevamento_inquinanti_aria"],
    df_filtered["inquinanti_aria"],
    marker="o"
)

ax.set_xlabel("Anno")
ax.set_ylabel("Valore")
ax.set_title(f"Andamento media annua - {pollutant}")

st.pyplot(fig)

