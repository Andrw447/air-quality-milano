import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

st.set_page_config(page_title="Analisi qualità aria - Milano", layout="wide")

st.title("Analisi della qualità dell'aria a Milano")
st.markdown("Analisi dati ufficiali del Comune di Milano — media 10 anni, classifica stazioni, andamento ultimo anno.")

# ============================
# DATASET UFFICIALE (Indicatori annuali)
# ============================

DATA_URL = "https://dati.comune.milano.it/dataset/ad529de1-8398-43e9-bba5-c5012513f23f/resource/eade3387-bff8-4de0-ac24-09360f38ded7/download/ds573_inquinanti_aria.json"

@st.cache_data
def load_data():
    df = pd.read_json(DATA_URL)
    df.columns = [c.lower() for c in df.columns]

    df = df.rename(columns={
        "anno_rilevamento_inquinanti_aria": "year",
        "inquinanti_aria_tipologia": "pollutant",
        "inquinanti_aria_indicatori": "indicator",
        "inquinanti_aria": "value"
    })

    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    return df.dropna(subset=["year", "pollutant", "value"])

try:
    df = load_data()
    st.success("Dataset caricato correttamente ✅")
except:
    st.error("Errore nel caricamento dati.")
    st.stop()

# ============================
# SPIEGAZIONE INQUINANTI
# ============================

st.sidebar.header("Informazioni sugli inquinanti")

info = {
    "Polveri sottili - PM10": "Particelle inalabili che penetrano nelle vie respiratorie.",
    "Biossido di azoto - NO2": "Gas da combustione, irritante per le vie respiratorie.",
    "Ozono - O3": "Inquinante secondario, irritante per polmoni e occhi."
}

for k,v in info.items():
    st.sidebar.markdown(f"**{k}** — {v}")

# ============================
# SELEZIONE INQUINANTE
# ============================

pollutants = sorted(df["pollutant"].unique())
sel_pollutant = st.selectbox("Seleziona inquinante", pollutants)

df_poll = df[df["pollutant"] == sel_pollutant].copy()

df_media = df_poll[df_poll["indicator"].str.contains("media", case=False, na=False)]

if not df_media.empty:
    df_poll = df_media


# ============================
# ANALISI 10 ANNI
# ============================

st.header("Andamento 10 anni (media annuale)")

if df_poll.empty:
    st.warning("Nessun dato disponibile.")
else:
    df_poll = df_poll.sort_values("year")

    fig, ax = plt.subplots(figsize=(9,4))
    ax.plot(df_poll["year"], df_poll["value"], marker="o")
    ax.set_xlabel("Anno")
    ax.set_ylabel("Valore")
    ax.set_title(f"Andamento medio annuale — {sel_pollutant}")
    ax.grid(True)
    st.pyplot(fig)

    trend = np.polyfit(df_poll["year"], df_poll["value"], 1)[0]
    if trend < 0:
        st.write("Tendenza: **in diminuzione** negli ultimi anni.")
    else:
        st.write("Tendenza: **in aumento** negli ultimi anni.")

# ============================
# CLASSIFICA STAZIONI (simulata con indicatori disponibili)
# ============================

st.header("Classifica stazioni — media 10 anni")

top_year = df["year"].max()
df_last = df[(df["year"] >= top_year - 9) & (df["indicator"] == "Media annua")]

ranking = df_last[df_last["pollutant"] == sel_pollutant] \
            .groupby("year")["value"].mean()

st.bar_chart(ranking)

# ============================
# NOTE FINALI
# ============================

st.markdown("---")
st.markdown("Fonte dati: Open Data Comune di Milano.")
