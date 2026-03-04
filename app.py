# importiamo le librerie necessarie
import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt


# titolo della pagina
st.title("Analisi della qualità dell'aria a Milano")

st.write(
    "Questa applicazione utilizza i dataset pubblici del Comune di Milano "
    "per analizzare l'andamento degli inquinanti nell'aria."
)


# caricamento del dataset dal sito open data

st.subheader("Caricamento dei dati dal sito del Comune di Milano")

url = "https://dati.comune.milano.it/datastore_search?resource_id=4b6f1a8d-6e3d-4a62-bb0d-0a4d9e6e3e3c&limit=50000"

data = pd.read_json(url)

records = data["result"]["records"]

df = pd.DataFrame(records)


# mostriamo le prime righe del dataset

st.write("Anteprima dei dati")

st.write(df.head())


# controlliamo se esiste la colonna data

if "data" in df.columns:

    df["data"] = pd.to_datetime(df["data"])

    df["anno"] = df["data"].dt.year

    df["mese"] = df["data"].dt.month


# selezione inquinante

st.subheader("Selezione dell'inquinante")

if "inquinante" in df.columns:

    lista_inquinanti = df["inquinante"].unique()

    inquinante = st.selectbox(
        "Scegli un inquinante da analizzare",
        lista_inquinanti
    )

    df_inquinante = df[df["inquinante"] == inquinante]

else:

    st.write("La colonna inquinante non è presente nel dataset")

    df_inquinante = df


# analisi degli ultimi 10 anni

st.subheader("Andamento dell'inquinante negli ultimi 10 anni")

if "anno" in df_inquinante.columns and "valore" in df_inquinante.columns:

    media_annuale = df_inquinante.groupby("anno")["valore"].mean()

    fig, ax = plt.subplots()

    ax.plot(media_annuale.index, media_annuale.values, marker="o")

    ax.set_xlabel("Anno")

    ax.set_ylabel("Valore medio")

    ax.set_title("Media annuale dell'inquinante")

    st.pyplot(fig)

else:

    st.write("Le colonne necessarie per l'analisi non sono presenti")


# analisi delle stazioni

st.subheader("Classifica delle stazioni più inquinate")

if "stazione_id" in df_inquinante.columns:

    media_stazioni = (
        df_inquinante
        .groupby("stazione_id")["valore"]
        .mean()
        .sort_values(ascending=False)
    )

    st.write("Le 5 stazioni con il valore medio più alto")

    st.write(media_stazioni.head(5))


    fig2, ax2 = plt.subplots()

    media_stazioni.head(5).plot(kind="bar", ax=ax2)

    ax2.set_xlabel("Stazione")

    ax2.set_ylabel("Valore medio")

    ax2.set_title("Classifica stazioni")

    st.pyplot(fig2)

else:

    st.write("Colonna stazione_id non trovata nel dataset")


# selezione della stazione

st.subheader("Selezione della stazione")

if "stazione_id" in df_inquinante.columns:

    stazione = st.selectbox(
        "Scegli una stazione",
        df_inquinante["stazione_id"].unique()
    )

    df_stazione = df_inquinante[df_inquinante["stazione_id"] == stazione]

else:

    df_stazione = df_inquinante


# analisi ultimo anno

st.subheader("Andamento dell'inquinante nell'ultimo anno")

if "anno" in df_stazione.columns:

    ultimo_anno = df_stazione["anno"].max()

    df_ultimo_anno = df_stazione[df_stazione["anno"] == ultimo_anno]

    if "mese" in df_ultimo_anno.columns:

        media_mensile = df_ultimo_anno.groupby("mese")["valore"].mean()

        fig3, ax3 = plt.subplots()

        ax3.plot(media_mensile.index, media_mensile.values, marker="o")

        ax3.set_xlabel("Mese")

        ax3.set_ylabel("Valore medio")

        ax3.set_title("Andamento nell'ultimo anno")

        st.pyplot(fig3)

    else:

        st.write("Colonna mese non trovata")

else:

    st.write("Colonna anno non trovata")


# spiegazione degli inquinanti

st.subheader("Spiegazione degli inquinanti")

st.write(
"""
NO2 (biossido di azoto)

È un gas prodotto soprattutto dal traffico automobilistico e dalle combustioni.
Può causare irritazioni alle vie respiratorie.

PM10

Sono particelle di polvere molto piccole presenti nell'aria.
Possono entrare nei polmoni e causare problemi respiratori.

PM2.5

Sono particelle ancora più piccole del PM10.
Sono considerate più pericolose perché riescono a penetrare più facilmente nei polmoni.
"""
)
