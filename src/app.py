# importiamo le librerie necessarie
import streamlit as st
import pandas as pd
import json
import os
import matplotlib.pyplot as plt

# titolo della pagina
st.title("Analisi della qualità dell'aria a Milano")

# descrizione del progetto
st.write("""
Questa applicazione analizza i dati sull'inquinamento dell'aria a Milano.
I dati provengono dal portale Open Data del Comune di Milano.

L'obiettivo del progetto è:
- analizzare l'andamento degli inquinanti negli anni
- confrontare diversi inquinanti
- visualizzare i dati con grafici
""")

# ----------------------------
# CARICAMENTO DATI
# ----------------------------

st.header("Caricamento dei dataset")

# cartella dove si trovano i file json
data_folder = os.path.join(os.path.dirname(__file__), "data")

# lista che conterrà tutti i dati
all_data = []

# contatore file
numero_file = 0

# leggiamo tutti i file presenti nella cartella data
for file in os.listdir(data_folder):

    # controlliamo che sia un file json
    if file.endswith(".json"):

        numero_file += 1

        percorso = os.path.join(data_folder, file)

        with open(percorso, encoding="utf-8") as f:

            dati = json.load(f)

            # aggiungiamo i dati alla lista principale
            all_data.extend(dati)

# mostriamo quanti file sono stati caricati
st.write("Numero file caricati:", numero_file)

# ----------------------------
# CREAZIONE DATAFRAME
# ----------------------------

st.header("Creazione DataFrame")

# trasformiamo i dati in un dataframe pandas
df = pd.DataFrame(all_data)

# convertiamo alcune colonne
df["valore"] = pd.to_numeric(df["valore"], errors="coerce")

df["data"] = pd.to_datetime(df["data"], errors="coerce")

# mostriamo numero totale di righe
st.write("Numero totale di misurazioni:", len(df))

# mostriamo le prime righe
st.subheader("Anteprima dataset")

st.dataframe(df.head())

# ----------------------------
# ANALISI INQUINANTI
# ----------------------------

st.header("Analisi degli inquinanti")

# lista degli inquinanti presenti
lista_inquinanti = df["inquinante"].dropna().unique()

# selezione inquinante
inquinante_scelto = st.selectbox(
    "Seleziona un inquinante da analizzare",
    lista_inquinanti
)

# filtro dataframe
df_filtrato = df[df["inquinante"] == inquinante_scelto]

# ----------------------------
# ANALISI TEMPORALE
# ----------------------------

st.header("Andamento nel tempo")

# creiamo una colonna anno
df_filtrato["anno"] = df_filtrato["data"].dt.year

# calcoliamo media annuale
media_annuale = df_filtrato.groupby("anno")["valore"].mean()

# ----------------------------
# GRAFICO
# ----------------------------

st.subheader("Grafico andamento medio annuale")

fig, ax = plt.subplots()

ax.plot(
    media_annuale.index,
    media_annuale.values,
    marker="o"
)

ax.set_xlabel("Anno")
ax.set_ylabel("Valore medio")
ax.set_title(f"Andamento {inquinante_scelto}")

ax.grid(True)

st.pyplot(fig)

# ----------------------------
# STATISTICHE
# ----------------------------

st.header("Statistiche")

media = df_filtrato["valore"].mean()
massimo = df_filtrato["valore"].max()
minimo = df_filtrato["valore"].min()

st.write("Valore medio:", round(media,2))
st.write("Valore massimo:", massimo)
st.write("Valore minimo:", minimo)

# ----------------------------
# CONCLUSIONE
# ----------------------------

st.header("Conclusione")

st.write("""
Questo progetto dimostra come i dati open data possano essere utilizzati
per analizzare l'inquinamento atmosferico.

Utilizzando Python, pandas e Streamlit è possibile creare applicazioni
interattive per esplorare i dati ambientali.
""")
