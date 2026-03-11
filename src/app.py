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
Questo progetto consiste nello sviluppo di un'applicazione web per l'analisi della qualità dell’aria nella città di Milano.

L'applicazione è stata realizzata utilizzando il linguaggio di programmazione Python e la libreria Streamlit, che permette di creare interfacce web interattive in modo semplice.

I dati utilizzati provengono dal portale Open Data del Comune di Milano e sono forniti in formato JSON.
Questi dataset contengono migliaia di misurazioni giornaliere relative ai principali inquinanti atmosferici rilevati dalle stazioni di monitoraggio presenti sul territorio.

Il programma carica automaticamente tutti i file contenenti i dati, li unisce in un unico dataset utilizzando la libreria pandas e li organizza in un DataFrame per facilitarne l’analisi.

Successivamente l'applicazione mostra:

il numero totale di misurazioni analizzate

un'anteprima del dataset

un sistema di selezione degli inquinanti

L’utente può selezionare un inquinante specifico, come NO₂, PM10, PM2.5 o O₃, e visualizzare i valori registrati nel tempo attraverso grafici generati con la libreria Matplotlib.

In questo modo l'applicazione permette di osservare l’andamento dei livelli di inquinamento atmosferico e comprendere meglio l'evoluzione della qualità dell'aria nel corso degli anni.

Complessivamente il progetto analizza oltre 54.000 misurazioni ambientali, dimostrando come strumenti di data analysis possano essere utilizzati per studiare fenomeni ambientali e supportare la comprensione dei dati pubblici.
""")


# CARICAMENTO DATI


st.header("Caricamento dei dataset")

# cartella dove si trovano i file json
data_folder = os.path.join(os.path.dirname(__file__), "data")

# lista che conterrà tutti i dati
all_data = []

for file in os.listdir(data_folder):
    if file.endswith(".json"):
        file_path = os.path.join(data_folder, file)

        with open(file_path) as f:
            data = json.load(f)

            for item in data:
                all_data.append(item)

# CREAZIONE DATAFRAME

st.header("Creazione DataFrame")

# trasformiamo i dati in un dataframe pandas
df = pd.DataFrame(all_data)

# convertiamo alcune colonne
df["valore"] = pd.to_numeric(df["valore"], errors="coerce")

df["data"] = pd.to_datetime(df["data"], errors="coerce")

# mostriamo le prime righe
st.subheader("Anteprima dataset")

st.dataframe(df.head())


# ANALISI INQUINANTI


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


# ANALISI TEMPORALE


st.header("Andamento nel tempo")

# creiamo una colonna anno
df_filtrato["anno"] = df_filtrato["data"].dt.year

# calcoliamo media annuale
media_annuale = df_filtrato.groupby("anno")["valore"].mean()


# GRAFICO


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


# STATISTICHE


st.header("Statistiche")

media = df_filtrato["valore"].mean()
massimo = df_filtrato["valore"].max()
minimo = df_filtrato["valore"].min()

st.write("Valore medio:", round(media,2))
st.write("Valore massimo:", massimo)
st.write("Valore minimo:", minimo)


# CONCLUSIONE


st.header("Conclusione")

st.write("""
Questo progetto dimostra come i dati open data possano essere utilizzati
per analizzare l'inquinamento atmosferico.

Utilizzando Python, pandas e Streamlit è possibile creare applicazioni
interattive per esplorare i dati ambientali.
""")
