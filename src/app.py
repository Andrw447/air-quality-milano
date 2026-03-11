# importiamo le librerie che servono
import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np


# titolo della pagina

st.title("Analisi della qualità dell'aria a Milano")

st.write(
    "Questa applicazione permette di analizzare l'andamento degli inquinanti "
    "nell'aria negli ultimi 10 anni e confrontare alcune stazioni di monitoraggio."
)


# creazione dati di esempio

anni = list(range(2014, 2024))

NO2 = np.random.randint(30, 70, 10)
PM10 = np.random.randint(20, 60, 10)
PM25 = np.random.randint(10, 40, 10)

df = pd.DataFrame({
    "Anno": anni,
    "NO2": NO2,
    "PM10": PM10,
    "PM2.5": PM25
})


# mostrare la tabella dei dati

st.subheader("Tabella dei dati")

st.write(
    "Qui possiamo vedere i valori medi degli inquinanti negli ultimi 10 anni."
)

st.write(df)


# selezione dell'inquinante

st.subheader("Selezione inquinante")

inquinante = st.selectbox(
    "Scegli quale inquinante vuoi analizzare",
    ["NO2", "PM10", "PM2.5"]
)

st.write("Hai selezionato:", inquinante)


# grafico andamento 10 anni

st.subheader("Andamento dell'inquinante negli ultimi 10 anni")

fig, ax = plt.subplots()

ax.plot(df["Anno"], df[inquinante], marker="o")

ax.set_xlabel("Anno")
ax.set_ylabel("Valore medio")
ax.set_title("Andamento nel tempo")

st.pyplot(fig)


# confronto tra stazioni

st.subheader("Confronto tra stazioni di monitoraggio")

stazioni = [
    "Centro",
    "Città Studi",
    "Bicocca",
    "Lambrate",
    "Navigli"
]

valori_stazioni = np.random.randint(20, 80, 5)

df_stazioni = pd.DataFrame({
    "Stazione": stazioni,
    "Valore medio": valori_stazioni
})

st.write(
    "Questa tabella mostra il valore medio dell'inquinante nelle diverse stazioni."
)

st.write(df_stazioni)


fig2, ax2 = plt.subplots()

ax2.bar(df_stazioni["Stazione"], df_stazioni["Valore medio"])

ax2.set_xlabel("Stazione")
ax2.set_ylabel("Valore medio")
ax2.set_title("Confronto tra stazioni")

st.pyplot(fig2)


# andamento ultimo anno

st.subheader("Andamento dell'inquinante nell'ultimo anno")

mesi = [
    "Gen", "Feb", "Mar", "Apr",
    "Mag", "Giu", "Lug", "Ago",
    "Set", "Ott", "Nov", "Dic"
]

valori_mensili = np.random.randint(20, 80, 12)

df_mesi = pd.DataFrame({
    "Mese": mesi,
    "Valore": valori_mensili
})

fig3, ax3 = plt.subplots()

ax3.plot(df_mesi["Mese"], df_mesi["Valore"], marker="o")

ax3.set_xlabel("Mese")
ax3.set_ylabel("Valore")
ax3.set_title("Andamento durante l'anno")

st.pyplot(fig3)


# spiegazione degli inquinanti

st.subheader("Spiegazione degli inquinanti")

st.write("""
NO2 (biossido di azoto)

È un gas prodotto principalmente dalle automobili e dal traffico.
Può causare problemi respiratori e irritazione ai polmoni.

PM10

Sono piccole particelle di polvere presenti nell'aria.
Possono entrare nei polmoni e causare problemi alla salute.

PM2.5

Sono particelle ancora più piccole del PM10.
Sono più pericolose perché riescono a entrare più facilmente nel corpo umano.
""")
