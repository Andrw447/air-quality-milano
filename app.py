
import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

st.title("Analisi della qualità dell'aria a Milano")

st.write("Applicazione per analizzare gli inquinanti negli ultimi 10 anni.")

# dati di esempio
anni = list(range(2014, 2024))

data = {
    "anno": anni,
    "NO2": np.random.randint(30, 70, 10),
    "PM10": np.random.randint(20, 60, 10),
    "PM2.5": np.random.randint(10, 40, 10),
}

df = pd.DataFrame(data)

st.subheader("Dati utilizzati")
st.write(df)

# selezione inquinante
inquinante = st.selectbox(
    "Seleziona inquinante",
    ["NO2", "PM10", "PM2.5"]
)

st.subheader("Andamento negli ultimi 10 anni")

fig, ax = plt.subplots()
ax.plot(df["anno"], df[inquinante], marker="o")

ax.set_xlabel("Anno")
ax.set_ylabel("Valore medio")
ax.set_title(f"Andamento {inquinante}")

st.pyplot(fig)

st.subheader("Spiegazione inquinanti")

st.write("""
NO2 (biossido di azoto)
Gas prodotto soprattutto dal traffico e può causare problemi respiratori.

PM10
Particelle molto piccole presenti nell'aria che possono entrare nei polmoni.

PM2.5
Particelle ancora più piccole e più pericolose per la salute.
""")
