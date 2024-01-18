import streamlit as st
from PIL import Image

#st.write('****')
st.write("<h3>Bienvenidos</h3>", unsafe_allow_html=True)

img = Image.open('parnertship.png')
st.image(img)

# Botón de Presentación
#if st.sidebar.button("Presentación"):
#    webbrowser.open(canva_url)

