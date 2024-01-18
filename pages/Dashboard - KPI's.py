# Librerias
import streamlit as st
import webbrowser


# URL de tu dashboard de Power BI
power_bi_dashboard_url = "https://app.powerbi.com/view?r=eyJrIjoiZGRjYTY3N2UtZDFlZS00YWVkLTg1Y2ItZTc3MmEyOGYzNWM1IiwidCI6ImRmODY3OWNkLWE4MGUtNDVkOC05OWFjLWM4M2VkN2ZmOTVhMCJ9"
canva_url = "https://www.canva.com/design/DAF48wQchTA/GOhZ0RJePBVnwqMsk7cLSw/edit?utm_content=DAF48wQchTA&utm_campaign=designshare&utm_medium=link2&utm_source=sharebutton"

# Configurar Streamlit
st.write("<h2>Dashboard - KPI's</h2>", unsafe_allow_html=True)

# Embeber el dashboard de Power BI en Streamlit
st.components.v1.iframe(power_bi_dashboard_url, width=800, height=600)








