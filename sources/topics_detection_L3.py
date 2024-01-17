# Importaciones
import streamlit as st
import pandas as pd

# Ruta 
ruta_csv = r'C:\Users\johan\Bootcamp_SoyHenry\PI_Final\sentiment_analysis.csv'

# Lee un número limitado de registros aleatorios desde el archivo CSV
num_registros_a_traer = 2000  
df_data = pd.read_csv(ruta_csv, encoding='latin1').sample(n=num_registros_a_traer, random_state=42)

# Declaración de variables
max_rows = 10

# Funciones
def apply_filters(data_frame, filters):
    try:
        filtered_df = data_frame.copy()

        for key, values in filters.items():
            if key == "stars" and values:
                # Asegurarse de que el valor de 'stars' se trate como una lista de enteros en la condición
                values = list(map(int, values))
                filtered_df = filtered_df[filtered_df[key].isin(values)]
                
        if filtered_df.empty:
            st.error("Debe seleccionarse al menos una opción de estrella.")
            return pd.DataFrame(columns=data_frame.columns)

        return filtered_df
    except Exception as e:
        st.error(f"Error al aplicar los filtros: {e}")
        return pd.DataFrame(columns=data_frame.columns)

# Interfaz de usuario con Streamlit
st.title("Consulta")

# Filtros Desplegable
with st.sidebar.expander("Detección de Tópicos"):
    # STARS
    op_stars = [1, 2, 3, 4, 5]

    # Verifica el checkbox "Seleccionar Todas"
    select_all_stars = st.checkbox("Seleccionar Todas")

    # Filtros de estrellas como checkboxes
    stars_filter = [st.checkbox(f"Star {star}", value=(select_all_stars or (star in op_stars))) for star in op_stars]

    # Actualiza op_stars basado en la selección manual
    op_stars = [star for star, selected in zip(op_stars, stars_filter) if selected]

    # Si no se selecciona al menos una estrella, muestra un mensaje de error
    if not op_stars and not select_all_stars:
        st.error("Debe seleccionarse al menos una opción de estrella.")
        # Establece automáticamente todas las estrellas como seleccionadas
        op_stars = [1, 2, 3, 4, 5]

    # Si todas las estrellas están seleccionadas, muestra un mensaje
    if select_all_stars and all(stars_filter):
        st.info("Todas las estrellas están seleccionadas.")

filters = {
    "stars": op_stars,
}

if st.sidebar.button("Realizar Consultas"):
    try:
        # Aplica los filtros llamando a la función
        df_filter = apply_filters(df_data, filters)
        # Verifica si df_filter es None antes de intentar utilizar head()
        if not df_filter.empty:
            st.table(df_filter.head())
            st.success("Consultas exitosas.")
        else:
            st.warning("No hay datos que cumplan con los filtros.")

        # Aplicar modelo LDA al conjunto de datos resultante
        # (agrega tu código de modelo LDA aquí)

    except ValueError as ve:
        st.error(f"Error: {ve}")
    except Exception as e:
        st.error(f"Error inesperado: {e}")
        print(e)
