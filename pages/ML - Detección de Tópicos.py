import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.decomposition import LatentDirichletAllocation
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import nltk

nltk.download('stopwords')
nltk.download('punkt')

# Cargar tu DataFrame inicial (reemplaza esto con tu carga de datos real)
df = pd.read_csv("C:/Users/johan/Bootcamp_SoyHenry/PI_Final/topics.csv")

# Definir las opciones por defecto (vacías)
default_options = {
    "business_id": [],
    "source": [],
    "stars": [],
    "city": [],
    "state": [],
    "year": [],
    "month": [],
    "sentiment_label": [],
}


filters = {}


#filters = {}
for column in default_options:
    filters[column] = st.sidebar.multiselect(column.capitalize(), default_options[column] + list(df[column].unique()))

# Botón para realizar la consulta
if st.sidebar.button("Realizar Consultas"):
    # Aplicar los filtros al DataFrame solo cuando el botón es presionado
    filter_conditions = pd.Series(True, index=df.index)
    for column in filters:
        if filters[column]:  # Solo aplicar el filtro si la lista no está vacía
            filter_conditions = filter_conditions & df[column].isin(filters[column])

    # Filtrar el DataFrame
    filtered_df = df[filter_conditions]

    # Obtener las stop words en inglés
    stop_words = set(stopwords.words('english'))

    # Aplicar modelo de tópicos
    def preprocess_text(text, stop_words):
        try:
            # Check if the text is a string
            if isinstance(text, str):
                # Tokenize and remove stop words
                tokens = word_tokenize(text.lower())
                tokens = [token for token in tokens if token.isalpha() and token not in stop_words]
                return ' '.join(tokens)
            else:
                # If not a string, return an empty string or handle it according to your needs
                return ''
        except Exception as e:
            # Handle any exceptions that might occur during text preprocessing
            st.error(f"An error occurred during text preprocessing: {e}")
            return ''

    filtered_df['text_processed'] = filtered_df['text'].apply(preprocess_text, stop_words=stop_words)

    vectorizer = CountVectorizer(max_features=1000, stop_words='english')
    X = vectorizer.fit_transform(filtered_df['text_processed'])

    lda = LatentDirichletAllocation(n_components=5, random_state=42)
    topics = lda.fit_transform(X)

    # Crear un gráfico por tópico
    st.write("<h2>Top 10 Palabras por Tópico</h2>", unsafe_allow_html=True)
    for topic_idx, topic in enumerate(lda.components_):
        top_keywords_idx = topic.argsort()[:-11:-1]
        top_keywords = [vectorizer.get_feature_names_out()[i] for i in top_keywords_idx]

        # Crear un DataFrame para la tabla
        topic_df = pd.DataFrame({'Palabras': top_keywords, 'Peso': topic[top_keywords_idx]})
        topic_df['Peso'] = topic_df['Peso'].round(2)  # Redondear a 2 decimales
        topic_df = topic_df.sort_values(by='Peso', ascending=False)

        # Crear un subplot para el gráfico
        fig, (ax_table, ax_plot) = plt.subplots(1, 2, gridspec_kw={'width_ratios': [1, 3]}, figsize=(7, 4))

        # Mostrar la tabla
        #table = ax_table.table(cellText=topic_df.values, colLabels=topic_df.columns, cellLoc='center', loc='center')
        #table.auto_set_font_size(False)
        #table.set_fontsize(11)  # Aumentar el tamaño de la fuente
        #ax_table.axis('off')
        
        # Mostrar la tabla
        table = ax_table.table(cellText=topic_df.values, colLabels=topic_df.columns, cellLoc='center', loc='center')
        table.auto_set_font_size(False)
        table.set_fontsize(11)  # Aumentar el tamaño de la fuente

        # Ajustes adicionales para la tabla
        table.auto_set_column_width([0, 1, 2, 3, 4, 5, 6])  # Ajustar automáticamente el ancho de las columnas
        table.scale(1, 1.5)  # Escalar la tabla para hacerla un poco más ancha

        # Establecer el fondo gris para el encabezado
        header_cells = table.get_celld()
        for (i, j), cell in header_cells.items():
            if i == 0:
                cell.set_text_props(weight='bold', color='w')  # Establecer el texto en negrita y color blanco
                cell.set_facecolor('#D3D3D3')  # Código de color gris


        ax_table.axis('off')

        
        # Mostrar el gráfico
        sns.barplot(x='Peso', y='Palabras', data=topic_df, ax=ax_plot, palette='viridis')

        # Ajustes para el gráfico
        ax_plot.set_title(f"Tópico {topic_idx + 1}", fontsize=12)  # Ajustar el tamaño de la fuente del título
        ax_plot.set_xlabel("Peso de la Palabra", fontsize=7)  # Ajustar el tamaño de la fuente del eje x
        ax_plot.set_ylabel("Palabras", fontsize=7)  # Ajustar el tamaño de la fuente del eje y

        # Ajustes de fondo del gráfico
        ax_plot.set_facecolor('#ededed')  # Código de color gris para el fondo del gráfico

        # Ocultar los nombres de los ejes
        ax_plot.set_xlabel('')
        ax_plot.set_ylabel('')

        # Ajustes de diseño
        plt.tight_layout()
        st.pyplot(fig)
