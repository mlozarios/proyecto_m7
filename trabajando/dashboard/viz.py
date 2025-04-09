import os
import streamlit as st
import pandas as pd
import requests
import psycopg2
import matplotlib.pyplot as plt
import numpy as np
from dotenv import main
main.load_dotenv()

# Set Streamlit layout
st.set_page_config(layout="wide")

# --- Database Connection ---
@st.cache_resource
def init_connection():
    conn_params = {
        'host': os.getenv('DB_HOST'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'dbname': os.getenv('DB_DATABASE')
    }
    return psycopg2.connect(**conn_params)

@st.cache_data
def get_data(query):
    with init_connection().cursor() as cur:
        cur.execute(query)
        columns = [desc[0] for desc in cur.description]
        data = cur.fetchall()
        return pd.DataFrame(data, columns=columns)

# --- Data Cleaning ---
def clean_data(df):
    df['date_published'] = df['date_published'].replace('not supported', '17-17-17')
    df['date_published'] = df['date_published'].apply(lambda x: str(x).split('T')[0] if 'T' in str(x) else x)
    df['date_published'] = pd.to_datetime(df['date_published'], errors='coerce')
    df['location'] = df['location'].str.strip()
    df['type_job'] = df['type_job'].str.strip()
    return df.dropna(subset=['date_published'])

# --- Currency API ---
def fetch_currency():
    response = requests.get('https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@latest/v1/currencies/bob.json')
    if response.status_code == 200:
        data = response.json()
        return data.get('bob', {}).get('1inch', '')
    return ''

# --- Load and Prepare Data ---
query = "SELECT * FROM job_data_refined"
df = clean_data(get_data(query))
currency_val = fetch_currency()

# Date range for analysis
jan_start, april_end = pd.Timestamp('2025-01-01'), pd.Timestamp('2025-04-30')
df_april = df[(df['date_published'] >= jan_start) & (df['date_published'] <= april_end)].copy()

# --- Header / Metrics ---
with st.container():
    _, main, _ = st.columns([1, 9, 1], vertical_alignment='center', gap='small')

with main:
    with st.container():
        col1, col2, _ = st.columns([1, 6, 0.8], vertical_alignment='center', gap='small')
        col1.metric(label='Currency Boliviano', value=currency_val, delta=0.05)

        col2.markdown(
            "<h1 style='text-align: center; font-size: 35px;'>Análisis de Empleos</h1>"
            f"<h3 style='text-align: center;'>Total registros extraidos: <span style='color: red'>{len(df)}</span></h3>",
            unsafe_allow_html=True
        )

    # --- Top Job Types by Month Visualization ---
    with st.container():
        col1, col2 = st.columns(2, vertical_alignment='top', gap='large')
     
        with col1:
            # Get top 5 job types
            top_jobs = (
                df_april.groupby('type_job')
                .size()
                .sort_values(ascending=False)
                .head(5)
                .index.tolist()
            )

            # Prepare data for visualization
            top_df = df_april[df_april['type_job'].isin(top_jobs)].copy()
            top_df['month_year'] = top_df['date_published'].dt.to_period('M')
            monthly_counts = (
                top_df.groupby(['month_year', 'type_job'])
                .size()
                .unstack(fill_value=0)
                .sort_index()  # Ensures chronological order
            )

            # Convert Period index to proper date format
            monthly_counts.index = monthly_counts.index.to_timestamp()
            monthly_counts.index = monthly_counts.index.strftime('%b %Y')

            # Create matplotlib figure
            fig, ax = plt.subplots(figsize=(12, 6))
            
            # Set up positions for grouped bars
            n_types = len(monthly_counts.columns)
            x = np.arange(len(monthly_counts.index))  # the label locations
            width = 0.15  # width of the bars

            for i, job_type in enumerate(monthly_counts.columns):
                offset = width * i - (width * (n_types - 1) / 2)
                ax.bar(x + offset, monthly_counts[job_type], width, label=job_type)

            # Add labels and title
            ax.set_xlabel('Month')
            ax.set_ylabel('Number of Job Postings')
            ax.set_title('Top 5 Job Types by Month (Side-by-Side Comparison)')
            ax.set_xticks(x)
            ax.set_xticklabels(monthly_counts.index, rotation=45)
            ax.legend(title='Job Type')

            # Display in Streamlit
            st.subheader('Los 5 tipos de trabajo más frecuentes por mes')
            st.pyplot(fig)


    with col2:
        #st.subheader("Distribución de Trabajos por Plataforma")
    
        if 'url' in df.columns:
            df['platform'] = df['url'].apply(lambda x: 'Trabajando' if 'trabajando' in x.lower() else 'Trabajito' if 'trabajito' in x.lower() else 'Otra')
            platform_counts = df[df['platform'].isin(['Trabajando', 'Trabajito'])]['platform'].value_counts()
        
            if len(platform_counts) > 0:
                fig, ax = plt.subplots(figsize=(4, 3))
                labels = platform_counts.index
                sizes = platform_counts.values
                colors = ['#FF6384', '#36A2EB']
            
                wedges, texts, autotexts = ax.pie(
                    sizes,
                    labels=labels,
                    colors=colors,
                    autopct=lambda p: f'{p:.1f}%\n({int(p*sum(sizes)/100)})',  # Corregido aquí
                    startangle=90,
                    wedgeprops=dict(width=0.4),
                    textprops={'fontsize': 5},
                    pctdistance=0.85
                )
            
                centre_circle = plt.Circle((0,0), 0.2, color='white')
                fig.gca().add_artist(centre_circle)
                ax.set_title('Proporción de Trabajos por Plataforma', pad=6)
                ax.axis('equal')
                st.pyplot(fig)
            
        #        st.markdown("**Resumen:**")
        #        total = platform_counts.sum()
        #        st.metric("Total de trabajos analizados", total)
            
        #        cols = st.columns(2)
        #        with cols[0]:
        #            st.metric("Trabajando", 
        #                 platform_counts.get('Trabajando', 0),
        #                 f"{platform_counts.get('Trabajando', 0)/total*100:.1f}%")
        #        with cols[1]:
        #            st.metric("Trabajito", 
        #                 platform_counts.get('Trabajito', 0),
        #                 f"{platform_counts.get('Trabajito', 0)/total*100:.1f}%")
            else:
                st.warning("No se encontraron URLs de las plataformas conocidas")
        else:
            st.warning("No se encontró la columna 'url' en los datos")# ... (el resto de tu código permanece igual)

            
          


    
    st.markdown("###")

    # --- Left Column: Location Filter + Top Jobs ---
    col3, col4 = st.columns(2, vertical_alignment='top', gap='large')
    selected_location = None
    with col3:
        locations = sorted(df['location'].unique().tolist())
        default_idx = locations.index('santa cruz') if 'santa cruz' in locations else 0
        selected_location = st.selectbox("Seleccione un departamento para analizar:", options=locations, index=default_idx)

        loc_data = df_april[df_april['location'] == selected_location]
        loc_data = loc_data[loc_data['type_job'].isin(top_jobs)].copy()

        if not loc_data.empty:
            rate = loc_data.groupby(loc_data['date_published'].dt.date).size().reset_index(name='count')
            rate['pct_change'] = rate['count'].pct_change()

            st.header(f"Publicaciones en: {selected_location}")
            st.line_chart(rate.dropna(), x='date_published', y='pct_change')
        else:
            st.warning(f"No data available for {selected_location}")

        st.divider()

        st.header("Los 10 trabajos más buscados")
        job_freq = df['type_job'].value_counts().reset_index()
        job_freq.columns = ['type_job', 'count']
        st.write(job_freq.head(6))

    # --- Right Column: Location Frequency + Bernoulli Plot ---
    with col4:
        st.header(f"Bernoulli Distribution Of {selected_location} Over Time")
        df_bern_april = df[(df['date_published'] >= jan_start) & (df['date_published'] <= april_end)].copy()
        df_bern_april['is_santa_cruz'] = df_bern_april['location'].apply(lambda x: 1 if x == selected_location else 0)
        bernoulli = df_bern_april.groupby('date_published')['is_santa_cruz'].mean().reset_index(name='probability')
        bernoulli.set_index('date_published', inplace=True)
        st.subheader(f"Probability of {selected_location} Location Over Time")
        st.line_chart(bernoulli)

        st.divider()

        st.header("Histograma de trabajos por departamento")
        loc_freq = df['location'].value_counts().reset_index()
        loc_freq.columns = ['location', 'count']
        st.bar_chart(loc_freq, x='location', y='count')
        
    
st.markdown("###")
_, col5, _ = st.columns([2,0.7, 2], vertical_alignment='center', gap='small')

with col5:
    st.markdown(
            "<button style='padding: 10px; margin:8px; border-radius: 5px; background-color: #023047;'><a href='https://trabajito.com.bo' style='color: #fff; text-decoration: none'>Trabajito</a></button> "
            "<button style='padding: 10px; margin:8px; border-radius: 5px; background-color: #023047;'><a href='https://trabajando.com.bo/' style='color: #fff; text-decoration: none'>Trabajando</a></button>",
            unsafe_allow_html=True
        )