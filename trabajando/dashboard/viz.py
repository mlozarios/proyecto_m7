import os
import streamlit as st
import pandas as pd
import requests
import psycopg2
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

    #- Tipos de trabajo por fecha ---
    with st.container():
        #_, col, _ = st.columns([1, 4, 1], vertical_alignment='center', gap='small')
        col1, col2 = st.columns(2, vertical_alignment='top', gap='large')
     
        with col1:
            jan_start, april_end = pd.Timestamp('2025-01-01'), pd.Timestamp('2025-04-30')
            df_april = df[(df['date_published'] >= jan_start) & (df['date_published'] <= april_end)]

            top_jobs = (
                df_april.groupby('type_job')
                .size()
                .sort_values(ascending=False)
                .head(5)
                .index.tolist()
            )

            top_df = df_april[df_april['type_job'].isin(top_jobs)].copy()
            top_df['month'] = top_df['date_published'].dt.to_period('M')
            monthly_counts = top_df.groupby(['month', 'type_job']).size().unstack(fill_value=0)
            monthly_counts.index = monthly_counts.index.to_timestamp()

            st.subheader('Los 5 tipos de trabajo mas frecuentes por mes')
            st.line_chart(monthly_counts, height=500)
    
    st.markdown("###")

    # --- Left Column: Location Filter + Top Jobs ---
    col3, col4 = st.columns(2, vertical_alignment='top', gap='large')

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
        st.write(job_freq.head(10))

# --- Right Column: Location Frequency + Bernoulli Plot ---
    with col4:
        st.header("Histograma de trabajos por departamento")
        loc_freq = df['location'].value_counts().reset_index()
        loc_freq.columns = ['location', 'count']
        st.bar_chart(loc_freq, x='location', y='count')

        st.divider()
        st.header("Bernoulli Distribution Of Santa Cruz Over Time")
        df_bern_april = df[(df['date_published'] >= jan_start) & (df['date_published'] <= april_end)].copy()

        df_bern_april['is_santa_cruz'] = df_bern_april['location'].apply(lambda x: 1 if x == 'santa cruz' else 0)
        bernoulli = df_bern_april.groupby('date_published')['is_santa_cruz'].mean().reset_index(name='probability')
        bernoulli.set_index('date_published', inplace=True)

        st.subheader("Probability of Santa Cruz Location Over Time")
        st.line_chart(bernoulli)
    


st.markdown("###")
_, col5, _ = st.columns([2,0.7, 2], vertical_alignment='center', gap='small')

with col5:
    st.markdown(
            "<button style='padding: 10px; margin:8px; border-radius: 5px; background-color: #023047;'><a href='https://trabajito.com.bo' style='color: #fff; text-decoration: none'>Trabajito</a></button> "
            "<button style='padding: 10px; margin:8px; border-radius: 5px; background-color: #023047;'><a href='https://trabajando.com.bo/' style='color: #fff; text-decoration: none'>Trabajando</a></button>",
            unsafe_allow_html=True
        )

