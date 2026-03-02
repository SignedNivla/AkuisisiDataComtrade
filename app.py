import streamlit as st
import pandas as pd
import requests
import psycopg2
from psycopg2.extras import execute_values
import os
from dotenv import load_dotenv

load_dotenv()  # Load environment variables from .env file

# ---------- Configuration ----------
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
    "database": os.getenv("DB_NAME", "comtrade_db"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "")
}

API_KEY = os.getenv("UN_COMTRADE_API_KEY")
if not API_KEY:
    st.error("Please set UN_COMTRADE_API_KEY in .env file")
    st.stop()

# UN Comtrade API base URL (v1)
BASE_URL = "https://comtradeapi.un.org/data/v1/get"

# ---------- Helper Functions ----------
def get_db_connection():
    """Return a PostgreSQL connection."""
    return psycopg2.connect(**DB_CONFIG)

def fetch_comtrade_data(reporter, partner, years, flow, hs, freq):
    """
    Call UN Comtrade API and return list of records.
    """
    headers = {
        "Ocp-Apim-Subscription-Key": API_KEY
    }
    # Note: API may have limits on number of years per request. If many years, loop.
    # For simplicity, we pass years as comma-separated.
    params = {
        "reporterCode": reporter,
        "partnerCode": partner,
        "period": ",".join(str(y) for y in years),
        "flowCode": flow,
        "cmdCode": hs,
        "fmt": "json"
    }
    # URL format: /data/v1/get/{type}/{freq}/{classification}
    # type = C (commodities), freq = A or M, classification = HS
    url = f"{BASE_URL}/C/{freq}/HS"

    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        if data.get("error"):
            st.error(f"API error: {data['error']}")
            return []
        return data.get("data", [])
    except Exception as e:
        st.error(f"Request failed: {e}")
        return []

def insert_records(records, hs_code, frequency):
    """
    Insert records into PostgreSQL using batch insert.
    Returns number of inserted rows.
    """
    if not records:
        return 0

    conn = get_db_connection()
    cur = conn.cursor()

    # Prepare data for insertion
    values = []
    for rec in records:
        values.append((
            rec.get("reporterCode"),
            rec.get("reporterDesc"),
            rec.get("partnerCode"),
            rec.get("partnerDesc"),
            rec.get("period"),          # year
            rec.get("flowCode"),
            rec.get("flowDesc"),
            hs_code,                    # from input
            rec.get("cmdCode"),
            rec.get("cmdDescEN"),
            frequency,                   # from input
            rec.get("tradeValue", 0)
        ))

    insert_sql = """
        INSERT INTO trade_data (
            reporter_code, reporter_name,
            partner_code, partner_name,
            year, trade_flow_code, trade_flow_name,
            hs_code, cmd_code, cmd_name_en,
            frequency, trade_value
        ) VALUES %s
        ON CONFLICT (reporter_code, partner_code, year, trade_flow_code, hs_code, frequency)
        DO NOTHING
        RETURNING id;
    """

    try:
        # Use execute_values for efficient batch insert
        inserted_ids = execute_values(cur, insert_sql, values, fetch=True)
        conn.commit()
        return len(inserted_ids)
    except Exception as e:
        conn.rollback()
        st.error(f"Database error: {e}")
        return 0
    finally:
        cur.close()
        conn.close()

# ---------- Streamlit UI ----------
st.set_page_config(page_title="UN Comtrade Extractor", layout="centered")
st.title("🌐 UN Comtrade Data Extractor")
st.markdown("Select filters and click **Extract & Save** to fetch data and store in PostgreSQL.")

# Define options (you can expand these from API or local lists)
countries = {
    "360": "Indonesia",
    "392": "Japan",
    "124": "Canada",
    "842": "United States",
    "156": "China",
    "276": "Germany",
}

trade_flows = {
    "1": "Import",
    "2": "Export",
    "3": "Re-import",
    "4": "Re-export",
}

hs_codes = {
    "1001": "Wheat",
    "1002": "Rye",
    "1003": "Barley",
    "1004": "Oats",
    "1005": "Maize (corn)",
    "1006": "Rice",
}

# Layout columns for better arrangement
col1, col2 = st.columns(2)
with col1:
    reporter = st.selectbox("Reporter Country", options=list(countries.keys()), format_func=lambda x: countries[x])
with col2:
    partner = st.selectbox("Partner Country", options=list(countries.keys()), format_func=lambda x: countries[x])

col3, col4 = st.columns(2)
with col3:
    start_year = st.number_input("Start Year", min_value=2000, max_value=2025, value=2010, step=1)
with col4:
    end_year = st.number_input("End Year", min_value=2000, max_value=2025, value=2022, step=1)

col5, col6 = st.columns(2)
with col5:
    flow = st.selectbox("Trade Flow", options=list(trade_flows.keys()), format_func=lambda x: trade_flows[x])
with col6:
    hs_code = st.selectbox("HS Code", options=list(hs_codes.keys()), format_func=lambda x: f"{x} - {hs_codes[x]}")

freq = st.selectbox("Frequency", options=["A", "M"], format_func=lambda x: "Annual" if x == "A" else "Monthly")

# Extract button
if st.button("📥 Extract & Save to Database", type="primary"):
    if start_year > end_year:
        st.error("Start year must be ≤ end year.")
        st.stop()

    years = list(range(int(start_year), int(end_year) + 1))

    with st.spinner("Fetching data from UN Comtrade..."):
        records = fetch_comtrade_data(reporter, partner, years, flow, hs_code, freq)

    if not records:
        st.warning("No data returned from API.")
    else:
        st.info(f"Received {len(records)} records from API. Inserting into database...")

        inserted = insert_records(records, hs_code, freq)
        if inserted > 0:
            st.success(f"✅ {inserted} rows successfully inserted into PostgreSQL.")
        else:
            st.warning("No new rows inserted (all records already exist or insertion failed).")

# Optional: Show a preview of recent data (if needed)
if st.checkbox("Show recent inserted data"):
    conn = get_db_connection()
    df = pd.read_sql("SELECT * FROM trade_data ORDER BY id DESC LIMIT 10", conn)
    conn.close()
    st.dataframe(df)