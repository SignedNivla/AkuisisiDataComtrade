import streamlit as st
import requests
import json
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import os
from dotenv import load_dotenv
import shutil
from datetime import datetime, timedelta

from pathlib import Path
env_path = Path(__file__).parent / ".env"
load_dotenv(dotenv_path=env_path)  # Load environment variables from .env file

# ---------- Configuration ----------
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
    "database": os.getenv("DB_NAME", "comtrade_db"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "5M4T4RK1")
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
    headers = {
        "Ocp-Apim-Subscription-Key": API_KEY,
        "Cache-Control": "no-cache"
    }

    params = {
        "reporterCode": reporter,
        "period": ",".join(str(y) for y in years),
        "flowCode": flow,
        "cmdCode": hs,
        "fmt": "json",
        "includeDesc": "true"
    }

    if partner != "0":   # only add partnerCode if not "All Partners"
        params["partnerCode"] = partner

    url = f"{BASE_URL}/C/{freq}/HS"

    # st.write(f"**Request URL:** {url}")
    # st.write(f"**Params:** {params}")

    try:
        response = requests.get(url, headers=headers, params=params)
        # st.write(f"**Status Code:** {response.status_code}")

        if response.status_code != 200:
            st.error(f"HTTP Error: {response.text}")
            return []

        data = response.json()

        if data.get("error"):
            st.error(f"API error: {data['error']}")
            return []

        return data.get("data", [])

    except Exception as e:
        st.error(f"Request failed: {e}")
        return []

CACHE_DIR = "cache"
os.makedirs(CACHE_DIR, exist_ok=True)

def load_from_cache(filename, max_age_days=7):
    """Load data from a cache file if it exists and is not too old."""
    path = os.path.join(CACHE_DIR, filename)
    if os.path.exists(path):
        mod_time = datetime.fromtimestamp(os.path.getmtime(path))
        if datetime.now() - mod_time < timedelta(days=max_age_days):
            with open(path, 'r') as f:
                return json.load(f)
    return None

def save_to_cache(filename, data):
    """Save data to a cache file."""
    path = os.path.join(CACHE_DIR, filename)
    with open(path, 'w') as f:
        json.dump(data, f)

@st.cache_data(ttl=86400)
def fetch_countries():
    # Try cache first
    cached = load_from_cache("countries.json")
    if cached is not None:
        return cached

    url = "https://comtradeapi.un.org/files/v1/app/reference/partnerAreas.json"
    try:
        resp = requests.get(url)
        resp.raise_for_status()
        data = resp.json()
        countries = {str(item["id"]): item["text"] for item in data.get("results", [])}
        save_to_cache("countries.json", countries)
        return countries
    except Exception as e:
        st.warning(f"Could not fetch countries: {e}. Using fallback.")
        return {"360": "Indonesia", "392": "Japan", "0": "World"}

@st.cache_data(ttl=86400)
def fetch_hs_codes():
    cached = load_from_cache("hs_codes.json")
    if cached is not None:
        return cached

    url = "https://comtradeapi.un.org/files/v1/app/reference/HS.json"
    try:
        resp = requests.get(url)
        resp.raise_for_status()
        data = resp.json()
        hs_dict = {}
        for item in data.get("results", []):
            code = str(item["id"])
            if code.isdigit() and len(code) == 4:
                desc = item.get("text", "")
                # Clean description (remove leading code)
                if desc.startswith(code):
                    desc = desc[len(code):].strip().lstrip('-').strip()
                hs_dict[code] = desc
        save_to_cache("hs_codes.json", hs_dict)
        return hs_dict
    except Exception as e:
        st.warning(f"Could not fetch HS codes: {e}. Using fallback.")
        return {"1001": "Wheat", "1002": "Rye"}

def get_table_names():
    """Return a list of table names in the public schema."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT tablename FROM pg_tables
            WHERE schemaname = 'public'
            ORDER BY tablename;
        """)
        tables = [row[0] for row in cur.fetchall()]
        return tables
    except Exception as e:
        st.error(f"Could not fetch tables: {e}")
        return []
    finally:
        cur.close()
        conn.close()

def insert_records(records, hs_code, frequency, hs_dict, table_name):
    if not records:
        return 0

    FLOW_CODE_MAP = {
        "M": 1,    # Import
        "X": 2,    # Export
        "RM": 3,   # Re-import
        "RX": 4,   # Re-export
        "DX": 5,   # Domestic Export
        "FM": 6,   # Foreign Import
        "MIP": 7,  # Import of goods for inward processing
        "MOP": 8,  # Import of goods after outward processing
        "XIP": 9,  # Export of goods after inward processing
        "XOP": 10, # Export of goods for outward processing
    }

    conn = get_db_connection()
    cur = conn.cursor()
    values = []

    for rec in records:
        flow_raw = rec.get("flowCode")
        flow_code = FLOW_CODE_MAP.get(flow_raw)
        if flow_code is None:
            try:
                flow_code = int(flow_raw) if flow_raw else None
            except (ValueError, TypeError):
                flow_code = None

        trade_val = rec.get("primaryValue", 0)
        if trade_val is None:
            trade_val = 0
        else:
            try:
                trade_val = float(trade_val)
            except (ValueError, TypeError):
                trade_val = 0

        cmd_code = rec.get("cmdCode")
        cmd_name_en = hs_dict.get(cmd_code, "")

        values.append((
            rec.get("reporterCode"),
            rec.get("reporterDesc"),
            rec.get("partnerCode"),
            rec.get("partnerDesc"),
            rec.get("period"),
            flow_code,
            rec.get("flowDesc"),
            hs_code,
            cmd_code,
            cmd_name_en,
            frequency,
            trade_val,
        ))

    # Use the dynamic table name in the INSERT statement
    insert_sql = f"""
        INSERT INTO {table_name} (
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

st.markdown("""
<style>
    .stApp { background-color: #0C1222; }
    .stMultiSelect div[data-baseweb="select"] span[data-baseweb="tag"] {
        background-color: #003366 !important;
        color: white !important;
    }
    .stMultiSelect div[data-baseweb="select"] span[data-baseweb="tag"] svg {
        fill: white !important;
    }
    button[kind="primary"],
    button[data-testid*="extract_btn"] {
        background-color: #003366 !important;
        color: white !important;
        border: none;
    }
    button[kind="primary"]:hover,
    button[data-testid*="extract_btn"]:hover {
        background-color: #002244 !important;
    }
</style>
""", unsafe_allow_html=True)

st.title("UN Comtrade Data Extractor")
st.markdown("Select filters and click **Extract & Save** to fetch data and store in PostgreSQL.")

# Define options (you can expand these from API or local lists)

if "countries" not in st.session_state:
    with st.spinner("Loading country list..."):
        st.session_state.countries = fetch_countries()

trade_flows = {
    "M": "Import",
    "X": "Export",
    "RM": "Re-import",
    "RX": "Re-export",
    "DX": "Domestic Export",
    "FM": "Foreign Import",
    "MIP": "Import of goods for inward processing",
    "MOP": "Import of goods after outward processing",
    "XIP": "Export of goods after inward processing",
    "XOP": "Export of goods for outward processing",
}

if "hs_codes" not in st.session_state:
    with st.spinner("Loading HS codes..."):
        st.session_state.hs_codes = fetch_hs_codes()

if "0" not in st.session_state.countries:
    st.session_state.countries["0"] = "🌍 All Partners"

country_keys = list(st.session_state.countries.keys())

# reporter_default_idx = country_keys.index("360") if "360" in country_keys else 0
# partner_default_idx = country_keys.index("0") if "0" in country_keys else 0

reporter_options = list(st.session_state.countries.keys())
reporter_labels = [f"{code} - {st.session_state.countries[code]}" for code in reporter_options]

partner_options = list(st.session_state.countries.keys())

# Layout columns for better arrangement
col1, col2 = st.columns(2)
with col1:
    reporter_codes = st.multiselect(
    "Reporter Country",
    options=reporter_options,
    format_func=lambda x: st.session_state.countries[x],
    default=["360"]  # default Indonesia
)
with col2:
    partner_codes = st.multiselect(
    "Partner Country",
    options=partner_options,
    format_func=lambda x: st.session_state.countries[x],
    default=["0"]  # default World
)

col3, col4 = st.columns(2)
with col3:
    start_year = st.number_input("Start Year [Range is 2000-2025]", min_value=2000, max_value=2025, value=2019, step=1)
with col4:
    end_year = st.number_input("End Year [12 Year Limit]", min_value=2000, max_value=2025, value=2025, step=1)

col5, col6 = st.columns(2)
with col5:
    flow = st.selectbox("Trade Flow", options=list(trade_flows.keys()), format_func=lambda x: trade_flows[x])
with col6:
    hs_code = st.selectbox("HS Code", options=list(st.session_state.hs_codes.keys()), format_func=lambda x: f"{x} - {st.session_state.hs_codes[x]}")

# freq = st.selectbox("Frequency", options=["A", "M"], format_func=lambda x: "Annual" if x == "A" else "Monthly")

# ... after the HS Code row ...
col7, col8 = st.columns(2)
with col7:
    freq = st.selectbox(
        "Frequency",
        options=["A", "M"],
        format_func=lambda x: "Annual" if x == "A" else "Monthly",
        key="freq_select"   # add unique key
    )
with col8:
    if "table_names" not in st.session_state:
        st.session_state.table_names = get_table_names()
    selected_table = st.selectbox(
        "Target Table",
        options=st.session_state.table_names,
        index=0 if "trade_data" in st.session_state.table_names else 0,
        key="table_select"   # add unique key
    )

# Extract button
if st.button("📥 Extract & Save to Database", type="primary"):
    if start_year > end_year:
        st.error("Start year must be ≤ end year.")
        st.stop()
    if not reporter_codes:
        st.error("Please select at least one reporter.")
        st.stop()
    if not partner_codes:
        st.error("Please select at least one partner.")
        st.stop()

    years = list(range(int(start_year), int(end_year) + 1))
    all_records = []

    # Handle partner codes: if "0" is selected, ignore others and use ["0"]
    if "0" in partner_codes:
        partner_list = ["0"]
    else:
        partner_list = partner_codes

    # Loop over reporters
    for rep in reporter_codes:
        # For each reporter, we'll make one request with all partners combined
        partner_str = ",".join(partner_list)
        with st.spinner(f"Fetching data for reporter {st.session_state.countries[rep]}..."):
            records = fetch_comtrade_data(rep, partner_str, years, flow, hs_code, freq)
            if records:
                all_records.extend(records)

    if not all_records:
        st.warning("No data returned from API.")
    else:
        st.info(f"Received {len(all_records)} total records from API. Inserting into database...")
        inserted = insert_records(all_records, hs_code, freq, st.session_state.hs_codes, selected_table)
        if inserted > 0:
            st.success(f"✅ {inserted} rows successfully inserted into PostgreSQL.")
        else:
            st.warning("No new rows inserted (all records already exist or insertion failed).")

    # years = list(range(int(start_year), int(end_year) + 1))

    # with st.spinner("Fetching data from UN Comtrade..."):
    #     records = fetch_comtrade_data(reporter, partner, years, flow, hs_code, freq)
    # if records:
    #     st.write("First record keys:", records[0].keys())
    #     st.write("First record values:", records[0])
    # if not records:
    #     st.warning("No data returned from API.")
    # else:
        # st.info(f"Received {len(records)} records from API. Inserting into database...")

        # inserted = insert_records(records, hs_code, freq, st.session_state.hs_codes)
        # if inserted > 0:
        #     st.success(f"✅ {inserted} rows successfully inserted into PostgreSQL.")
        # else:
        #     st.warning("No new rows inserted (all records already exist or insertion failed).")

if st.button("🗑️ Clear Cache"):
    if os.path.exists(CACHE_DIR):
        shutil.rmtree(CACHE_DIR)
        st.success("Cache cleared! Run the code again to reload data [For testing].")
    else:
        st.info("No cache found.")

# Optional: Show a preview of recent data (if needed)
if st.checkbox("Show recent inserted data"):
    conn = get_db_connection()
    df = pd.read_sql("SELECT * FROM trade_data ORDER BY id DESC LIMIT 10", conn)
    conn.close()
    st.dataframe(df)