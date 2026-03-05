import streamlit as st
import requests
import json
import pandas as pd
import psycopg2
import pymysql
from psycopg2.extras import execute_values
import os
import shutil
from datetime import datetime, timedelta
from pathlib import Path

# ---------- Page config ----------
st.set_page_config(page_title="UN Comtrade Extractor", layout="centered")

# ---------- Constants ----------
BASE_URL = "https://comtradeapi.un.org/data/v1/get"
CACHE_DIR = "cache"
os.makedirs(CACHE_DIR, exist_ok=True)

# ---------- Helper functions (caching, fetching, DB) ----------
def load_from_cache(filename, max_age_days=7):
    path = os.path.join(CACHE_DIR, filename)
    if os.path.exists(path):
        mod_time = datetime.fromtimestamp(os.path.getmtime(path))
        if datetime.now() - mod_time < timedelta(days=max_age_days):
            with open(path, 'r') as f:
                return json.load(f)
    return None

def save_to_cache(filename, data):
    path = os.path.join(CACHE_DIR, filename)
    with open(path, 'w') as f:
        json.dump(data, f)

# @st.cache_data(ttl=86400)
def fetch_countries():
    cached = load_from_cache("countries.json")
    if cached:
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

# @st.cache_data(ttl=86400)
def fetch_hs_codes():
    cached = load_from_cache("hs_codes.json")
    if cached:
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
                if desc.startswith(code):
                    desc = desc[len(code):].strip().lstrip('-').strip()
                hs_dict[code] = desc
        save_to_cache("hs_codes.json", hs_dict)
        return hs_dict
    except Exception as e:
        st.warning(f"Could not fetch HS codes: {e}. Using fallback.")
        return {"1001": "Wheat", "1002": "Rye"}

def fetch_comtrade_data(reporter, partner, years, flow, hs, freq):
    if "api_key" not in st.session_state or not st.session_state.api_key:
        st.error("API key missing.")
        return []
    headers = {"Ocp-Apim-Subscription-Key": st.session_state.api_key}
    params = {
        "reporterCode": reporter,
        "period": ",".join(str(y) for y in years),
        "flowCode": flow,
        "cmdCode": hs,
        "fmt": "json",
        "includeDesc": "true"
    }
    if partner:   # if partner is not None or empty string
        params["partnerCode"] = partner
    # If partner is None, omit parameter – API returns all partners individually
    url = f"{BASE_URL}/C/{freq}/HS"
    try:
        response = requests.get(url, headers=headers, params=params)
        if response.status_code != 200:
            st.warning(f"API returned status {response.status_code}")
            return []
        data = response.json()
        if data.get("error"):
            st.warning(f"API error: {data['error']}")
            return []
        return data.get("data", [])
    except Exception as e:
        st.warning(f"Request failed: {e}")
        return []

def get_db_connection():
    if "db_credentials" not in st.session_state:
        st.error("Database not connected.")
        st.stop()
    creds = st.session_state.db_credentials
    db_type = st.session_state.get("db_type", "PostgreSQL")
    try:
        if db_type == "PostgreSQL":
            conn = psycopg2.connect(
                host=creds["host"],
                port=creds["port"],
                database=creds["database"],
                user=creds["user"],
                password=creds["password"]
            )
        else:
            conn = pymysql.connect(
                host=creds["host"],
                port=int(creds["port"]),
                user=creds["user"],
                password=creds["password"],
                database=creds["database"],
                charset='utf8mb4'
            )
        return conn
    except Exception as e:
        st.error(f"Connection failed: {e}")
        st.stop()

def get_table_names():
    if "db_credentials" not in st.session_state:
        return []
    db_type = st.session_state.get("db_type", "PostgreSQL")
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        if db_type == "PostgreSQL":
            cur.execute("SELECT tablename FROM pg_tables WHERE schemaname='public' ORDER BY tablename;")
        else:
            cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema=%s ORDER BY table_name;",
                        (st.session_state.db_credentials['database'],))
        return [row[0] for row in cur.fetchall()]
    except Exception as e:
        st.error(f"Could not fetch tables: {e}")
        return []
    finally:
        cur.close()
        conn.close()

FLOW_CODE_MAP = {
    "M": 1, "X": 2, "RM": 3, "RX": 4,
    "DX": 5, "FM": 6, "MIP": 7, "MOP": 8,
    "XIP": 9, "XOP": 10,
}

def transform_record(rec, frequency, hs_dict):
    """Convert a raw API record into a dictionary matching the DB schema."""
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

    cmd_code = rec.get("cmdCode")                # e.g., "0101"
    cmd_name_en = hs_dict.get(cmd_code, "")      # look up description

    return {
        "reporter_code": rec.get("reporterCode"),
        "reporter_name": rec.get("reporterDesc"),
        "partner_code": rec.get("partnerCode"),
        "partner_name": rec.get("partnerDesc"),
        "year": rec.get("period"),
        "trade_flow_code": flow_code,
        "trade_flow_name": rec.get("flowDesc"),
        "hs_code": cmd_code,                      # now uses the record's own code
        "cmd_code": cmd_code,
        "cmd_name_en": cmd_name_en,
        "frequency": frequency,
        "trade_value": trade_val,
    }

def insert_records(records, frequency, hs_dict, table_name):
    if not records:
        return 0
    db_type = st.session_state.get("db_type", "PostgreSQL")

    values = []
    for rec in records:
        transformed = transform_record(rec, frequency, hs_dict)
        values.append((
            transformed["reporter_code"],
            transformed["reporter_name"],
            transformed["partner_code"],
            transformed["partner_name"],
            transformed["year"],
            transformed["trade_flow_code"],
            transformed["trade_flow_name"],
            transformed["hs_code"],
            transformed["cmd_code"],
            transformed["cmd_name_en"],
            transformed["frequency"],
            transformed["trade_value"],
        ))

    conn = get_db_connection()
    cur = conn.cursor()
    if db_type == "PostgreSQL":
        insert_sql = f"""
            INSERT INTO "{table_name}" (
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
            st.error(f"DB error: {e}")
            return 0
        finally:
            cur.close()
            conn.close()
    else:
        insert_sql = f"""
            INSERT IGNORE INTO `{table_name}` (
                reporter_code, reporter_name,
                partner_code, partner_name,
                year, trade_flow_code, trade_flow_name,
                hs_code, cmd_code, cmd_name_en,
                frequency, trade_value
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        try:
            cur.executemany(insert_sql, values)
            conn.commit()
            return cur.rowcount
        except Exception as e:
            conn.rollback()
            st.error(f"DB error: {e}")
            return 0
        finally:
            cur.close()
            conn.close()

# ---------- Initialize session state ----------
if "step" not in st.session_state:
    st.session_state.step = "api_key"
if "api_key_valid" not in st.session_state:
    st.session_state.api_key_valid = False
if "filters" not in st.session_state:
    st.session_state.filters = {}
if "preview_data" not in st.session_state:
    st.session_state.preview_data = None
if "full_data" not in st.session_state:
    st.session_state.full_data = None

# ---------- Sidebar (always visible) ----------
with st.sidebar:
    st.markdown("## 🔌 Connection Status")
    if "db_credentials" in st.session_state:
        st.success("✅ Database connected")
        if st.button("Disconnect"):
            del st.session_state.db_credentials
            if "table_names" in st.session_state:
                del st.session_state.table_names
            st.rerun()
    else:
        st.info("Database not connected")
    st.markdown("---")
    if "api_key" in st.session_state:
        st.success("✅ API key set")
    else:
        st.warning("API key not set")
    if st.button("🗑️ Clear Cache"):
        # Delete on‑disk cache folder
        if os.path.exists(CACHE_DIR):
            shutil.rmtree(CACHE_DIR)

        # Clear Streamlit's in‑memory function cache
        fetch_countries.clear()
        fetch_hs_codes.clear()

        # Remove all user‑specific session state
        keys_to_remove = [
            "countries", "hs_codes", "api_key", "db_credentials",
            "table_names", "preview_data", "full_data", "filters",
            "reference_loaded", "step"
        ]
        for key in keys_to_remove:
            if key in st.session_state:
                del st.session_state[key]

        # Explicitly set step back to api_key for the next run
        st.session_state.step = "api_key"

        st.success("Cache cleared!")
        st.rerun()

# ----- API Key -----
if st.session_state.step == "api_key":
    st.markdown("### Enter your UN Comtrade API Key")
    api_key = st.text_input("API Key", type="password", key="api_key_input")
    if st.button("Validate Key"):
        if not api_key:
            st.error("Please enter an API key.")
        else:
            # Test key with a simple call
            test_params = {
                "reporterCode": "360",
                "period": "2020",
                "flowCode": "M",
                "cmdCode": "1001",
                "fmt": "json"
            }
            headers = {"Ocp-Apim-Subscription-Key": api_key}
            try:
                resp = requests.get(f"{BASE_URL}/C/A/HS", headers=headers, params=test_params)
                if resp.status_code == 200:
                    data = resp.json()
                    if data.get("error"):
                        st.error(f"Invalid API key: {data['error']}")
                    else:
                        # Key is valid – store it
                        st.session_state.api_key = api_key
                        
                        # Load reference data now (with spinners)
                        with st.spinner("Loading country list..."):
                            st.session_state.countries = fetch_countries()
                        with st.spinner("Loading HS codes..."):
                            st.session_state.hs_codes = fetch_hs_codes()
                        
                        # Add special options
                        if "0" not in st.session_state.countries:
                            st.session_state.countries["0"] = "World"
                        if "ALL" not in st.session_state.countries:
                            st.session_state.countries["ALL"] = "All"
                        
                        # Move to Step 2
                        st.session_state.step = "filters"
                        st.rerun()
                else:
                    st.error(f"API test failed (status {resp.status_code}). Check your key.")
            except Exception as e:
                st.error(f"Connection error: {e}")
    st.markdown("Don't have a key? [Get it here](https://comtradedeveloper.un.org/)")

# ----- Filter Selection and Preview -----
elif st.session_state.step == "filters":
    st.markdown("### Select filters and preview data")

    trade_flows = {
        "M": "Import", "X": "Export", "RM": "Re-import", "RX": "Re-export",
        "DX": "Domestic Export", "FM": "Foreign Import",
        "MIP": "Import of goods for inward processing",
        "MOP": "Import of goods after outward processing",
        "XIP": "Export of goods after inward processing",
        "XOP": "Export of goods for outward processing",
    }

    col1, col2 = st.columns(2)
    with col1:
        reporter_codes = st.multiselect(
            "Reporter Country",
            options=list(st.session_state.countries.keys()),
            format_func=lambda x: st.session_state.countries[x],
            default=["360"]
        )
    with col2:
        partner_codes = st.multiselect(
            "Partner Country",
            options=list(st.session_state.countries.keys()),
            format_func=lambda x: st.session_state.countries[x],
            default=["0"]
        )

    col3, col4 = st.columns(2)
    with col3:
        start_year = st.number_input("Start Year", min_value=2000, max_value=2025, value=2019, step=1)
    with col4:
        end_year = st.number_input("End Year", min_value=2000, max_value=2025, value=2025, step=1)

    col5, col6 = st.columns(2)
    with col5:
        flow = st.selectbox("Trade Flow", options=list(trade_flows.keys()), format_func=lambda x: trade_flows[x])
    with col6:
        # 👉 HS Code is now a multiselect
        selected_hs_codes = st.multiselect(
            "HS Code",
            options=list(st.session_state.hs_codes.keys()),
            format_func=lambda x: f"{x} - {st.session_state.hs_codes[x]}",
            default=["0101"]  # default to one code
        )

    freq = st.selectbox("Frequency", options=["A", "M"], format_func=lambda x: "Annual" if x == "A" else "Monthly")

    # Store current filters
    current_filters = {
        "reporter_codes": reporter_codes,
        "partner_codes": partner_codes,
        "start_year": start_year,
        "end_year": end_year,
        "flow": flow,
        "hs_codes": selected_hs_codes,          # now a list
        "freq": freq,
        "years": list(range(int(start_year), int(end_year) + 1))
    }

    # ---------- Preview Button ----------
    if st.button("🔍 Preview Data (first 5 rows)"):
        if not reporter_codes:
            st.error("Select at least one reporter.")
        elif not partner_codes:
            st.error("Select at least one partner.")
        elif not selected_hs_codes:
            st.error("Select at least one HS code.")
        elif start_year > end_year:
            st.error("Start year must be ≤ end year.")
        else:
            # Determine partner string
            if "ALL" in partner_codes:
                partner_str = None  # omit partnerCode to fetch all partners
                st.info("Fetching data for all partners individually. This may take a while.")
            else:
                partner_list = ["0"] if "0" in partner_codes else partner_codes
                partner_str = ",".join(partner_list)

            # Combine selected HS codes into a comma‑separated string
            hs_codes_str = ",".join(selected_hs_codes)

            all_data = []
            for rep in reporter_codes:
                records = fetch_comtrade_data(
                    rep, partner_str, current_filters["years"],
                    flow, hs_codes_str, freq
                )
                if records:
                    all_data.extend(records)

            # If "ALL" selected, remove world aggregate (partnerCode == 0)
            if "ALL" in partner_codes:
                all_data = [r for r in all_data if r.get("partnerCode") != 0]

            if all_data:
                # Transform all records to DB format using the updated transform_record
                all_transformed = []
                for raw in all_data:
                    # transform_record no longer needs the separate hs_code argument
                    all_transformed.append(transform_record(
                        raw, freq, st.session_state.hs_codes
                    ))

                # Deduplicate based on unique key
                unique_data = {}
                for item in all_transformed:
                    key = (item["reporter_code"], item["partner_code"], item["year"],
                           item["trade_flow_code"], item["hs_code"], item["frequency"])
                    if key not in unique_data:
                        unique_data[key] = item

                unique_list = list(unique_data.values())
                st.session_state.full_data = all_data
                st.session_state.filters = current_filters

                # Sort by year and take first 5 for preview
                sorted_unique = sorted(unique_list, key=lambda x: x["year"])
                sampled_data = sorted_unique[:5]

                # If user selected "World" (aggregate), override partner_name to "World"
                if "0" in partner_codes:
                    for item in sampled_data:
                        item["partner_name"] = "World"
                # If "ALL" selected, we leave actual partner names

                st.session_state.preview_data = sampled_data
                st.success(f"Fetched {len(unique_list)} unique records. Showing first 5 sorted by year:")
                df_preview = pd.DataFrame(sampled_data)
                column_order = ["reporter_code", "reporter_name", "partner_code", "partner_name",
                                "year", "trade_flow_code", "trade_flow_name", "hs_code",
                                "cmd_code", "cmd_name_en", "frequency", "trade_value"]
                st.dataframe(df_preview[column_order])
            else:
                st.warning("No data returned for these filters.")

    # ---------- Confirm Button ----------
    if st.session_state.preview_data is not None:
        if st.button("✅ Confirm Filters & Proceed to Database"):
            st.session_state.step = "db_connect"
            st.rerun()

# ----- Database Connection and Insertion -----
elif st.session_state.step == "db_connect":
    st.markdown("### Insert to db")

    # If already connected
    if "db_credentials" in st.session_state:
        # Fetch table names if not cached
        if "table_names" not in st.session_state:
            with st.spinner("Loading tables..."):
                st.session_state.table_names = get_table_names()

        if st.session_state.table_names:
            selected_table = st.selectbox("Select target table", st.session_state.table_names)

            # Insert button
            if st.button("🚀 Insert All Data"):
                if st.session_state.full_data is None:
                    st.error("No data to insert. Please go back and preview again.")
                else:
                    with st.spinner("Inserting into database..."):
                        inserted = insert_records(
                            st.session_state.full_data,
                            st.session_state.filters["freq"],
                            st.session_state.hs_codes,
                            selected_table
                        )
                    if inserted > 0:
                        st.success(f"✅ {inserted} rows inserted into {selected_table}.")
                    else:
                        st.warning("No new rows inserted (duplicates or error).")

            # Single Back to Filters button (resets preview)
            if st.button("← Back to Filters", key="back_from_connected"):
                st.session_state.preview_data = None
                st.session_state.full_data = None
                st.session_state.step = "filters"
                st.rerun()

        else:
            st.warning("No tables found in the database.")
            if st.button("← Back to Filters", key="back_no_tables"):
                st.session_state.preview_data = None
                st.session_state.full_data = None
                st.session_state.step = "filters"
                st.rerun()

    else:
        # Not connected – show connection form
        db_type = st.radio("Database Type", ["PostgreSQL", "MySQL"], horizontal=True, key="db_type_radio")
        with st.form("db_connection_form"):
            db_host = st.text_input("Host *", value="localhost")
            db_port = st.text_input("Port *", value="5432" if db_type == "PostgreSQL" else "3306")
            db_name = st.text_input("Database Name *", value="comtrade_db")
            db_user = st.text_input("Username *", value="postgres" if db_type == "PostgreSQL" else "root")
            db_password = st.text_input("Password" + (" *" if db_type == "PostgreSQL" else ""), 
                                         type="password")
            submitted = st.form_submit_button("Connect")

        if submitted:
            missing = []
            if not db_host: missing.append("Host")
            if not db_port: missing.append("Port")
            if not db_name: missing.append("Database Name")
            if not db_user: missing.append("Username")
            if db_type == "PostgreSQL" and not db_password:
                missing.append("Password")
            if missing:
                st.error(f"Missing required fields: {', '.join(missing)}")
            else:
                try:
                    if db_type == "PostgreSQL":
                        conn = psycopg2.connect(
                            host=db_host,
                            port=db_port,
                            database=db_name,
                            user=db_user,
                            password=db_password
                        )
                    else:  # MySQL
                        conn = pymysql.connect(
                            host=db_host,
                            port=int(db_port),
                            user=db_user,
                            password=db_password if db_password else "",
                            database=db_name,
                            charset='utf8mb4'
                        )
                    conn.close()
                    st.session_state.db_credentials = {
                        "host": db_host,
                        "port": db_port,
                        "database": db_name,
                        "user": db_user,
                        "password": db_password
                    }
                    st.session_state.db_type = db_type
                    st.success("Connected!")
                    st.rerun()
                except Exception as e:
                    st.error(f"Connection failed: {e}")

        # Back to Filters button (when not connected) – resets preview
        if st.button("← Back to Filters", key="back_not_connected"):
            st.session_state.preview_data = None
            st.session_state.full_data = None
            st.session_state.step = "filters"
            st.rerun()