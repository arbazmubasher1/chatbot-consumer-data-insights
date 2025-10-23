# -------------------------------------------------------
# Restaurant Responses & Ops Analytics – Full Dashboard
# -------------------------------------------------------
# Tabs:
# 1) Overview
# 2) Time Intelligence
# 3) Lifecycle & SLA
# 4) Themes & Text
# 5) Branch & Agent
# 6) Customer Analysis
# 7) Risk & Stability
# 8) Data Quality
# 9) Classification Audit
# -------------------------------------------------------

import os
import io
import re
import requests
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from wordcloud import WordCloud
import matplotlib.pyplot as plt
from collections import Counter
from typing import Optional

# =========================
# Page Config (MUST be first)
# =========================
st.set_page_config(page_title="Restaurant Responses Dashboard", layout="wide")

# =========================
# Shift labels & SPARK rules
# =========================
SHIFT_TIMES = {
    "10AM–4PM":  "10 AM – 4 PM",
    "4PM–10PM":  "4 PM – 10 PM",
    "10PM–3AM":  "10 PM – 3 AM",
    "Outside Slot (3AM–10AM)": "3 AM – 10 AM",   # catch-all so records never go missing
}
SHIFT_ORDER = ["10AM–4PM", "4PM–10PM", "10PM–3AM", "Outside Slot (3AM–10AM)"]

def classify_spark(tag: str) -> str:
    if not isinstance(tag, str):
        return "Unclassified"
    t = tag.lower()

    # Speed of Service
    if any(k in t for k in ["time above", "time between", "delay", "late", "slow", "time", "responding"]):
        return "SPARK: Speed of Service"

    # Product Quality
    if any(k in t for k in [
        "cold","soggy","undercooked","overcooked","raw","oily","unfresh","dryness","dry",
        "stale","patty size","burnt","chicken item","bakery item"
    ]):
        return "SPARK: Product Quality"

    # Accuracy
    if any(k in t for k in [
        "wrong","missed","missing","addons","dip missed","fries missed",
        "wrong product","wrong sauce","product missed"
    ]):
        return "SPARK: Accuracy"

    # Relationship
    if any(k in t for k in ["service", "remarks", "compensated", "delivery", "others", "not responding"]):
        return "SPARK: Relationship"

    # Keep it Clean
    if any(k in t for k in ["foreign object", "hygiene", "clean", "dirty"]):
        return "SPARK: Keep it Clean"

    return "Unclassified"

# =========================
# Data Source (Local ➜ Secrets URL ➜ Uploader)
# =========================
DATA_PATH_CANDIDATES = [
    "cx9_tickets_1760606268482.xlsx",
    "/mnt/data/1234567.xlsx",  # your uploaded sample path
]

def _read_excel_from_bytes(xls_bytes: bytes, preferred_sheet: str = "tickets") -> pd.DataFrame:
    xls = pd.ExcelFile(io.BytesIO(xls_bytes))
    sheet = preferred_sheet if preferred_sheet in xls.sheet_names else xls.sheet_names[0]
    df = pd.read_excel(io.BytesIO(xls_bytes), sheet_name=sheet)
    return df

def _try_load_local() -> Optional[bytes]:
    for path in DATA_PATH_CANDIDATES:
        if path and os.path.exists(path):
            try:
                with open(path, "rb") as f:
                    return f.read()
            except Exception:
                continue
    return None

def _try_load_from_secret_url() -> Optional[bytes]:
    try:
        url = st.secrets["data"]["url"]
        if not url:
            return None
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        return r.content
    except Exception:
        return None

@st.cache_data
def parse_excel_bytes(xls_bytes: bytes, preferred_sheet: str = "tickets") -> pd.DataFrame:
    return _read_excel_from_bytes(xls_bytes, preferred_sheet)

def load_data(preferred_sheet: str = "tickets") -> pd.DataFrame:
    xls_bytes = _try_load_local()
    if xls_bytes is None:
        xls_bytes = _try_load_from_secret_url()
    if xls_bytes is None:
        st.warning("No local file found and no secret URL configured. Upload an Excel file to continue.")
        up = st.file_uploader("Upload responses Excel (.xlsx)", type=["xlsx"])
        if up is None:
            st.stop()
        xls_bytes = up.read()
    return parse_excel_bytes(xls_bytes, preferred_sheet)

# =========================
# Load & Prepare Data
# =========================
@st.cache_data
def prepare_data(raw: pd.DataFrame) -> pd.DataFrame:
    df = raw.copy()

    # Trim accidental whitespace in headers
    df.columns = [str(c).strip() for c in df.columns]

    # Parse timestamps
    time_cols = [
        "Updated At", "Updated At", "First Public Reply At", "First Private Reply At",
        "Last Public Reply At", "Last Private Reply At", "Opened At", "Closed At",
        "Re-Opened At", "First Response Till", "Due Date"
    ]
    for col in time_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    created = pd.to_datetime(df.get("Updated At"), errors="coerce")
    df["Date"] = created.dt.date
    df["Hour"] = created.dt.hour
    df["DayOfWeek"] = created.dt.day_name()
    df["IsWeekend"] = df["DayOfWeek"].isin(["Saturday", "Sunday"])

    # ISO week fields + real datetime WeekStart (Monday)
    iso = created.dt.isocalendar()
    df["ISO_Year"] = iso.year
    df["ISO_Week"] = iso.week
    df["WeekStart"] = (created - pd.to_timedelta(created.dt.weekday, unit="D")).dt.normalize()

    # Shifts (updated ranges)
    def shift_label(h):
        if pd.isna(h): 
            return np.nan
        h = int(h)
        if 10 <= h < 16:
            return "10AM–4PM"
        elif 16 <= h < 22:
            return "4PM–10PM"
        elif h >= 22 or h < 3:      # wraps over midnight
            return "10PM–3AM"
        else:                        # 3:00–9:59
            return "Outside Slot (3AM–10AM)"
    df["Shift"] = df["Hour"].apply(shift_label)

    # Durations
    df["FRT_min"] = np.nan
    if {"First Public Reply At","Updated At"}.issubset(df.columns):
        df["FRT_min"] = (df["First Public Reply At"] - df["Updated At"]).dt.total_seconds() / 60
    df["TTR_min"] = np.nan
    if {"Closed At","Updated At"}.issubset(df.columns):
        df["TTR_min"] = (df["Closed At"] - df["Updated At"]).dt.total_seconds() / 60

    # Clean categoricals
    for c in ["Branch Name", "Feedback Head", "Tags", "Team Member", "Pipeline Stage", "Status"]:
        if c in df.columns:
            df[c] = df[c].fillna("Unspecified")

    # Normalize booleans
    for b in ["First Response SLA", "Resolution SLA", "SLA Breach", "Re-Opened", "Opened"]:
        if b in df.columns:
            df[b] = df[b].astype(str).str.lower().map({"true": True, "false": False})

    # SPARK from Tags
    df["SPARK"] = df["Tags"].apply(classify_spark) if "Tags" in df.columns else "Unclassified"

    return df

raw_df = load_data("tickets")
df = prepare_data(raw_df)

# =========================
# Sidebar Filters
# =========================
st.sidebar.header("Filters")
branch_options = sorted(df["Branch Name"].dropna().unique()) if "Branch Name" in df.columns else []
feedback_options = sorted(df["Feedback Head"].dropna().unique()) if "Feedback Head" in df.columns else []
shift_options = [s for s in SHIFT_ORDER if "Shift" in df.columns and s in df["Shift"].unique()]

sel_branches = st.sidebar.multiselect("Branches", options=branch_options)
sel_feedback = st.sidebar.multiselect("Feedback Type", options=feedback_options)
sel_shifts = st.sidebar.multiselect("Shifts", options=shift_options)

if "Date" in df.columns:
    date_min, date_max = df["Date"].min(), df["Date"].max()
    sel_dates = st.sidebar.date_input("Date range", [date_min, date_max])
else:
    sel_dates = []

filtered = df.copy()
if sel_branches and "Branch Name" in filtered.columns:
    filtered = filtered[filtered["Branch Name"].isin(sel_branches)]
if sel_feedback and "Feedback Head" in filtered.columns:
    filtered = filtered[filtered["Feedback Head"].isin(sel_feedback)]
if sel_shifts and "Shift" in filtered.columns:
    filtered = filtered[filtered["Shift"].isin(sel_shifts)]
if "Date" in filtered.columns and len(sel_dates) == 2:
    filtered = filtered[(filtered["Date"] >= sel_dates[0]) & (filtered["Date"] <= sel_dates[1])]

# =========================
# Exclusion of Demoters with "Not Responding" in Tags — DISABLED (keep all data)
# =========================
pre_analysis_df = filtered.copy()
excluded_mask = pd.Series(False, index=filtered.index)
excluded_count = 0
audit_payload = {"pre": pre_analysis_df, "post": filtered.copy(), "mask": excluded_mask, "count": excluded_count}

# =========================
# Helpers
# =========================
def safe_count(frame: pd.DataFrame) -> int:
    return int(frame.shape[0]) if frame is not None else 0

def pct(n: float, d: float) -> float:
    return (100.0 * n / d) if d else 0.0

def compute_nps(df_part: pd.DataFrame) -> float:
    if "Feedback Head" not in df_part.columns or df_part.empty:
        return 0.0
    p = (df_part["Feedback Head"] == "Promoter").sum()
    d = (df_part["Feedback Head"] == "Demoter").sum()
    base = len(df_part)
    if base == 0:
        return 0.0
    return (p / base * 100.0) - (d / base * 100.0)

# =========================
# Field resolvers & phone utils (for Customer Analysis)
# =========================
POSSIBLE_PHONE_COLS = [
    "Customer CLI",
    "Customer Phone", "customer_phone",
    "Phone", "Phone Number", "Contact", "Contact Number",
]
POSSIBLE_NAME_COLS  = ["Customer Name", "customer_name", "Name"]
POSSIBLE_ADDR_COLS  = [
    "Customer Address", "customer_address",
    "Delivery Address", "Address", "Location",
]
TICKET_ID_COLS      = ["Ticket number", "Ticket ID", "id", "ID"]

def col_exists(frame: pd.DataFrame, candidates: list[str]) -> Optional[str]:
    for c in candidates:
        if c in frame.columns:
            return c
    return None

PHONE_COL  = col_exists(df, POSSIBLE_PHONE_COLS)
NAME_COL   = col_exists(df, POSSIBLE_NAME_COLS)
ADDR_COL   = col_exists(df, POSSIBLE_ADDR_COLS)
TICKET_COL = col_exists(df, TICKET_ID_COLS)

# If there's no explicit address in the data, use Branch Name as an "address proxy"
ADDRESS_PROXY_COL = None
if not ADDR_COL and "Branch Name" in df.columns:
    ADDRESS_PROXY_COL = "Branch Name"

def digits_only(s: str) -> str:
    try:
        s = str(s)
    except Exception:
        s = ""
    return re.sub(r"\D+", "", s)

def normalize_phone_series(s: pd.Series) -> pd.Series:
    try:
        return s.astype(str).map(digits_only)
    except Exception:
        return pd.Series([""] * len(s), index=s.index)

# Precompute normalized phones for both full and filtered frames (if present)
if PHONE_COL:
    df["_PhoneNorm"] = normalize_phone_series(df[PHONE_COL])
    filtered["_PhoneNorm"] = normalize_phone_series(filtered[PHONE_COL])
else:
    df["_PhoneNorm"] = ""
    filtered["_PhoneNorm"] = ""

# =========================
# Tabs
# =========================
tabs = st.tabs([
    "Overview",
    "Time Intelligence",
    "Lifecycle & SLA",
    "Themes & Text",
    "Branch & Agent",
    "Customer Analysis",
    "Risk & Stability",
    "Data Quality",
    "Classification Audit",
])

# ======================================================
# 1) OVERVIEW
# ======================================================
with tabs[0]:
    st.title("Responses & Feedback Overview")

    total_responses = safe_count(filtered)
    demoters = int((filtered["Feedback Head"] == "Demoter").sum()) if "Feedback Head" in filtered.columns else 0
    promoters = int((filtered["Feedback Head"] == "Promoter").sum()) if "Feedback Head" in filtered.columns else 0
    neutrals = int((filtered["Feedback Head"] == "Neutral").sum()) if "Feedback Head" in filtered.columns else 0
    nps_all = compute_nps(filtered)

    sla_breach = int(filtered.get("SLA Breach", pd.Series([False]*len(filtered))).fillna(False).sum()) if "SLA Breach" in filtered.columns else 0
    reopened = int(filtered.get("Re-Opened", pd.Series([False]*len(filtered))).fillna(False).sum()) if "Re-Opened" in filtered.columns else 0

    c1, c2, c3, c4, c5, c6, c7 = st.columns(7)
    c1.metric("Total Responses", total_responses)
    c2.metric("Promoter %", f"{pct(promoters, total_responses):.1f}%")
    c3.metric("Demoter %", f"{pct(demoters, total_responses):.1f}%")
    c4.metric("Neutral %", f"{pct(neutrals, total_responses):.1f}%")
    c5.metric("NPS", f"{nps_all:.1f}")
    c6.metric("SLA Breach %", f"{pct(sla_breach, total_responses):.1f}%")
    c7.metric("Reopen %", f"{pct(reopened, total_responses):.1f}%")

    if "Branch Name" in filtered.columns:
        st.subheader("Responses by Branch")
        branch_counts = filtered["Branch Name"].value_counts().reset_index()
        branch_counts.columns = ["Branch", "Responses"]
        fig = px.bar(branch_counts, x="Responses", y="Branch", orientation="h", text="Responses",
                     color="Branch", title="Responses by Branch")
        st.plotly_chart(fig, use_container_width=True)

    if "Tags" in filtered.columns:
        st.subheader("Top Response Categories (Tags)")
        top_tags = filtered["Tags"].value_counts().head(15).reset_index()
       
