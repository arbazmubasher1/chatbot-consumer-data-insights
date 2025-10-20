# -------------------------------------------------------
# Restaurant Responses & Ops Analytics – Full Dashboard
# -------------------------------------------------------
# Tabs:
# 1) Overview
# 2) Time Intelligence
# 3) Lifecycle & SLA
# 4) Themes & Text
# 5) Branch & Agent
# 6) Risk & Stability
# 7) Data Quality
# 8) Classification Audit
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

# =========================
# Page Config (MUST be first)
# =========================
st.set_page_config(page_title="Restaurant Responses Dashboard", layout="wide")

# =========================
# Shift labels & SPARK rules
# =========================
SHIFT_TIMES = {
    "Breakfast": "7 AM – 12 PM",
    "Lunch": "12 PM – 5 PM",
    "Dinner": "5 PM – 11 PM",
    "Late Night": "11 PM – 7 AM",
}

def classify_spark(tag: str) -> str:
    if not isinstance(tag, str):
        return "Unclassified"
    t = tag.lower()

    # Speed of Service
    if any(k in t for k in ["time above", "time between", "delay", "late", "slow", "time", "responding"]):
        return "SPARK: Speed of Service"

    # Product Quality
    if any(k in t for k in ["cold", "soggy", "undercooked", "overcooked", "raw", "oily", "unfresh",
                            "dryness", "dry", "stale", "patty size", "burnt", "chicken item", "bakery item"]):
        return "SPARK: Product Quality"

    # Accuracy
    if any(k in t for k in ["wrong", "missed", "missing", "addons", "dip missed", "fries missed",
                            "wrong product", "wrong sauce", "product missed"]):
        return "SPARK: Accuracy"

    # Relationship (CX / delivery / others)
    if any(k in t for k in ["service", "remarks", "compensated", "delivery", "others", "not responding"]):
        return "SPARK: Relationship"

    # Keep it Clean
    if any(k in t for k in ["foreign object", "hygiene", "clean", "dirty"]):
        return "SPARK: Keep it Clean"

    return "Unclassified"

# =========================
# Data Source (Local ➜ Secrets URL ➜ Uploader)
# =========================
DATA_PATH = "cx9_tickets_1760606268482.xlsx"  # keep for local runs

def _read_excel_from_bytes(xls_bytes: bytes, preferred_sheet: str = "tickets") -> pd.DataFrame:
    xls = pd.ExcelFile(io.BytesIO(xls_bytes))
    sheet = preferred_sheet if preferred_sheet in xls.sheet_names else xls.sheet_names[0]
    df = pd.read_excel(io.BytesIO(xls_bytes), sheet_name=sheet)
    return df

def _try_load_local(path: str) -> bytes | None:
    if path and os.path.exists(path):
        with open(path, "rb") as f:
            return f.read()
    return None

def _try_load_from_secret_url() -> bytes | None:
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
    xls_bytes = _try_load_local(DATA_PATH)
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

    # Parse timestamps
    time_cols = [
        "Created At", "Updated At", "First Public Reply At", "First Private Reply At",
        "Last Public Reply At", "Last Private Reply At", "Opened At", "Closed At",
        "Re-Opened At", "First Response Till", "Due Date"
    ]
    for col in time_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    created = pd.to_datetime(df.get("Created At"), errors="coerce")
    df["Date"] = created.dt.date
    df["Hour"] = created.dt.hour
    df["DayOfWeek"] = created.dt.day_name()
    df["IsWeekend"] = df["DayOfWeek"].isin(["Saturday", "Sunday"])

    # ISO week fields + real datetime WeekStart (Monday)
    iso = created.dt.isocalendar()
    df["ISO_Year"] = iso.year
    df["ISO_Week"] = iso.week
    df["WeekStart"] = (created - pd.to_timedelta(created.dt.weekday, unit="D")).dt.normalize()

    # Shifts
    def shift_label(h):
        if pd.isna(h): return np.nan
        h = int(h)
        if 7 <= h < 12: return "Breakfast"
        elif 12 <= h < 17: return "Lunch"
        elif 17 <= h < 23: return "Dinner"
        else: return "Late Night"
    df["Shift"] = df["Hour"].apply(shift_label)

    # Durations
    df["FRT_min"] = np.nan
    if {"First Public Reply At","Created At"}.issubset(df.columns):
        df["FRT_min"] = (df["First Public Reply At"] - df["Created At"]).dt.total_seconds() / 60
    df["TTR_min"] = np.nan
    if {"Closed At","Created At"}.issubset(df.columns):
        df["TTR_min"] = (df["Closed At"] - df["Created At"]).dt.total_seconds() / 60

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
shift_options = [s for s in ["Breakfast", "Lunch", "Dinner", "Late Night"] if "Shift" in df.columns and s in df["Shift"].unique()]

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
# Exclusion of Demoters with "Not Responding" in Tags
# =========================
pre_analysis_df = filtered.copy()  # snapshot before exclusions (for audit only)

excluded_mask = pd.Series(False, index=filtered.index)
if set(["Tags", "Feedback Head"]).issubset(filtered.columns):
    nr_mask = filtered["Tags"].str.contains(r"\bNot Responding\b", case=False, na=False)
    demoter_mask = filtered["Feedback Head"].astype(str).str.casefold().eq("demoter")
    excluded_mask = nr_mask & demoter_mask

excluded_count = int(excluded_mask.sum())
filtered = filtered[~excluded_mask].copy()  # remove those rows from all downstream analyses

# Keep an audit payload for the tab
audit_payload = {
    "pre": pre_analysis_df,        # before exclusion
    "post": filtered.copy(),       # after exclusion
    "mask": excluded_mask,         # which rows were excluded
    "count": excluded_count,
}

pre_classification = filtered.copy()  # snapshot before we fix labels

misclassified_mask = pd.Series(False, index=filtered.index)
if set(["Tags", "Feedback Head"]).issubset(filtered.columns):
    nr_mask = filtered["Tags"].str.contains(r"\bNot Responding\b", case=False, na=False)
    demoter_mask = filtered["Feedback Head"].astype(str).str.casefold().eq("demoter")
    misclassified_mask = nr_mask & demoter_mask

# Audit metrics (before fix)
misclassified_count = int(misclassified_mask.sum())
total_demoters_pre = int((pre_classification["Feedback Head"] == "Demoter").sum()) if "Feedback Head" in pre_classification.columns else 0
share_of_demoters = (misclassified_count / total_demoters_pre * 100.0) if total_demoters_pre else 0.0

# NPS before fix
def compute_nps(df_part: pd.DataFrame) -> float:
    if "Feedback Head" not in df_part.columns or df_part.empty:
        return 0.0
    total = len(df_part)
    if total == 0:
        return 0.0
    promoters = (df_part["Feedback Head"] == "Promoter").sum()
    demoters = (df_part["Feedback Head"] == "Demoter").sum()
    return (promoters/total*100.0) - (demoters/total*100.0)

nps_before_fix = compute_nps(pre_classification)

# Apply the fix: reclassify Demoter → Neutral when Tags contain "Not Responding"
if misclassified_count > 0:
    filtered.loc[misclassified_mask, "Feedback Head"] = "Neutral"

# NPS after fix
nps_after_fix = compute_nps(filtered)
nps_delta = nps_after_fix - nps_before_fix

audit_payload = {
    "pre": pre_classification,
    "post": filtered.copy(),
    "mask": misclassified_mask,
    "count": misclassified_count,
    "demoters_pre": total_demoters_pre,
    "share": share_of_demoters,
    "nps_before": nps_before_fix,
    "nps_after": nps_after_fix,
    "nps_delta": nps_delta,
}

# =========================
# Helpers
# =========================
def safe_count(frame: pd.DataFrame) -> int:
    return int(frame.shape[0]) if frame is not None else 0

def pct(n: float, d: float) -> float:
    return (100.0 * n / d) if d else 0.0

# =========================
# Tabs
# =========================
tabs = st.tabs([
    "Overview",
    "Time Intelligence",
    "Lifecycle & SLA",
    "Themes & Text",
    "Branch & Agent",
    "Risk & Stability",
    "Data Quality",
    "Classification Audit"
])

# ======================================================
# 1) OVERVIEW
# ======================================================
with tabs[0]:
    st.title("Responses & Feedback Overview")

    total_responses = safe_count(filtered)
    unique_customers = filtered["Customer CLI"].nunique() if "Customer CLI" in filtered.columns else 0
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
        top_tags.columns = ["Tag", "Responses"]
        fig2 = px.bar(top_tags, x="Responses", y="Tag", orientation="h", text="Responses",
                      color="Tag", title="Top Tags (by Response Volume)")
        st.plotly_chart(fig2, use_container_width=True)

    if set(["Branch Name", "Feedback Head"]).issubset(filtered.columns):
        st.subheader("Feedback Sentiment by Branch")
        sentiment = filtered.groupby(["Branch Name", "Feedback Head"]).size().reset_index(name="Count")
        fig3 = px.bar(sentiment, x="Branch Name", y="Count", color="Feedback Head", barmode="stack",
                      title="Feedback Sentiment by Branch")
        st.plotly_chart(fig3, use_container_width=True)

# ======================================================
# 2) TIME INTELLIGENCE
# ======================================================
with tabs[1]:
    st.title("Time Intelligence")

    # Weekday vs Weekend volume + Demoter % + NPS
    if set(["IsWeekend", "Feedback Head", "Ticket number"]).issubset(filtered.columns):
        tmp = filtered.copy()
        tmp["DayType"] = tmp["IsWeekend"].apply(lambda x: "Weekend" if x else "Weekday")
        daytype = tmp.groupby("DayType").agg(
            Responses=("Ticket number", "count"),
            Demoters=("Feedback Head", lambda s: (s == "Demoter").sum()),
            Promoters=("Feedback Head", lambda s: (s == "Promoter").sum()),
        ).reset_index()
        daytype["Demoter %"] = daytype["Demoters"] / daytype["Responses"] * 100.0
        daytype["NPS"] = (daytype["Promoters"]/daytype["Responses"]*100.0) - (daytype["Demoters"]/daytype["Responses"]*100.0)

        bar = go.Bar(x=daytype["DayType"], y=daytype["Responses"], name="Responses")
        line = go.Scatter(x=daytype["DayType"], y=daytype["Demoter %"], name="Demoter %", yaxis="y2", mode="lines+markers")
        fig_combo = go.Figure(data=[bar, line])
        fig_combo.update_layout(
            title="Weekday vs Weekend Responses (with Demoter %)",
            yaxis=dict(title="Responses"),
            yaxis2=dict(title="Demoter %", overlaying="y", side="right"),
            legend=dict(orientation="h")
        )
        st.plotly_chart(fig_combo, use_container_width=True)

        fig_nps_dw = px.bar(
            daytype, x="DayType", y="NPS",
            title="Weekday vs Weekend NPS",
            text=daytype["NPS"].round(1)
        )
        fig_nps_dw.update_layout(xaxis_title="", yaxis_title="NPS")
        st.plotly_chart(fig_nps_dw, use_container_width=True)

    # Day of Week profile
    if "DayOfWeek" in filtered.columns:
        order = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
        dow = filtered.groupby("DayOfWeek").size().reindex(order).reset_index(name="Responses")
        fig_dow = px.line(dow, x="DayOfWeek", y="Responses", markers=True, title="Responses by Day of Week")
        st.plotly_chart(fig_dow, use_container_width=True)

    # Intraday heatmap (Hour x DayOfWeek)
    if set(["Hour", "DayOfWeek", "Ticket number"]).issubset(filtered.columns):
        intraday = filtered.dropna(subset=["Hour"]).copy()
        intraday_pivot = intraday.pivot_table(index="Hour", columns="DayOfWeek",
                                              values="Ticket number", aggfunc="count", fill_value=0)
        intraday_pivot = intraday_pivot.reindex(columns=["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"], fill_value=0)
        fig_heat = px.imshow(intraday_pivot, color_continuous_scale="YlOrRd", title="Intraday Responses Heatmap (Hour × Day)")
        st.plotly_chart(fig_heat, use_container_width=True)

    # Weekly Response Trend (ISO)
    if set(["WeekStart", "ISO_Year", "ISO_Week", "Ticket number"]).issubset(filtered.columns):
        wk = (filtered
              .dropna(subset=["WeekStart"])
              .groupby(["ISO_Year", "ISO_Week", "WeekStart"])
              .size()
              .reset_index(name="Responses")
              .sort_values("WeekStart"))
        wk["WeekLabel"] = wk["ISO_Year"].astype(str) + "-W" + wk["ISO_Week"].astype(str).str.zfill(2)
        fig_weekly = px.line(
            wk, x="WeekStart", y="Responses", markers=True,
            hover_data={"WeekStart": False, "WeekLabel": True, "ISO_Year": False, "ISO_Week": False}
        )
        fig_weekly.update_layout(title="Weekly Response Trend (ISO weeks, Monday start)",
                                 xaxis_title="Week (Mon start)", yaxis_title="Responses")
        st.plotly_chart(fig_weekly, use_container_width=True)

    # Shift-wise NPS + volumes (with time slots)
    if set(["Shift","Feedback Head","Ticket number"]).issubset(filtered.columns):
        nps_shift = (
            filtered.groupby("Shift").agg(
                Responses=("Ticket number","count"),
                Promoters=("Feedback Head", lambda s: (s=="Promoter").sum()),
                Demoters=("Feedback Head", lambda s: (s=="Demoter").sum())
            ).reset_index()
        )
        nps_shift["NPS"] = (nps_shift["Promoters"] / nps_shift["Responses"] * 100.0) - \
                           (nps_shift["Demoters"] / nps_shift["Responses"] * 100.0)
        nps_shift["Shift (Time)"] = nps_shift["Shift"].map(lambda s: f"{s} ({SHIFT_TIMES.get(s,'')})")

        fig_nps_shift = px.bar(
            nps_shift.sort_values("NPS", ascending=False),
            x="Shift (Time)", y="NPS",
            title="Shift-wise NPS (Promoters% − Demoters%)",
            text=nps_shift["NPS"].round(1)
        )
        fig_nps_shift.update_layout(xaxis_title="Shift (with time slot)", yaxis_title="NPS")
        st.plotly_chart(fig_nps_shift, use_container_width=True)

        fig_vol_shift = px.bar(
            nps_shift.sort_values("Responses", ascending=False),
            x="Shift (Time)", y="Responses",
            title="Shift-wise Responses (volume)",
            text="Responses"
        )
        fig_vol_shift.update_layout(xaxis_title="Shift (with time slot)", yaxis_title="Responses")
        st.plotly_chart(fig_vol_shift, use_container_width=True)

        # Best performing days & shifts (by NPS) with minimum volume
        MIN_RESP = 30
        if set(["DayOfWeek","Feedback Head","Ticket number"]).issubset(filtered.columns):
            dow_nps = (
                filtered.groupby("DayOfWeek").agg(
                    Responses=("Ticket number","count"),
                    Promoters=("Feedback Head", lambda s: (s=="Promoter").sum()),
                    Demoters=("Feedback Head", lambda s: (s=="Demoter").sum())
                ).reset_index()
            )
            dow_nps["NPS"] = (dow_nps["Promoters"]/dow_nps["Responses"]*100.0) - (dow_nps["Demoters"]/dow_nps["Responses"]*100.0)
            best_days = dow_nps.query("Responses >= @MIN_RESP").sort_values("NPS", ascending=False)
            st.subheader("Best Performing Days (by NPS)")
            st.dataframe(best_days, use_container_width=True)

        best_shifts = nps_shift.query("Responses >= @MIN_RESP").sort_values("NPS", ascending=False).copy()
        best_shifts["Shift (Time)"] = best_shifts["Shift"].map(lambda s: f"{s} ({SHIFT_TIMES.get(s,'')})")
        st.subheader("Best Performing Shifts (by NPS)")
        st.dataframe(best_shifts[["Shift (Time)","Responses","NPS","Promoters","Demoters"]], use_container_width=True)

# ======================================================
# 3) LIFECYCLE & SLA
# ======================================================
with tabs[2]:
    st.title("Lifecycle & SLA")

    created_cnt = safe_count(filtered)
    first_reply_cnt = int(filtered["First Public Reply At"].notna().sum()) if "First Public Reply At" in filtered.columns else 0
    closed_cnt = int(filtered["Closed At"].notna().sum()) if "Closed At" in filtered.columns else 0
    reopened_cnt = int(filtered.get("Re-Opened", pd.Series([False]*len(filtered))).fillna(False).sum()) if "Re-Opened" in filtered.columns else 0

    funnel_df = pd.DataFrame({
        "Stage": ["Created", "First Public Reply", "Closed", "Re-Opened"],
        "Count": [created_cnt, first_reply_cnt, closed_cnt, reopened_cnt]
    })
    fig_fun = px.funnel(funnel_df, x="Count", y="Stage", title="Ticket Funnel")
    st.plotly_chart(fig_fun, use_container_width=True)

    if "FRT_min" in filtered.columns and filtered["FRT_min"].notna().sum() > 0:
        fig_frt = px.histogram(filtered, x="FRT_min", nbins=50, title="First Response Time (minutes) – Distribution")
        fig_frt.add_vline(x=np.nanmedian(filtered["FRT_min"]), line_dash="dash", annotation_text="Median", annotation_position="top")
        st.plotly_chart(fig_frt, use_container_width=True)

    if "TTR_min" in filtered.columns and filtered["TTR_min"].notna().sum() > 0:
        fig_ttr = px.histogram(filtered, x="TTR_min", nbins=50, title="Resolution Time (minutes) – Distribution")
        fig_ttr.add_vline(x=np.nanmedian(filtered["TTR_min"]), line_dash="dash", annotation_text="Median", annotation_position="top")
        st.plotly_chart(fig_ttr, use_container_width=True)

    if "SLA Breach" in filtered.columns:
        if "Branch Name" in filtered.columns:
            sla_branch = filtered.pivot_table(index="Branch Name", values="SLA Breach",
                                              aggfunc=lambda s: np.mean(s.fillna(False).astype(float))*100)
            fig_sla_b = px.imshow(sla_branch.sort_values("SLA Breach"), color_continuous_scale="Reds",
                                  title="SLA Breach % by Branch", text_auto=".1f")
            st.plotly_chart(fig_sla_b, use_container_width=True)

        if "Shift" in filtered.columns:
            sla_shift = filtered.pivot_table(index="Shift", values="SLA Breach",
                                             aggfunc=lambda s: np.mean(s.fillna(False).astype(float))*100)
            sla_shift = sla_shift.reindex(["Breakfast","Lunch","Dinner","Late Night"]).fillna(0)
            fig_sla_s = px.imshow(sla_shift, color_continuous_scale="Reds", title="SLA Breach % by Shift", text_auto=".1f")
            st.plotly_chart(fig_sla_s, use_container_width=True)

    if set(["Re-Opened","Branch Name"]).issubset(filtered.columns):
        reopen_by_branch = filtered.groupby("Branch Name")["Re-Opened"].apply(lambda s: np.mean(s.fillna(False))*100).reset_index(name="Reopen %")
        fig_ro = px.bar(reopen_by_branch.sort_values("Reopen %", ascending=False), x="Branch Name", y="Reopen %",
                        title="Reopen % by Branch")
        st.plotly_chart(fig_ro, use_container_width=True)

# ======================================================
# 4) THEMES & TEXT
# ======================================================
with tabs[3]:
    st.title("Themes & Text Analysis")

    # Tag × Branch heatmap
    if set(["Tags","Branch Name","Ticket number"]).issubset(filtered.columns):
        topN = 20
        top_tag_values = filtered["Tags"].value_counts().head(topN).index
        tag_branch = filtered[filtered["Tags"].isin(top_tag_values)]
        if not tag_branch.empty:
            tb = tag_branch.pivot_table(index="Tags", columns="Branch Name", values="Ticket number",
                                        aggfunc="count", fill_value=0)
            fig_tb = px.imshow(tb, color_continuous_scale="YlGnBu", title=f"Tag × Branch Heatmap (Top {topN} Tags)")
            st.plotly_chart(fig_tb, use_container_width=True)

    # Tag × Shift heatmap
    if set(["Tags","Shift","Ticket number"]).issubset(filtered.columns):
        tag_shift = filtered[filtered["Tags"].isin(top_tag_values)]
        if not tag_shift.empty:
            ts = tag_shift.pivot_table(index="Tags", columns="Shift", values="Ticket number",
                                       aggfunc="count", fill_value=0)
            ts = ts.reindex(columns=["Breakfast","Lunch","Dinner","Late Night"])
            fig_ts = px.imshow(ts, color_continuous_scale="YlOrRd", title=f"Tag × Shift Heatmap (Top {topN} Tags)")
            st.plotly_chart(fig_ts, use_container_width=True)

    # Sentiment by Tag
    if set(["Tags","Feedback Head"]).issubset(filtered.columns):
        sent_tag = filtered[filtered["Tags"].isin(top_tag_values)].groupby(["Tags","Feedback Head"]).size().reset_index(name="Count")
        if not sent_tag.empty:
            fig_st = px.bar(sent_tag, y="Tags", x="Count", color="Feedback Head", orientation="h", barmode="relative",
                            title="Sentiment Breakdown by Tag", text="Count")
            st.plotly_chart(fig_st, use_container_width=True)

    # Text mining
    st.subheader("Text Mining on Descriptions")
    text = " ".join(filtered["Description"].dropna().astype(str)) if "Description" in filtered.columns else ""
    if text.strip():
        wc = WordCloud(width=900, height=400, background_color="white").generate(text)
        fig_wc, ax = plt.subplots(figsize=(10, 4))
        ax.imshow(wc, interpolation="bilinear")
        ax.axis("off")
        st.pyplot(fig_wc)

        def clean_tokens(s):
            return re.findall(r"\b[a-zA-Z]{3,}\b", s.lower())
        tokens = clean_tokens(text)
        bigrams = [" ".join(p) for p in zip(tokens, tokens[1:])]
        trigrams = [" ".join((tokens[i], tokens[i+1], tokens[i+2])) for i in range(len(tokens)-2)]

        bi_df = pd.DataFrame(Counter(bigrams).most_common(15), columns=["bigram","count"])
        tri_df = pd.DataFrame(Counter(trigrams).most_common(15), columns=["trigram","count"])

        c1, c2 = st.columns(2)
        with c1:
            if not bi_df.empty:
                st.markdown("Top Bigrams")
                st.dataframe(bi_df, use_container_width=True)
        with c2:
            if not tri_df.empty:
                st.markdown("Top Trigrams")
                st.dataframe(tri_df, use_container_width=True)

        common_terms = [w for w, _ in Counter(tokens).most_common(30)]
        insights = []
        if any(w in common_terms for w in ["cold","soggy","undercooked"]): insights.append("Frequent temperature/undercooked issues — review hot-hold and pass-through checks.")
        if any(w in common_terms for w in ["delay","late","slow","time"]): insights.append("Strong delay signal — rebalance rider allocation and prep station throughput.")
        if any(w in common_terms for w in ["wrong","missing","item","order","addons","sauce"]): insights.append("Order accuracy/packing concerns — add pack checklist and QC at peak.")
        if any(w in common_terms for w in ["service","respond","answer","call"]): insights.append("Customer service/response gaps — tighten first-reply SOPs and scripts.")
        if any(w in common_terms for w in ["fries","burger","drink"]): insights.append("Product-specific feedback (fries/burger/drinks) — focus recipe adherence and batch timing.")
        if not insights:
            insights.append("No dominant repeating theme detected — responses are dispersed.")
        st.markdown("Key Insights from Responses")
        for p in insights:
            st.markdown(f"- {p}")

    # SPARK visuals
    if "SPARK" in filtered.columns:
        st.subheader("SPARK Breakdown (Top Drivers)")
        spark_counts = filtered["SPARK"].value_counts().reset_index()
        spark_counts.columns = ["SPARK", "Responses"]
        fig_spark = px.bar(spark_counts, x="Responses", y="SPARK", orientation="h", text="Responses",
                           title="SPARK Categories by Volume")
        st.plotly_chart(fig_spark, use_container_width=True)

        if "Shift" in filtered.columns:
            spark_shift = filtered.groupby(["SPARK","Shift"]).size().reset_index(name="Responses")
            spark_shift["Shift (Time)"] = spark_shift["Shift"].map(lambda s: f"{s} ({SHIFT_TIMES.get(s,'')})")
            fig_spark_shift = px.bar(
                spark_shift, x="Responses", y="SPARK", color="Shift (Time)",
                orientation="h", barmode="group", title="SPARK by Shift"
            )
            st.plotly_chart(fig_spark_shift, use_container_width=True)

# ======================================================
# 5) BRANCH & AGENT
# ======================================================
with tabs[4]:
    st.title("Branch & Agent Analytics")

    frames = []
    if "Branch Name" in filtered.columns and "Feedback Head" in filtered.columns:
        demoter_rate = filtered.groupby("Branch Name")["Feedback Head"].apply(lambda s: (s == "Demoter").mean()*100).reset_index(name="Demoter %")
        frames.append(demoter_rate)
    if "SLA Breach" in filtered.columns and "Branch Name" in filtered.columns:
        sla_rate = filtered.groupby("Branch Name")["SLA Breach"].apply(lambda s: np.mean(s.fillna(False))*100).reset_index(name="SLA Breach %")
        frames.append(sla_rate)
    if "Re-Opened" in filtered.columns and "Branch Name" in filtered.columns:
        reopen_rate = filtered.groupby("Branch Name")["Re-Opened"].apply(lambda s: np.mean(s.fillna(False))*100).reset_index(name="Re-Opened %")
        frames.append(reopen_rate)
    if "Branch Name" in filtered.columns and "FRT_min" in filtered.columns:
        frt_median = filtered.groupby("Branch Name")["FRT_min"].median().reset_index(name="Median FRT (min)")
        frames.append(frt_median)
    if "Branch Name" in filtered.columns and "TTR_min" in filtered.columns:
        ttr_median = filtered.groupby("Branch Name")["TTR_min"].median().reset_index(name="Median TTR (min)")
        frames.append(ttr_median)

    if frames:
        branch_kpis = frames[0]
        for f in frames[1:]:
            branch_kpis = branch_kpis.merge(f, how="outer", on="Branch Name")
        branch_kpis = branch_kpis.fillna(0).rename(columns={"Branch Name": "Branch"})
        st.dataframe(branch_kpis.sort_values("Demoter %", ascending=False), use_container_width=True)
    else:
        st.info("Not enough columns to compute branch KPIs.")

    if set(["Team Member","Ticket number"]).issubset(filtered.columns):
        agent_agg = filtered.groupby("Team Member").agg(
            Responses=("Ticket number","count"),
            Median_TTR=("TTR_min","median"),
            DemoterRate=("Feedback Head", lambda s: (s=="Demoter").mean()*100) if "Feedback Head" in filtered.columns else ("Ticket number","count")
        ).reset_index()
        agent_agg = agent_agg[agent_agg["Responses"] > 0]
        fig_agent = px.scatter(agent_agg, x="Responses", y="Median_TTR", color="DemoterRate",
                               hover_data=["Team Member"], title="Agent Throughput vs Resolution Time (color=Demoter %)")
        st.plotly_chart(fig_agent, use_container_width=True)

    if set(["Team Member","SLA Breach","Ticket number"]).issubset(filtered.columns):
        workload = filtered.groupby("Team Member").agg(
            Responses=("Ticket number","count"),
            SLABreachRate=("SLA Breach", lambda s: np.mean(s.fillna(False))*100)
        ).reset_index()
        fig_wl = px.scatter(workload, x="Responses", y="SLABreachRate", size="Responses", hover_data=["Team Member"],
                            title="Workload vs SLA Breach Rate (by Agent)")
        st.plotly_chart(fig_wl, use_container_width=True)

# ======================================================
# 6) RISK & STABILITY
# ======================================================
with tabs[5]:
    st.title("Risk & Stability (SPC & Outliers)")

    if set(["Date","Ticket number"]).issubset(filtered.columns):
        daily = filtered.groupby("Date").agg(
            Responses=("Ticket number","count"),
            Demoters=("Feedback Head", lambda s: (s=="Demoter").sum()) if "Feedback Head" in filtered.columns else ("Ticket number","count")
        ).reset_index()
        if not daily.empty and "Feedback Head" in filtered.columns:
            daily["Demoter %"] = daily["Demoters"]/daily["Responses"]*100
            mu = daily["Demoter %"].mean()
            sigma = daily["Demoter %"].std(ddof=1)
            ucl, lcl = mu + 3*sigma, max(mu - 3*sigma, 0)

            fig_cc = go.Figure()
            fig_cc.add_trace(go.Scatter(x=daily["Date"], y=daily["Demoter %"], mode="lines+markers", name="Demoter %"))
            fig_cc.add_hline(y=mu, line_dash="dash", annotation_text="Mean")
            fig_cc.add_hline(y=ucl, line_dash="dot", line_color="red", annotation_text="UCL (+3σ)")
            fig_cc.add_hline(y=lcl, line_dash="dot", line_color="red", annotation_text="LCL (-3σ)")
            fig_cc.update_layout(title="Control Chart – Daily Demoter %")
            st.plotly_chart(fig_cc, use_container_width=True)

    out = filtered.copy()
    out["Outlier"] = False
    if "TTR_min" in out.columns and out["TTR_min"].notna().sum() > 0:
        cut_ttr = np.nanpercentile(out["TTR_min"], 99)
        out.loc[out["TTR_min"] >= cut_ttr, "Outlier"] = True
    if "FRT_min" in out.columns and out["FRT_min"].notna().sum() > 0:
        cut_frt = np.nanpercentile(out["FRT_min"], 99)
        out.loc[out["FRT_min"] >= cut_frt, "Outlier"] = True

    st.subheader("Outlier Tickets (top 1% FRT/TTR)")
    cols = [c for c in ["Ticket number","Branch Name","Tags","Shift","Feedback Head","FRT_min","TTR_min","Created At","Closed At"] if c in out.columns]
    if cols:
        st.dataframe(out[out["Outlier"]][cols].sort_values(["TTR_min","FRT_min"], ascending=False).head(50), use_container_width=True)
    else:
        st.info("Not enough columns to render outlier table.")

# ======================================================
# 7) DATA QUALITY
# ======================================================
with tabs[6]:
    st.title("Data Quality & Governance")

    key_cols = [
        "Branch Name","Feedback Head","Tags","Description","First Public Reply At",
        "Closed At","First Response SLA","Resolution SLA","SLA Breach"
    ]
    present_cols = [c for c in key_cols if c in filtered.columns]
    if present_cols:
        miss_df = pd.DataFrame({
            "Field": present_cols,
            "Missing %": [filtered[c].isna().mean()*100 for c in present_cols]
        }).sort_values("Missing %", ascending=False)
        fig_miss = px.bar(miss_df, x="Missing %", y="Field", orientation="h", title="Missingness by Field")
        st.plotly_chart(fig_miss, use_container_width=True)
    else:
        st.info("No key fields found for missingness check.")

    sanity_rows = []

    def add_rule(name, mask: pd.Series):
        try:
            violations = int(mask.fillna(False).sum())
        except Exception:
            violations = int((mask.astype(bool)).fillna(False).sum())
        sanity_rows.append({"Rule": name, "Violations": violations})

    if set(["Created At","First Public Reply At"]).issubset(filtered.columns):
        add_rule("Created At ≤ First Public Reply At", (filtered["Created At"] > filtered["First Public Reply At"]))
    if set(["First Public Reply At","Closed At"]).issubset(filtered.columns):
        add_rule("First Public Reply At ≤ Closed At", (filtered["First Public Reply At"] > filtered["Closed At"]))
    if set(["Created At","Closed At"]).issubset(filtered.columns):
        add_rule("Created At ≤ Closed At", (filtered["Created At"] > filtered["Closed At"]))

    sanity = pd.DataFrame(sanity_rows)
    if not sanity.empty:
        st.subheader("Timestamp Sanity Checks")
        st.dataframe(sanity, use_container_width=True)
    else:
        st.info("No timestamp rules evaluated (required columns missing).")

# ======================================================
# 8) CLASSIFICATION AUDIT
# ======================================================
with tabs[7]:
    st.title("Exclusions Audit — Demoter with 'Not Responding'")

    if "audit_payload" not in locals():
        st.info("No audit payload available.")
    else:
        pre = audit_payload["pre"]
        post = audit_payload["post"]
        mask = audit_payload["mask"]
        excl_count = audit_payload["count"]

        c1, = st.columns(1)
        c1.metric("Rows excluded (Demoter AND 'Not Responding')", excl_count)

        if excl_count > 0:
            affected = pre.loc[mask].copy()

            if "Branch Name" in affected.columns:
                st.subheader("Excluded by Branch")
                b = affected["Branch Name"].value_counts().reset_index()
                b.columns = ["Branch", "Count"]
                fig_b = px.bar(b, x="Count", y="Branch", orientation="h", text="Count",
                               title="Excluded (Demoter + 'Not Responding') by Branch")
                st.plotly_chart(fig_b, use_container_width=True)

            if "Shift" in affected.columns:
                st.subheader("Excluded by Shift")
                s = affected["Shift"].value_counts().reset_index()
                s.columns = ["Shift", "Count"]
                s["Shift (Time)"] = s["Shift"].map(lambda x: f"{x} ({SHIFT_TIMES.get(x,'')})")
                fig_s = px.bar(s, x="Count", y="Shift (Time)", orientation="h", text="Count",
                               title="Excluded (Demoter + 'Not Responding') by Shift")
                st.plotly_chart(fig_s, use_container_width=True)

            if "WeekStart" in affected.columns:
                st.subheader("Exclusions over Time (ISO Week)")
                w = affected.groupby("WeekStart").size().reset_index(name="Count").sort_values("WeekStart")
                fig_w = px.line(w, x="WeekStart", y="Count", markers=True, title="Weekly Count of Excluded Rows")
                st.plotly_chart(fig_w, use_container_width=True)

            st.subheader("Excluded Rows (details)")
            cols = [c for c in [
                "Ticket number","Created At","Branch Name","Shift","DayOfWeek",
                "Tags","Feedback Head","Description"
            ] if c in pre.columns]
            if cols:
                st.dataframe(affected[cols].sort_values("Created At").reset_index(drop=True),
                             use_container_width=True, height=380)
            else:
                st.info("No suitable columns available to display excluded rows.")
        else:
            st.success("No Demoter rows with 'Not Responding' were found.")
