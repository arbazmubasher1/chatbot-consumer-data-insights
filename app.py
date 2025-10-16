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
# -------------------------------------------------------

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from wordcloud import WordCloud
import matplotlib.pyplot as plt
from collections import Counter
import re
from datetime import timedelta

# =========================
# Page Config (MUST be first)
# =========================
st.set_page_config(page_title="Restaurant Responses Dashboard", page_icon="🍔", layout="wide")

# =========================
# Load & Prepare Data
# =========================
@st.cache_data
def load_data(path: str):
    df = pd.read_excel(path, sheet_name="tickets")

    # Parse timestamps safely
    for col in [
        "Created At", "Updated At", "First Public Reply At", "First Private Reply At",
        "Last Public Reply At", "Last Private Reply At", "Opened At", "Closed At",
        "Re-Opened At", "First Response Till", "Due Date"
    ]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # Derived time columns
    df["Date"] = pd.to_datetime(df["Created At"], errors="coerce").dt.date
    df["Hour"] = pd.to_datetime(df["Created At"], errors="coerce").dt.hour
    df["DayOfWeek"] = pd.to_datetime(df["Created At"], errors="coerce").dt.day_name()
    df["Week"] = pd.to_datetime(df["Created At"], errors="coerce").dt.strftime("%Y-%U")
    df["IsWeekend"] = df["DayOfWeek"].isin(["Saturday", "Sunday"])

    # Shifts
    def shift_label(h):
        if pd.isna(h): return np.nan
        h = int(h)
        if 7 <= h < 12: return "Breakfast"
        elif 12 <= h < 17: return "Lunch"
        elif 17 <= h < 23: return "Dinner"
        else: return "Late Night"
    df["Shift"] = df["Hour"].apply(shift_label)

    # Lifecycle durations
    if "First Public Reply At" in df.columns and "Created At" in df.columns:
        df["FRT_min"] = (df["First Public Reply At"] - df["Created At"]).dt.total_seconds() / 60
    else:
        df["FRT_min"] = np.nan

    if "Closed At" in df.columns and "Created At" in df.columns:
        df["TTR_min"] = (df["Closed At"] - df["Created At"]).dt.total_seconds() / 60
    else:
        df["TTR_min"] = np.nan

    # Clean key categoricals
    for c in ["Branch Name", "Feedback Head", "Tags", "Team Member", "Pipeline Stage", "Status"]:
        if c in df.columns:
            df[c] = df[c].fillna("Unspecified")

    # Booleans normalization (SLA & flags)
    for b in ["First Response SLA", "Resolution SLA", "SLA Breach", "Re-Opened", "Opened"]:
        if b in df.columns:
            df[b] = df[b].astype(str).str.lower().map({"true": True, "false": False})
    return df

# 👇 change path if needed (local or mounted file)
DATA_PATH = "cx9_tickets_1760606268482.xlsx"
df = load_data(DATA_PATH)

# =========================
# Sidebar Filters
# =========================
st.sidebar.header("🔍 Filters")

branch_options = sorted(df["Branch Name"].dropna().unique())
feedback_options = sorted(df["Feedback Head"].dropna().unique())
shift_options = [s for s in ["Breakfast", "Lunch", "Dinner", "Late Night"] if s in df["Shift"].unique()]

sel_branches = st.sidebar.multiselect("Branches", options=branch_options)
sel_feedback = st.sidebar.multiselect("Feedback Type", options=feedback_options)
sel_shifts = st.sidebar.multiselect("Shifts", options=shift_options)
date_min, date_max = df["Date"].min(), df["Date"].max()
sel_dates = st.sidebar.date_input("Date range", [date_min, date_max])

filtered = df.copy()
if sel_branches:
    filtered = filtered[filtered["Branch Name"].isin(sel_branches)]
if sel_feedback:
    filtered = filtered[filtered["Feedback Head"].isin(sel_feedback)]
if sel_shifts:
    filtered = filtered[filtered["Shift"].isin(sel_shifts)]
if len(sel_dates) == 2:
    filtered = filtered[(filtered["Date"] >= sel_dates[0]) & (filtered["Date"] <= sel_dates[1])]

# Helper counts
def safe_count(series):
    return int(series.shape[0])

def pct(n, d):
    return (100.0 * n / d) if d else 0.0

# =========================
# Tabs
# =========================
tabs = st.tabs([
    "📊 Overview",
    "🗓️ Time Intelligence",
    "⏱️ Lifecycle & SLA",
    "🍔 Themes & Text",
    "🏪 Branch & 👤 Agent",
    "⚠️ Risk & Stability",
    "🧼 Data Quality"
])

# ======================================================
# 1) OVERVIEW
# ======================================================
with tabs[0]:
    st.title("📊 Responses & Feedback Overview")

    total_responses = safe_count(filtered)
    unique_customers = filtered.get("Customer CLI", pd.Series([])).nunique()
    demoters = int((filtered["Feedback Head"] == "Demoter").sum())
    promoters = int((filtered["Feedback Head"] == "Promoter").sum())
    neutrals = int((filtered["Feedback Head"] == "Neutral").sum())

    sla_breach = int(filtered.get("SLA Breach", pd.Series([False]*len(filtered))).fillna(False).sum())
    reopened = int(filtered.get("Re-Opened", pd.Series([False]*len(filtered))).fillna(False).sum())

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Total Responses", total_responses)
    c2.metric("Promoter %", f"{pct(promoters, total_responses):.1f}%")
    c3.metric("Demoter %", f"{pct(demoters, total_responses):.1f}%")
    c4.metric("Neutral %", f"{pct(neutrals, total_responses):.1f}%")
    c5.metric("SLA Breach %", f"{pct(sla_breach, total_responses):.1f}%")
    c6.metric("Reopen %", f"{pct(reopened, total_responses):.1f}%")

    st.subheader("📍 Responses by Branch")
    branch_counts = filtered["Branch Name"].value_counts().reset_index()
    branch_counts.columns = ["Branch", "Responses"]
    fig = px.bar(branch_counts, x="Responses", y="Branch", orientation="h", text="Responses",
                 color="Branch", title="Responses by Branch")
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("🍔 Top Response Categories (Tags)")
    if "Tags" in filtered.columns:
        top_tags = filtered["Tags"].value_counts().head(15).reset_index()
        top_tags.columns = ["Tag", "Responses"]
        fig2 = px.bar(top_tags, x="Responses", y="Tag", orientation="h", text="Responses",
                      color="Tag", title="Top Tags (by Response Volume)")
        st.plotly_chart(fig2, use_container_width=True)

# ======================================================
# 2) TIME INTELLIGENCE
# ======================================================
with tabs[1]:
    st.title("🗓️ Time Intelligence")

    # Weekday vs Weekend (with Demoter % line)
    filtered["DayType"] = filtered["IsWeekend"].apply(lambda x: "Weekend" if x else "Weekday")
    daytype = filtered.groupby("DayType").agg(
        Responses=("Ticket number", "count"),
        Demoters=("Feedback Head", lambda s: (s == "Demoter").sum())
    ).reset_index()
    daytype["Demoter %"] = daytype["Demoters"] / daytype["Responses"] * 100.0

    bar = go.Bar(x=daytype["DayType"], y=daytype["Responses"], name="Responses", marker_color="#219EBC")
    line = go.Scatter(x=daytype["DayType"], y=daytype["Demoter %"], name="Demoter %", yaxis="y2", mode="lines+markers")
    fig_combo = go.Figure(data=[bar, line])
    fig_combo.update_layout(
        title="📆 Weekday vs Weekend Responses (w/ Demoter %)",
        yaxis=dict(title="Responses"),
        yaxis2=dict(title="Demoter %", overlaying="y", side="right"),
        legend=dict(orientation="h")
    )
    st.plotly_chart(fig_combo, use_container_width=True)

    # Day of Week profile
    dow = filtered.groupby("DayOfWeek").size().reindex(
        ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
    ).reset_index(name="Responses")
    fig_dow = px.line(dow, x="DayOfWeek", y="Responses", markers=True, title="📈 Responses by Day of Week")
    st.plotly_chart(fig_dow, use_container_width=True)

    # Intraday heatmap (Hour x DayOfWeek)
    intraday = filtered.dropna(subset=["Hour"]).copy()
    intraday_pivot = intraday.pivot_table(index="Hour", columns="DayOfWeek", values="Ticket number", aggfunc="count", fill_value=0)
    intraday_pivot = intraday_pivot[["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]]
    fig_heat = px.imshow(intraday_pivot, color_continuous_scale="YlOrRd", title="🔥 Intraday Responses Heatmap (Hour × Day)")
    st.plotly_chart(fig_heat, use_container_width=True)

    # Weekly trend
    weekly = filtered.groupby("Week").size().reset_index(name="Responses")
    fig_weekly = px.line(weekly, x="Week", y="Responses", markers=True, title="🗓 Weekly Response Trend")
    st.plotly_chart(fig_weekly, use_container_width=True)

# ======================================================
# 3) LIFECYCLE & SLA
# ======================================================
with tabs[2]:
    st.title("⏱️ Lifecycle & SLA")

    # Funnel approximation
    funnel_df = pd.DataFrame({
        "Stage": ["Created", "First Public Reply", "Closed", "Re-Opened"],
        "Count": [
            safe_count(filtered),
            int(filtered["First Public Reply At"].notna().sum()),
            int(filtered["Closed At"].notna().sum()),
            int(filtered.get("Re-Opened", pd.Series([False]*len(filtered))).fillna(False).sum())
        ]
    })
    fig_fun = px.funnel(funnel_df, x="Count", y="Stage", title="🎯 Ticket Funnel")
    st.plotly_chart(fig_fun, use_container_width=True)

    # FRT distribution
    if filtered["FRT_min"].notna().sum() > 0:
        fig_frt = px.histogram(filtered, x="FRT_min", nbins=50, title="⏱ First Response Time (minutes) – Distribution")
        fig_frt.add_vline(x=np.nanmedian(filtered["FRT_min"]), line_dash="dash", annotation_text="Median", annotation_position="top")
        st.plotly_chart(fig_frt, use_container_width=True)

    # TTR distribution
    if filtered["TTR_min"].notna().sum() > 0:
        fig_ttr = px.histogram(filtered, x="TTR_min", nbins=50, title="🛠️ Resolution Time (minutes) – Distribution")
        fig_ttr.add_vline(x=np.nanmedian(filtered["TTR_min"]), line_dash="dash", annotation_text="Median", annotation_position="top")
        st.plotly_chart(fig_ttr, use_container_width=True)

    # SLA heatmaps (by Branch and by Shift)
    if "SLA Breach" in filtered.columns:
        sla_branch = filtered.pivot_table(index="Branch Name", values="SLA Breach", aggfunc=lambda s: np.mean(s.astype(float))*100).sort_values("SLA Breach")
        fig_sla_b = px.imshow(sla_branch, color_continuous_scale="Reds", title="🚨 SLA Breach % by Branch", text_auto=".1f")
        st.plotly_chart(fig_sla_b, use_container_width=True)

        sla_shift = filtered.pivot_table(index="Shift", values="SLA Breach", aggfunc=lambda s: np.mean(s.astype(float))*100).fillna(0)
        fig_sla_s = px.imshow(sla_shift, color_continuous_scale="Reds", title="🚨 SLA Breach % by Shift", text_auto=".1f")
        st.plotly_chart(fig_sla_s, use_container_width=True)

    # Reopen analysis
    if "Re-Opened" in filtered.columns:
        reopen_by_branch = filtered.groupby("Branch Name")["Re-Opened"].apply(lambda s: np.mean(s.fillna(False))*100).reset_index(name="Reopen %")
        fig_ro = px.bar(reopen_by_branch.sort_values("Reopen %", ascending=False), x="Branch Name", y="Reopen %", title="♻️ Reopen % by Branch")
        st.plotly_chart(fig_ro, use_container_width=True)

# ======================================================
# 4) THEMES & TEXT
# ======================================================
with tabs[3]:
    st.title("🍔 Themes & Text Analysis")

    # Tag × Branch heatmap
    topN = 20
    top_tag_values = filtered["Tags"].value_counts().head(topN).index
    tag_branch = filtered[filtered["Tags"].isin(top_tag_values)]
    if not tag_branch.empty:
        tb = tag_branch.pivot_table(index="Tags", columns="Branch Name", values="Ticket number", aggfunc="count", fill_value=0)
        fig_tb = px.imshow(tb, color_continuous_scale="YlGnBu", title=f"Tag × Branch Heatmap (Top {topN} Tags)")
        st.plotly_chart(fig_tb, use_container_width=True)

    # Tag × Shift heatmap
    tag_shift = filtered[filtered["Tags"].isin(top_tag_values)]
    if not tag_shift.empty:
        ts = tag_shift.pivot_table(index="Tags", columns="Shift", values="Ticket number", aggfunc="count", fill_value=0)
        fig_ts = px.imshow(ts, color_continuous_scale="YlOrRd", title=f"Tag × Shift Heatmap (Top {topN} Tags)")
        st.plotly_chart(fig_ts, use_container_width=True)

    # Sentiment by Tag (100% stacked)
    sent_tag = filtered[filtered["Tags"].isin(top_tag_values)].groupby(["Tags","Feedback Head"]).size().reset_index(name="Count")
    if not sent_tag.empty:
        fig_st = px.bar(sent_tag, y="Tags", x="Count", color="Feedback Head", orientation="h", barmode="relative",
                        title="Sentiment Breakdown by Tag", text="Count")
        st.plotly_chart(fig_st, use_container_width=True)

    # N-gram bars + WordCloud + Bullet Insights
    st.subheader("📝 Text Mining on Descriptions")
    text_series = filtered["Description"].dropna().astype(str)
    text = " ".join(text_series) if not text_series.empty else ""

    if text.strip():
        # WordCloud
        wc = WordCloud(width=900, height=400, background_color="white").generate(text)
        fig_wc, ax = plt.subplots(figsize=(10, 4))
        ax.imshow(wc, interpolation="bilinear")
        ax.axis("off")
        st.pyplot(fig_wc)

        # n-grams (bi & tri)
        def clean_tokens(s):
            return re.findall(r"\b[a-zA-Z]{3,}\b", s.lower())
        tokens = clean_tokens(text)
        # bigrams
        bigrams = [" ".join(p) for p in zip(tokens, tokens[1:])]
        # trigrams
        trigrams = [" ".join((tokens[i], tokens[i+1], tokens[i+2])) for i in range(len(tokens)-2)]

        bi_counts = Counter(bigrams)
        tri_counts = Counter(trigrams)

        bi_df = pd.DataFrame(bi_counts.most_common(15), columns=["bigram","count"])
        tri_df = pd.DataFrame(tri_counts.most_common(15), columns=["trigram","count"])

        c1, c2 = st.columns(2)
        with c1:
            if not bi_df.empty:
                st.markdown("**Top Bigrams**")
                st.dataframe(bi_df)
        with c2:
            if not tri_df.empty:
                st.markdown("**Top Trigrams**")
                st.dataframe(tri_df)

        # Bullet insights (theme heuristics)
        common_words = Counter(tokens).most_common(30)
        common_terms = [w for w, _ in common_words]
        insights = []
        if any(w in common_terms for w in ["cold","soggy","undercooked"]): insights.append("Frequent *temperature/undercooked* issues — review hot-hold & pass-through checks.")
        if any(w in common_terms for w in ["delay","late","slow","time"]): insights.append("Strong *delay* signal — rebalance rider allocation & prep station throughput.")
        if any(w in common_terms for w in ["wrong","missing","item","order","addons","sauce"]): insights.append("Order *accuracy/packing* concerns — add pack checklist & QC at peak.")
        if any(w in common_terms for w in ["service","respond","answer","call"]): insights.append("Customer *service/response* gaps — tighten first-reply SOPs & scripts.")
        if any(w in common_terms for w in ["fries","burger","drink"]): insights.append("Product-specific feedback (fries/burger/drinks) — focus recipe adherence & batch timing.")
        if not insights:
            insights.append("No dominant repeating theme detected — responses are dispersed.")

        st.markdown("### 🧠 Key Insights from Responses")
        for p in insights:
            st.markdown(f"- {p}")
    else:
        st.info("No response descriptions found for text analysis.")

# ======================================================
# 5) BRANCH & AGENT
# ======================================================
with tabs[4]:
    st.title("🏪 Branch & 👤 Agent Analytics")

    # Branch radar – normalize metrics
    metrics = pd.DataFrame({
        "Branch": filtered["Branch Name"].unique()
    })
    def by_branch_rate(col_true):
        s = filtered.groupby("Branch Name")[col_true].apply(lambda s: np.mean(s.fillna(False))*100)
        return s.reset_index(name=col_true+" %")

    demoter_rate = filtered.groupby("Branch Name")["Feedback Head"].apply(lambda s: (s == "Demoter").mean()*100).reset_index(name="Demoter %")
    sla_rate = by_branch_rate("SLA Breach") if "SLA Breach" in filtered.columns else pd.DataFrame(columns=["Branch Name","SLA Breach %"])
    reopen_rate = by_branch_rate("Re-Opened") if "Re-Opened" in filtered.columns else pd.DataFrame(columns=["Branch Name","Re-Opened %"])
    frt_median = filtered.groupby("Branch Name")["FRT_min"].median().reset_index(name="Median FRT (min)")
    ttr_median = filtered.groupby("Branch Name")["TTR_min"].median().reset_index(name="Median TTR (min)")

    branch_kpis = demoter_rate.merge(sla_rate, how="left", left_on="Branch Name", right_on="Branch Name") \
                              .merge(reopen_rate, how="left", on="Branch Name") \
                              .merge(frt_median, how="left", on="Branch Name") \
                              .merge(ttr_median, how="left", on="Branch Name") \
                              .rename(columns={"Branch Name": "Branch"}).fillna(0)

    st.dataframe(branch_kpis.sort_values("Demoter %", ascending=False), use_container_width=True)

    # Agent productivity vs quality
    if "Team Member" in filtered.columns:
        agent_agg = filtered.groupby("Team Member").agg(
            Responses=("Ticket number","count"),
            Median_TTR=("TTR_min","median"),
            DemoterRate=("Feedback Head", lambda s: (s=="Demoter").mean()*100)
        ).reset_index()
        agent_agg = agent_agg[agent_agg["Responses"]>0]
        fig_agent = px.scatter(agent_agg, x="Responses", y="Median_TTR", color="DemoterRate",
                               hover_data=["Team Member"], title="👤 Agent Throughput vs Resolution Time (color=Demoter %)")
        st.plotly_chart(fig_agent, use_container_width=True)

    # Workload vs SLA (bubble)
    if "SLA Breach" in filtered.columns and "Team Member" in filtered.columns:
        workload = filtered.groupby("Team Member").agg(
            Responses=("Ticket number","count"),
            SLABreachRate=("SLA Breach", lambda s: np.mean(s.fillna(False))*100)
        ).reset_index()
        fig_wl = px.scatter(workload, x="Responses", y="SLABreachRate", size="Responses", hover_data=["Team Member"],
                            title="📦 Workload vs SLA Breach Rate (by Agent)")
        st.plotly_chart(fig_wl, use_container_width=True)

# ======================================================
# 6) RISK & STABILITY
# ======================================================
with tabs[5]:
    st.title("⚠️ Risk & Stability (SPC & Outliers)")

    # Control charts for daily Demoter %
    daily = filtered.groupby("Date").agg(
        Responses=("Ticket number","count"),
        Demoters=("Feedback Head", lambda s: (s=="Demoter").sum())
    ).reset_index()
    if not daily.empty:
        daily["Demoter %"] = daily["Demoters"]/daily["Responses"]*100
        mu = daily["Demoter %"].mean()
        sigma = daily["Demoter %"].std(ddof=1)
        ucl, lcl = mu + 3*sigma, max(mu - 3*sigma, 0)

        fig_cc = go.Figure()
        fig_cc.add_trace(go.Scatter(x=daily["Date"], y=daily["Demoter %"], mode="lines+markers", name="Demoter %"))
        fig_cc.add_hline(y=mu, line_dash="dash", annotation_text="Mean")
        fig_cc.add_hline(y=ucl, line_dash="dot", line_color="red", annotation_text="UCL (+3σ)")
        fig_cc.add_hline(y=lcl, line_dash="dot", line_color="red", annotation_text="LCL (-3σ)")
        fig_cc.update_layout(title="📉 Control Chart – Daily Demoter %")
        st.plotly_chart(fig_cc, use_container_width=True)

    # Outlier explorer (top 1% TTR or FRT)
    cut_ttr = np.nanpercentile(filtered["TTR_min"], 99) if filtered["TTR_min"].notna().sum() else np.nan
    cut_frt = np.nanpercentile(filtered["FRT_min"], 99) if filtered["FRT_min"].notna().sum() else np.nan
    out = filtered.copy()
    out["Outlier"] = False
    if not np.isnan(cut_ttr):
        out.loc[out["TTR_min"] >= cut_ttr, "Outlier"] = True
    if not np.isnan(cut_frt):
        out.loc[out["FRT_min"] >= cut_frt, "Outlier"] = True
    st.subheader("🔎 Outlier Tickets (top 1% FRT/TTR)")
    cols = ["Ticket number","Branch Name","Tags","Shift","Feedback Head","FRT_min","TTR_min","Created At","Closed At"]
    cols = [c for c in cols if c in out.columns]
    st.dataframe(out[out["Outlier"]][cols].sort_values(["TTR_min","FRT_min"], ascending=False).head(50), use_container_width=True)

# ======================================================
# 7) DATA QUALITY
# ======================================================
with tabs[6]:
    st.title("🧼 Data Quality & Governance")

    # Missingness by key columns
    key_cols = [
        "Branch Name","Feedback Head","Tags","Description","First Public Reply At",
        "Closed At","First Response SLA","Resolution SLA","SLA Breach"
    ]
    present_cols = [c for c in key_cols if c in filtered.columns]
    miss_df = pd.DataFrame({
        "Field": present_cols,
        "Missing %": [filtered[c].isna().mean()*100 for c in present_cols]
    }).sort_values("Missing %", ascending=False)
    fig_miss = px.bar(miss_df, x="Missing %", y="Field", orientation="h", title="🚧 Missingness by Field")
    st.plotly_chart(fig_miss, use_container_width=True)

    # Timestamp sanity checks
    sanity = pd.DataFrame({"Rule":[],"Violations":[]})
    def add_rule(name, mask):
        nonlocal sanity
        sanity = pd.concat([sanity, pd.DataFrame({"Rule":[name], "Violations":[int(mask.sum())]})], ignore_index=True)

    if "Created At" in filtered.columns and "First Public Reply At" in filtered.columns:
        add_rule("Created At ≤ First Public Reply At", (filtered["Created At"] > filtered["First Public Reply At"]))
    if "First Public Reply At" in filtered.columns and "Closed At" in filtered.columns:
        add_rule("First Public Reply At ≤ Closed At", (filtered["First Public Reply At"] > filtered["Closed At"]))
    if "Created At" in filtered.columns and "Closed At" in filtered.columns:
        add_rule("Created At ≤ Closed At", (filtered["Created At"] > filtered["Closed At"]))

    if not sanity.empty:
        st.subheader("🧪 Timestamp Sanity Checks")
        st.dataframe(sanity, use_container_width=True)

st.caption("© 2025 Johnny & Jugnu | Built by Arbaz Mubasher — Streamlit + Plotly + pandas")
