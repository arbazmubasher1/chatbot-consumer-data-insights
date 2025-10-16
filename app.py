import streamlit as st
import pandas as pd
import plotly.express as px
from wordcloud import WordCloud
import matplotlib.pyplot as plt
from collections import Counter
import re

# -------------------------------------------------------
# ✅ MUST BE FIRST STREAMLIT COMMAND
# -------------------------------------------------------
st.set_page_config(page_title="Restaurant Complaints Dashboard", page_icon="🍔", layout="wide")

# -------------------------------------------------------
# DATA LOADING
# -------------------------------------------------------
@st.cache_data
def load_data():
    df = pd.read_excel("1234567.xlsx", sheet_name="tickets")
    df['Created At'] = pd.to_datetime(df['Created At'], errors='coerce')
    df['Date'] = df['Created At'].dt.date
    df['Hour'] = df['Created At'].dt.hour
    df['DayOfWeek'] = df['Created At'].dt.day_name()
    df['Week'] = df['Created At'].dt.strftime('%Y-%U')
    df['IsWeekend'] = df['DayOfWeek'].isin(['Saturday', 'Sunday'])
    return df

df = load_data()

# -------------------------------------------------------
# FILTERS
# -------------------------------------------------------
st.sidebar.header("🔍 Filters")
branches = st.sidebar.multiselect("Select Branches", options=sorted(df["Branch Name"].dropna().unique()))
feedback_types = st.sidebar.multiselect("Select Feedback Type", options=df["Feedback Head"].dropna().unique())
date_range = st.sidebar.date_input("Select Date Range", [df["Date"].min(), df["Date"].max()])

filtered_df = df.copy()
if branches:
    filtered_df = filtered_df[filtered_df["Branch Name"].isin(branches)]
if feedback_types:
    filtered_df = filtered_df[filtered_df["Feedback Head"].isin(feedback_types)]
filtered_df = filtered_df[(filtered_df["Date"] >= date_range[0]) & (filtered_df["Date"] <= date_range[-1])]

# -------------------------------------------------------
# HELPER FUNCTIONS
# -------------------------------------------------------
def shift_label(hour):
    if 7 <= hour < 12: return 'Breakfast'
    elif 12 <= hour < 17: return 'Lunch'
    elif 17 <= hour < 23: return 'Dinner'
    else: return 'Late Night'

filtered_df['Shift'] = filtered_df['Hour'].apply(shift_label)

# -------------------------------------------------------
# TABS
# -------------------------------------------------------
tab1, tab2, tab3, tab4 = st.tabs(["📊 Overview", "📅 Time Trends", "⏰ Shift Insights", "💬 Complaint Themes"])

# -------------------------------------------------------
# TAB 1: OVERVIEW
# -------------------------------------------------------
with tab1:
    st.title("📊 Restaurant Complaints & Feedback Overview")

    total_complaints = len(filtered_df)
    unique_customers = filtered_df["Customer CLI"].nunique()
    demoters = (filtered_df["Feedback Head"] == "Demoter").sum()
    promoters = (filtered_df["Feedback Head"] == "Promoter").sum()

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Complaints", total_complaints)
    c2.metric("Demoter %", f"{(demoters/total_complaints*100):.1f}%" if total_complaints else "0%")
    c3.metric("Promoter %", f"{(promoters/total_complaints*100):.1f}%" if total_complaints else "0%")
    c4.metric("Unique Customers", unique_customers)

    st.subheader("📍 Complaints by Branch")
    branch_counts = filtered_df["Branch Name"].value_counts().reset_index()
    branch_counts.columns = ["Branch", "Count"]
    fig1 = px.bar(branch_counts, x="Count", y="Branch", orientation="h", color="Branch", text="Count")
    st.plotly_chart(fig1, use_container_width=True)

    st.subheader("🍔 Top Complaint Categories (Tags)")
    if "Tags" in filtered_df.columns:
        tag_counts = filtered_df["Tags"].value_counts().head(10).reset_index()
        tag_counts.columns = ["Tag", "Count"]
        fig2 = px.bar(tag_counts, x="Count", y="Tag", orientation="h", color="Tag", text="Count")
        st.plotly_chart(fig2, use_container_width=True)

    st.subheader("😊 Feedback Sentiment by Branch")
    sentiment = filtered_df.groupby(["Branch Name", "Feedback Head"]).size().reset_index(name="Count")
    fig3 = px.bar(sentiment, x="Branch Name", y="Count", color="Feedback Head", barmode="stack")
    st.plotly_chart(fig3, use_container_width=True)

# -------------------------------------------------------
# TAB 2: TIME TRENDS
# -------------------------------------------------------
with tab2:
    st.title("📅 Time-Based Complaint Analysis")

    # Weekday vs Weekend
    weekend_summary = filtered_df.groupby("IsWeekend")["Ticket number"].count().reset_index()
    fig4 = px.bar(weekend_summary, x="IsWeekend", y="Ticket number", color="IsWeekend",
                  text="Ticket number", title="📆 Weekday vs Weekend Complaints")
    st.plotly_chart(fig4, use_container_width=True)

    # Day of Week
    dow = filtered_df.groupby("DayOfWeek").size().reindex(
        ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']
    ).reset_index(name="Count")
    fig5 = px.line(dow, x="DayOfWeek", y="Count", markers=True, title="📈 Complaints by Day of Week")
    st.plotly_chart(fig5, use_container_width=True)

    # Weekly Trend
    weekly = filtered_df.groupby("Week").size().reset_index(name="Count")
    fig6 = px.line(weekly, x="Week", y="Count", markers=True, title="🗓 Weekly Complaint Trend")
    st.plotly_chart(fig6, use_container_width=True)

# -------------------------------------------------------
# TAB 3: SHIFT INSIGHTS
# -------------------------------------------------------
with tab3:
    st.title("⏰ Shift-Wise Insights")

    shift_summary = filtered_df.groupby("Shift")["Ticket number"].count().reset_index()
    fig7 = px.pie(shift_summary, names="Shift", values="Ticket number", title="⏰ Complaints by Shift")
    st.plotly_chart(fig7, use_container_width=True)

    sentiment_shift = filtered_df.groupby(["Shift", "Feedback Head"]).size().reset_index(name="Count")
    fig8 = px.bar(sentiment_shift, x="Shift", y="Count", color="Feedback Head", barmode="group",
                  title="😊 Feedback Sentiment by Shift")
    st.plotly_chart(fig8, use_container_width=True)

# -------------------------------------------------------
# TAB 4: COMPLAINT THEMES
# -------------------------------------------------------
with tab4:
    st.title("💬 Complaint Themes & Insights")

    text = " ".join(str(desc) for desc in filtered_df["Description"].dropna())
    if text.strip():
        wc = WordCloud(width=800, height=400, background_color="white").generate(text)
        fig, ax = plt.subplots(figsize=(10, 5))
        ax.imshow(wc, interpolation="bilinear")
        ax.axis("off")
        st.pyplot(fig)

        # Keyword analysis for bullet insights
        words = re.findall(r'\b[a-zA-Z]{4,}\b', text.lower())
        common_words = Counter(words).most_common(20)
        common_terms = [w for w, c in common_words]
        insights = []
        if any(word in common_terms for word in ["cold", "food", "soggy", "undercooked"]):
            insights.append("Frequent mentions of *cold* or *undercooked* food — kitchen temperature control issues.")
        if any(word in common_terms for word in ["delay", "late", "slow", "time"]):
            insights.append("Complaints about *delay* or *late service* — review order prep and dispatch timing.")
        if any(word in common_terms for word in ["wrong", "missing", "order", "item"]):
            insights.append("Multiple mentions of *wrong* or *missing orders* — check packing and handoff accuracy.")
        if any(word in common_terms for word in ["service", "respond", "not", "answer"]):
            insights.append("Keywords like *service* and *respond* appear often — improve response time to customers.")
        if any(word in common_terms for word in ["fries", "burger", "sauce", "drink"]):
            insights.append("Product-level feedback spotted — e.g., *fries*, *burger*, *sauce*, *drink* quality concerns.")
        if not insights:
            insights.append("No strong recurring themes detected — complaints are dispersed.")

        st.markdown("### 🧠 Key Insights from Complaints")
        for point in insights:
            st.markdown(f"- {point}")

    else:
        st.info("No complaint descriptions available for word cloud.")

st.caption("© 2025 Johnny & Jugnu | Built by Arbaz Mubasher")
