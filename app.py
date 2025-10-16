import streamlit as st
import pandas as pd
import plotly.express as px
from wordcloud import WordCloud
import matplotlib.pyplot as plt


st.set_page_config(page_title="Restaurant Complaints Dashboard", layout="wide")

# ---- Load Data ----
@st.cache_data
def load_data():
    df = pd.read_excel("1234567.xlsx", sheet_name="tickets")
    df['Created At'] = pd.to_datetime(df['Created At'], errors='coerce')
    df['Date'] = df['Created At'].dt.date
    df['Hour'] = df['Created At'].dt.hour
    return df

df = load_data()

st.title("🍽️ Restaurant Complaints & Feedback Dashboard")

# ---- Sidebar Filters ----
st.sidebar.header("🔍 Filters")
branches = st.sidebar.multiselect("Select Branches", options=sorted(df["Branch Name"].dropna().unique()), default=None)
feedback_types = st.sidebar.multiselect("Select Feedback Type", options=df["Feedback Head"].dropna().unique(), default=None)
date_range = st.sidebar.date_input("Select Date Range", [df["Date"].min(), df["Date"].max()])

filtered_df = df.copy()
if branches:
    filtered_df = filtered_df[filtered_df["Branch Name"].isin(branches)]
if feedback_types:
    filtered_df = filtered_df[filtered_df["Feedback Head"].isin(feedback_types)]
filtered_df = filtered_df[(filtered_df["Date"] >= date_range[0]) & (filtered_df["Date"] <= date_range[-1])]

# ---- KPIs ----
total_complaints = len(filtered_df)
unique_customers = filtered_df["Customer CLI"].nunique()
demoters = (filtered_df["Feedback Head"] == "Demoter").sum()
promoters = (filtered_df["Feedback Head"] == "Promoter").sum()

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Complaints", total_complaints)
col2.metric("Demoter %", f"{(demoters/total_complaints*100):.1f}%" if total_complaints else "0%")
col3.metric("Promoter %", f"{(promoters/total_complaints*100):.1f}%" if total_complaints else "0%")
col4.metric("Unique Customers", unique_customers)

# ---- Complaint Volume by Branch ----
st.subheader("📍 Complaints by Branch")
branch_counts = filtered_df["Branch Name"].value_counts().reset_index()
branch_counts.columns = ["Branch", "Count"]
fig1 = px.bar(branch_counts, x="Count", y="Branch", orientation="h", color="Branch", text="Count")
st.plotly_chart(fig1, use_container_width=True)

# ---- Complaint Trend Over Time ----
st.subheader("📅 Complaint Trend Over Time")
daily_counts = filtered_df.groupby("Date").size().reset_index(name="Count")
fig2 = px.line(daily_counts, x="Date", y="Count", markers=True)
st.plotly_chart(fig2, use_container_width=True)

# ---- Complaint Categories ----
st.subheader("🍔 Complaint Categories (Tags)")
if "Tags" in filtered_df.columns:
    tag_counts = filtered_df["Tags"].value_counts().head(15).reset_index()
    tag_counts.columns = ["Tag", "Count"]
    fig3 = px.bar(tag_counts, x="Count", y="Tag", orientation="h", color="Tag", text="Count")
    st.plotly_chart(fig3, use_container_width=True)

# ---- Sentiment by Branch ----
st.subheader("😊 Feedback Sentiment by Branch")
sentiment = filtered_df.groupby(["Branch Name", "Feedback Head"]).size().reset_index(name="Count")
fig4 = px.bar(sentiment, x="Branch Name", y="Count", color="Feedback Head", barmode="stack")
st.plotly_chart(fig4, use_container_width=True)

# ---- Complaints by Time of Day ----
st.subheader("⏰ Complaints by Hour of Day")
hourly_counts = filtered_df.groupby("Hour").size().reset_index(name="Count")
fig5 = px.area(hourly_counts, x="Hour", y="Count", markers=True)
st.plotly_chart(fig5, use_container_width=True)

# ---- Word Cloud and Keyword Insights ----
st.subheader("💬 Top Complaint Keywords (Description)")
text = " ".join(str(desc) for desc in filtered_df["Description"].dropna())

if text.strip():
    # Generate WordCloud
    wc = WordCloud(width=800, height=400, background_color="white").generate(text)
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.imshow(wc, interpolation="bilinear")
    ax.axis("off")
    st.pyplot(fig)

    # --- Generate keyword insights ---
    from collections import Counter
    import re

    # Clean and split text
    words = re.findall(r'\b[a-zA-Z]{4,}\b', text.lower())
    common_words = Counter(words).most_common(15)
    common_terms = [w for w, c in common_words]

    # Group logical categories
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

    st.info("No complaint descriptions available for word cloud.")

st.caption("Dashboard built by Arbaz Mubasher – powered by Streamlit, Plotly, and pandas.")
