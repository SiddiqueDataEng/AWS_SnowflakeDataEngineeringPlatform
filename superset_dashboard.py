"""
superset_dashboard.py
======================
BI Dashboard for the Pakistani AWS + Snowflake
Data Engineering Platform — built with Streamlit + Plotly.

Covers all key BI Dashboard views:
  1.  Executive KPI summary
  2.  Sales by Province (bar + pie)
  3.  Daily Revenue trend (line chart)
  4.  Orders by Priority & Status
  5.  Top Customers by spend
  6.  Supplier performance
  7.  Shipmode & Courier analysis
  8.  Product category breakdown
  9.  Streaming sales live table (SALES_DATA)
  10. ML Forecast (predicted vs actual)
  11. SQL Explorer (run any query)

Run: streamlit run aws_snowflake_data_eng/superset_dashboard.py
"""

import os
import sqlite3
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

# ── Config ────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="🇵🇰 PK Data Platform — BI Dashboard",
    page_icon="🇵🇰",
    layout="wide",
    initial_sidebar_state="expanded",
)

DB_PATH = os.path.join(os.path.dirname(__file__), "local_db", "pk_ecommerce.db")
PROVINCE_COLORS = {
    "Punjab": "#006600", "Sindh": "#003087",
    "KPK": "#8B0000", "Balochistan": "#8B4513", "ICT": "#4B0082",
}


# ── DB helper ─────────────────────────────────────────────────────────────────

@st.cache_data(ttl=30)
def query(sql: str) -> pd.DataFrame:
    if not os.path.exists(DB_PATH):
        return pd.DataFrame()
    conn = sqlite3.connect(DB_PATH)
    try:
        df = pd.read_sql_query(sql, conn)
    except Exception as e:
        df = pd.DataFrame({"error": [str(e)]})
    finally:
        conn.close()
    return df


def query_live(sql: str) -> pd.DataFrame:
    """Uncached version for SQL explorer."""
    if not os.path.exists(DB_PATH):
        return pd.DataFrame()
    conn = sqlite3.connect(DB_PATH)
    try:
        df = pd.read_sql_query(sql, conn)
    except Exception as e:
        df = pd.DataFrame({"error": [str(e)]})
    finally:
        conn.close()
    return df


def get_tables() -> list[str]:
    conn = sqlite3.connect(DB_PATH)
    tables = [r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()]
    conn.close()
    return tables


# ── Sidebar navigation ────────────────────────────────────────────────────────

st.sidebar.image("https://flagcdn.com/w40/pk.png", width=40)
st.sidebar.title("🇵🇰 PK Data Platform")
st.sidebar.markdown("**BI Dashboard**")
st.sidebar.markdown("---")

page = st.sidebar.radio("📊 Views", [
    "Executive Summary",
    "Sales by Province",
    "Revenue Trend",
    "Orders Analysis",
    "Top Customers",
    "Supplier Performance",
    "Shipping & Courier",
    "Product Categories",
    "Streaming Sales",
    "ML Forecast",
    "SQL Explorer",
])

st.sidebar.markdown("---")
st.sidebar.markdown(f"🗄️ `pk_ecommerce.db`")
if os.path.exists(DB_PATH):
    size_mb = os.path.getsize(DB_PATH) / 1024 / 1024
    tables = get_tables()
    st.sidebar.success(f"{len(tables)} tables  ·  {size_mb:.1f} MB")
else:
    st.sidebar.error("DB not found — run run_pipeline.py")


# ════════════════════════════════════════════════════════════
# PAGE: Executive Summary
# ════════════════════════════════════════════════════════════
if page == "Executive Summary":
    st.title("🇵🇰 Pakistani Ecommerce — Executive Dashboard")
    st.caption("Simulated Snowflake + AWS data platform · All amounts in PKR")

    # KPI cards
    kpi = query("""
        SELECT
            (SELECT count(1) FROM ORDERS)                                  as TOTAL_ORDERS,
            (SELECT round(sum(O_TOTALPRICE),0) FROM ORDERS)                as TOTAL_REVENUE_PKR,
            (SELECT count(1) FROM CUSTOMER)                                as TOTAL_CUSTOMERS,
            (SELECT count(1) FROM LINEITEM)                                as TOTAL_LINEITEMS,
            (SELECT count(1) FROM ORDERS WHERE O_ORDERSTATUS='F')          as FULFILLED_ORDERS,
            (SELECT round(avg(O_TOTALPRICE),0) FROM ORDERS)                as AVG_ORDER_PKR
    """)

    if not kpi.empty and "error" not in kpi.columns:
        r = kpi.iloc[0]
        c1, c2, c3, c4, c5, c6 = st.columns(6)
        c1.metric("📦 Total Orders",     f"{int(r['TOTAL_ORDERS']):,}")
        c2.metric("💰 Total Revenue",    f"₨ {int(r['TOTAL_REVENUE_PKR']):,}")
        c3.metric("👥 Customers",        f"{int(r['TOTAL_CUSTOMERS']):,}")
        c4.metric("📋 Line Items",       f"{int(r['TOTAL_LINEITEMS']):,}")
        c5.metric("✅ Fulfilled Orders", f"{int(r['FULFILLED_ORDERS']):,}")
        c6.metric("📊 Avg Order Value",  f"₨ {int(r['AVG_ORDER_PKR']):,}")

    st.markdown("---")
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Revenue by Province")
        df = query("""
            SELECT c.C_PROVINCE, round(sum(o.O_TOTALPRICE),0) as REVENUE_PKR,
                   count(o.O_ORDERKEY) as ORDERS
            FROM ORDERS o JOIN CUSTOMER c ON o.O_CUSTKEY = c.C_CUSTKEY
            GROUP BY c.C_PROVINCE ORDER BY REVENUE_PKR DESC
        """)
        if not df.empty and "error" not in df.columns:
            fig = px.bar(df, x="C_PROVINCE", y="REVENUE_PKR", color="C_PROVINCE",
                         color_discrete_map=PROVINCE_COLORS,
                         labels={"C_PROVINCE": "Province", "REVENUE_PKR": "Revenue (PKR)"},
                         text_auto=".2s")
            fig.update_layout(showlegend=False, height=350)
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Order Status Distribution")
        df2 = query("""
            SELECT
                CASE O_ORDERSTATUS
                    WHEN 'F' THEN 'Fulfilled'
                    WHEN 'O' THEN 'Open'
                    WHEN 'P' THEN 'Processing'
                END as STATUS,
                count(1) as COUNT
            FROM ORDERS GROUP BY O_ORDERSTATUS
        """)
        if not df2.empty and "error" not in df2.columns:
            fig2 = px.pie(df2, names="STATUS", values="COUNT",
                          color_discrete_sequence=["#28a745", "#007bff", "#ffc107"],
                          hole=0.4)
            fig2.update_layout(height=350)
            st.plotly_chart(fig2, use_container_width=True)

    st.markdown("---")
    col3, col4 = st.columns(2)

    with col3:
        st.subheader("Top 5 Cities by Revenue")
        df3 = query("""
            SELECT c.C_CITY, round(sum(o.O_TOTALPRICE),0) as REVENUE
            FROM ORDERS o JOIN CUSTOMER c ON o.O_CUSTKEY=c.C_CUSTKEY
            GROUP BY c.C_CITY ORDER BY REVENUE DESC LIMIT 5
        """)
        if not df3.empty and "error" not in df3.columns:
            fig3 = px.bar(df3, x="REVENUE", y="C_CITY", orientation="h",
                          color="REVENUE", color_continuous_scale="Greens",
                          labels={"C_CITY": "City", "REVENUE": "Revenue (PKR)"})
            fig3.update_layout(height=300, showlegend=False, coloraxis_showscale=False)
            st.plotly_chart(fig3, use_container_width=True)

    with col4:
        st.subheader("Courier Market Share")
        df4 = query("""
            SELECT L_COURIER, count(1) as SHIPMENTS
            FROM LINEITEM GROUP BY L_COURIER ORDER BY SHIPMENTS DESC
        """)
        if not df4.empty and "error" not in df4.columns:
            fig4 = px.pie(df4, names="L_COURIER", values="SHIPMENTS",
                          color_discrete_sequence=px.colors.qualitative.Set3)
            fig4.update_layout(height=300)
            st.plotly_chart(fig4, use_container_width=True)


# ════════════════════════════════════════════════════════════
# PAGE: Sales by Province
# ════════════════════════════════════════════════════════════
elif page == "Sales by Province":
    st.title("🗺️ Sales by Province & City")

    df = query("""
        SELECT c.C_PROVINCE, c.C_CITY,
               count(o.O_ORDERKEY)            as ORDERS,
               round(sum(o.O_TOTALPRICE), 0)  as REVENUE_PKR,
               round(avg(o.O_TOTALPRICE), 0)  as AVG_ORDER_PKR,
               count(distinct o.O_CUSTKEY)    as UNIQUE_CUSTOMERS
        FROM ORDERS o
        JOIN CUSTOMER c ON o.O_CUSTKEY = c.C_CUSTKEY
        GROUP BY c.C_PROVINCE, c.C_CITY
        ORDER BY REVENUE_PKR DESC
    """)

    if df.empty or "error" in df.columns:
        st.error("No data. Run run_pipeline.py first.")
    else:
        prov_filter = st.multiselect("Filter Provinces",
                                     sorted(df["C_PROVINCE"].unique()),
                                     default=sorted(df["C_PROVINCE"].unique()))
        df = df[df["C_PROVINCE"].isin(prov_filter)]

        col1, col2 = st.columns(2)
        with col1:
            fig = px.bar(df.groupby("C_PROVINCE")["REVENUE_PKR"].sum().reset_index(),
                         x="C_PROVINCE", y="REVENUE_PKR", color="C_PROVINCE",
                         color_discrete_map=PROVINCE_COLORS, text_auto=".2s",
                         title="Total Revenue by Province (PKR)")
            fig.update_layout(showlegend=False, height=400)
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            fig2 = px.pie(df.groupby("C_PROVINCE")["ORDERS"].sum().reset_index(),
                          names="C_PROVINCE", values="ORDERS",
                          color="C_PROVINCE", color_discrete_map=PROVINCE_COLORS,
                          title="Order Count by Province", hole=0.35)
            fig2.update_layout(height=400)
            st.plotly_chart(fig2, use_container_width=True)

        st.subheader("City-level breakdown")
        fig3 = px.treemap(df, path=["C_PROVINCE", "C_CITY"], values="REVENUE_PKR",
                          color="REVENUE_PKR", color_continuous_scale="Greens",
                          title="Revenue Treemap: Province → City")
        fig3.update_layout(height=450)
        st.plotly_chart(fig3, use_container_width=True)

        st.subheader("Detailed Table")
        st.dataframe(df.style.format({
            "REVENUE_PKR": "₨ {:,.0f}", "AVG_ORDER_PKR": "₨ {:,.0f}"
        }), use_container_width=True)


# ════════════════════════════════════════════════════════════
# PAGE: Revenue Trend
# ════════════════════════════════════════════════════════════
elif page == "Revenue Trend":
    st.title("📈 Daily Revenue Trend")

    df = query("""
        SELECT O_ORDERDATE as DATE,
               count(O_ORDERKEY)            as DAILY_ORDERS,
               round(sum(O_TOTALPRICE), 0)  as DAILY_REVENUE_PKR,
               round(avg(O_TOTALPRICE), 0)  as AVG_ORDER_PKR
        FROM ORDERS
        GROUP BY O_ORDERDATE
        ORDER BY O_ORDERDATE
    """)

    if df.empty or "error" in df.columns:
        st.error("No data.")
    else:
        df["DATE"] = pd.to_datetime(df["DATE"])

        col1, col2 = st.columns([3, 1])
        with col2:
            granularity = st.selectbox("Granularity", ["Daily", "Weekly", "Monthly"])

        if granularity == "Weekly":
            df = df.resample("W", on="DATE").sum(numeric_only=True).reset_index()
        elif granularity == "Monthly":
            df = df.resample("ME", on="DATE").sum(numeric_only=True).reset_index()

        fig = px.area(df, x="DATE", y="DAILY_REVENUE_PKR",
                      title=f"{granularity} Revenue (PKR)",
                      color_discrete_sequence=["#28a745"])
        fig.update_layout(height=380)
        st.plotly_chart(fig, use_container_width=True)

        fig2 = px.bar(df, x="DATE", y="DAILY_ORDERS",
                      title=f"{granularity} Order Count",
                      color_discrete_sequence=["#007bff"])
        fig2.update_layout(height=300)
        st.plotly_chart(fig2, use_container_width=True)


# ════════════════════════════════════════════════════════════
# PAGE: Orders Analysis
# ════════════════════════════════════════════════════════════
elif page == "Orders Analysis":
    st.title("📦 Orders Analysis")

    col1, col2 = st.columns(2)

    with col1:
        df = query("""
            SELECT O_ORDERPRIORITY as PRIORITY,
                   count(1) as COUNT,
                   round(avg(O_TOTALPRICE),0) as AVG_PKR
            FROM ORDERS GROUP BY O_ORDERPRIORITY ORDER BY COUNT DESC
        """)
        if not df.empty and "error" not in df.columns:
            fig = px.bar(df, x="PRIORITY", y="COUNT", color="AVG_PKR",
                         color_continuous_scale="RdYlGn",
                         title="Orders by Priority (color = Avg Value PKR)",
                         text="COUNT")
            fig.update_layout(height=380)
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        df2 = query("""
            SELECT
                CASE O_ORDERSTATUS WHEN 'F' THEN 'Fulfilled'
                    WHEN 'O' THEN 'Open' WHEN 'P' THEN 'Processing' END as STATUS,
                count(1) as COUNT,
                round(sum(O_TOTALPRICE),0) as TOTAL_PKR
            FROM ORDERS GROUP BY O_ORDERSTATUS
        """)
        if not df2.empty and "error" not in df2.columns:
            fig2 = go.Figure(data=[
                go.Bar(name="Orders", x=df2["STATUS"], y=df2["COUNT"], yaxis="y",
                       marker_color=["#28a745","#007bff","#ffc107"]),
                go.Scatter(name="Revenue PKR", x=df2["STATUS"], y=df2["TOTAL_PKR"],
                           yaxis="y2", mode="markers+lines",
                           marker=dict(size=12, color="#dc3545")),
            ])
            fig2.update_layout(title="Orders + Revenue by Status",
                               yaxis=dict(title="Order Count"),
                               yaxis2=dict(title="Revenue PKR", overlaying="y", side="right"),
                               height=380)
            st.plotly_chart(fig2, use_container_width=True)

    # GST rate distribution
    df3 = query("""
        SELECT CASE L_TAX WHEN 0 THEN 'Zero-rated (0%)' WHEN 0.05 THEN 'Reduced (5%)'
                          WHEN 0.17 THEN 'Standard GST (17%)' ELSE 'Other' END as GST_BAND,
               count(1) as ITEMS, round(sum(L_TAX * L_EXTENDEDPRICE),0) as GST_COLLECTED_PKR
        FROM LINEITEM GROUP BY L_TAX ORDER BY ITEMS DESC
    """)
    col3, col4 = st.columns(2)
    with col3:
        if not df3.empty and "error" not in df3.columns:
            fig3 = px.pie(df3, names="GST_BAND", values="ITEMS",
                          title="Pakistan GST Rate Distribution (by item count)",
                          color_discrete_sequence=["#28a745","#ffc107","#dc3545"])
            fig3.update_layout(height=320)
            st.plotly_chart(fig3, use_container_width=True)
    with col4:
        if not df3.empty and "error" not in df3.columns:
            fig4 = px.bar(df3, x="GST_BAND", y="GST_COLLECTED_PKR",
                          title="GST Collected by Rate (PKR)",
                          color="GST_BAND",
                          color_discrete_sequence=["#28a745","#ffc107","#dc3545"],
                          text_auto=".2s")
            fig4.update_layout(showlegend=False, height=320)
            st.plotly_chart(fig4, use_container_width=True)


# ════════════════════════════════════════════════════════════
# PAGE: Top Customers
# ════════════════════════════════════════════════════════════
elif page == "Top Customers":
    st.title("👥 Top Customers")

    n = st.slider("Show top N customers", 5, 50, 20)
    df = query(f"""
        SELECT c.C_NAME, c.C_CITY, c.C_PROVINCE, c.C_GENDER,
               count(o.O_ORDERKEY)           as ORDERS,
               round(sum(o.O_TOTALPRICE), 0) as LIFETIME_VALUE_PKR,
               round(avg(o.O_TOTALPRICE), 0) as AVG_ORDER_PKR
        FROM CUSTOMER c
        JOIN ORDERS o ON c.C_CUSTKEY = o.O_CUSTKEY
        GROUP BY c.C_CUSTKEY
        ORDER BY LIFETIME_VALUE_PKR DESC
        LIMIT {n}
    """)

    if not df.empty and "error" not in df.columns:
        fig = px.bar(df, x="LIFETIME_VALUE_PKR", y="C_NAME", orientation="h",
                     color="C_PROVINCE", color_discrete_map=PROVINCE_COLORS,
                     title=f"Top {n} Customers by Lifetime Value (PKR)",
                     hover_data=["C_CITY", "ORDERS", "AVG_ORDER_PKR"])
        fig.update_layout(height=max(400, n * 22), yaxis={"categoryorder": "total ascending"})
        st.plotly_chart(fig, use_container_width=True)

        col1, col2 = st.columns(2)
        with col1:
            gender_df = query("""
                SELECT C_GENDER, count(1) as COUNT,
                       round(avg(o.O_TOTALPRICE),0) as AVG_PKR
                FROM CUSTOMER c JOIN ORDERS o ON c.C_CUSTKEY=o.O_CUSTKEY
                GROUP BY C_GENDER
            """)
            if not gender_df.empty:
                gender_df["GENDER"] = gender_df["C_GENDER"].map({"M": "Male", "F": "Female"})
                fig2 = px.pie(gender_df, names="GENDER", values="COUNT",
                              title="Customer Gender Split",
                              color_discrete_sequence=["#007bff", "#e83e8c"])
                st.plotly_chart(fig2, use_container_width=True)

        with col2:
            st.dataframe(df[["C_NAME","C_CITY","C_PROVINCE","ORDERS","LIFETIME_VALUE_PKR"]]
                         .style.format({"LIFETIME_VALUE_PKR": "₨ {:,.0f}"}),
                         use_container_width=True, height=300)


# ════════════════════════════════════════════════════════════
# PAGE: Supplier Performance
# ════════════════════════════════════════════════════════════
elif page == "Supplier Performance":
    st.title("🏭 Supplier Performance")

    df = query("""
        SELECT s.S_NAME, s.S_CITY, s.S_PROVINCE,
               count(li.L_ORDERKEY)                             as SHIPMENTS,
               round(sum(li.L_QUANTITY), 0)                     as TOTAL_QTY,
               round(sum(li.L_EXTENDEDPRICE), 0)                as GROSS_REVENUE_PKR,
               round(sum(li.L_EXTENDEDPRICE*(1-li.L_DISCOUNT)),0) as NET_REVENUE_PKR,
               round(avg(li.L_DISCOUNT)*100, 1)                 as AVG_DISCOUNT_PCT
        FROM SUPPLIER s
        JOIN LINEITEM li ON s.S_SUPPKEY = li.L_SUPPKEY
        GROUP BY s.S_SUPPKEY
        ORDER BY NET_REVENUE_PKR DESC
    """)

    if df.empty or "error" in df.columns:
        st.error("No data.")
    else:
        fig = px.bar(df, x="S_NAME", y="NET_REVENUE_PKR", color="S_PROVINCE",
                     color_discrete_map=PROVINCE_COLORS,
                     title="Supplier Net Revenue (after discounts) — PKR",
                     text_auto=".2s", hover_data=["AVG_DISCOUNT_PCT", "SHIPMENTS"])
        fig.update_layout(height=400, xaxis_tickangle=-30)
        st.plotly_chart(fig, use_container_width=True)

        col1, col2 = st.columns(2)
        with col1:
            fig2 = px.scatter(df, x="SHIPMENTS", y="NET_REVENUE_PKR",
                              size="TOTAL_QTY", color="S_PROVINCE",
                              color_discrete_map=PROVINCE_COLORS,
                              hover_name="S_NAME",
                              title="Shipments vs Revenue (bubble = quantity)")
            fig2.update_layout(height=350)
            st.plotly_chart(fig2, use_container_width=True)

        with col2:
            fig3 = px.bar(df, x="S_NAME", y="AVG_DISCOUNT_PCT",
                          title="Average Discount % by Supplier",
                          color="AVG_DISCOUNT_PCT",
                          color_continuous_scale="Reds",
                          text_auto=".1f")
            fig3.update_layout(height=350, xaxis_tickangle=-30, coloraxis_showscale=False)
            st.plotly_chart(fig3, use_container_width=True)

        st.dataframe(df.style.format({
            "NET_REVENUE_PKR": "₨ {:,.0f}",
            "GROSS_REVENUE_PKR": "₨ {:,.0f}",
            "AVG_DISCOUNT_PCT": "{:.1f}%",
        }), use_container_width=True)


# ════════════════════════════════════════════════════════════
# PAGE: Shipping & Courier
# ════════════════════════════════════════════════════════════
elif page == "Shipping & Courier":
    st.title("🚚 Shipping Mode & Courier Analysis")

    col1, col2 = st.columns(2)
    with col1:
        df = query("""
            SELECT L_SHIPMODE,
                   count(1) as SHIPMENTS,
                   round(sum(L_EXTENDEDPRICE*(1-L_DISCOUNT)),0) as NET_REVENUE_PKR
            FROM LINEITEM GROUP BY L_SHIPMODE ORDER BY SHIPMENTS DESC
        """)
        if not df.empty and "error" not in df.columns:
            fig = px.bar(df, x="L_SHIPMODE", y="SHIPMENTS", color="NET_REVENUE_PKR",
                         color_continuous_scale="Blues",
                         title="Shipments by Mode", text="SHIPMENTS")
            fig.update_layout(height=360, coloraxis_showscale=False)
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        df2 = query("""
            SELECT L_COURIER,
                   count(1) as SHIPMENTS,
                   round(avg(julianday(L_RECEIPTDATE)-julianday(L_SHIPDATE)),1) as AVG_DELIVERY_DAYS
            FROM LINEITEM GROUP BY L_COURIER ORDER BY SHIPMENTS DESC
        """)
        if not df2.empty and "error" not in df2.columns:
            fig2 = go.Figure(data=[
                go.Bar(name="Shipments", x=df2["L_COURIER"], y=df2["SHIPMENTS"],
                       marker_color="#007bff"),
                go.Scatter(name="Avg Delivery Days", x=df2["L_COURIER"],
                           y=df2["AVG_DELIVERY_DAYS"], yaxis="y2",
                           mode="markers+lines", marker=dict(size=10, color="#dc3545")),
            ])
            fig2.update_layout(
                title="Courier: Shipments vs Avg Delivery Days",
                yaxis=dict(title="Shipments"),
                yaxis2=dict(title="Avg Delivery Days", overlaying="y", side="right"),
                height=360,
            )
            st.plotly_chart(fig2, use_container_width=True)

    df3 = query("""
        SELECT L_SHIPMODE, L_COURIER, count(1) as SHIPMENTS
        FROM LINEITEM GROUP BY L_SHIPMODE, L_COURIER
    """)
    if not df3.empty and "error" not in df3.columns:
        fig3 = px.density_heatmap(df3, x="L_SHIPMODE", y="L_COURIER", z="SHIPMENTS",
                                  color_continuous_scale="Greens",
                                  title="Heatmap: Shipmode × Courier")
        fig3.update_layout(height=380)
        st.plotly_chart(fig3, use_container_width=True)


# ════════════════════════════════════════════════════════════
# PAGE: Product Categories
# ════════════════════════════════════════════════════════════
elif page == "Product Categories":
    st.title("🛍️ Product Category Analysis")

    df = query("""
        SELECT p.P_CATEGORY,
               count(li.L_ORDERKEY)                              as ITEMS_SOLD,
               round(sum(li.L_QUANTITY), 0)                      as TOTAL_QTY,
               round(sum(li.L_EXTENDEDPRICE*(1-li.L_DISCOUNT)),0) as NET_REVENUE_PKR,
               round(avg(p.P_RETAILPRICE),0)                     as AVG_PRICE_PKR
        FROM PART p
        JOIN LINEITEM li ON p.P_PARTKEY = li.L_PARTKEY
        GROUP BY p.P_CATEGORY
        ORDER BY NET_REVENUE_PKR DESC
    """)

    if df.empty or "error" in df.columns:
        st.error("No data.")
    else:
        col1, col2 = st.columns(2)
        with col1:
            fig = px.pie(df, names="P_CATEGORY", values="NET_REVENUE_PKR",
                         title="Revenue Share by Category",
                         color_discrete_sequence=px.colors.qualitative.Set2, hole=0.3)
            fig.update_layout(height=380)
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            fig2 = px.bar(df, x="P_CATEGORY", y="NET_REVENUE_PKR",
                          color="AVG_PRICE_PKR", color_continuous_scale="Viridis",
                          title="Revenue by Category (color = Avg Price)", text_auto=".2s")
            fig2.update_layout(height=380, xaxis_tickangle=-20, coloraxis_showscale=False)
            st.plotly_chart(fig2, use_container_width=True)

        # Top products per category
        st.subheader("Top Products")
        cat = st.selectbox("Select Category", df["P_CATEGORY"].tolist())
        df_prod = query(f"""
            SELECT p.P_NAME, count(li.L_ORDERKEY) as ITEMS_SOLD,
                   round(sum(li.L_EXTENDEDPRICE*(1-li.L_DISCOUNT)),0) as REVENUE_PKR,
                   round(avg(p.P_RETAILPRICE),0) as AVG_PRICE_PKR
            FROM PART p JOIN LINEITEM li ON p.P_PARTKEY=li.L_PARTKEY
            WHERE p.P_CATEGORY='{cat}'
            GROUP BY p.P_NAME ORDER BY REVENUE_PKR DESC LIMIT 10
        """)
        if not df_prod.empty and "error" not in df_prod.columns:
            fig3 = px.bar(df_prod, x="REVENUE_PKR", y="P_NAME", orientation="h",
                          title=f"Top Products — {cat}",
                          color="AVG_PRICE_PKR", color_continuous_scale="Blues",
                          text_auto=".2s")
            fig3.update_layout(height=350, yaxis={"categoryorder":"total ascending"},
                               coloraxis_showscale=False)
            st.plotly_chart(fig3, use_container_width=True)


# ════════════════════════════════════════════════════════════
# PAGE: Streaming Sales (Kafka → Snowflake Sink)
# ════════════════════════════════════════════════════════════
elif page == "Streaming Sales":
    st.title("📡 Live Streaming Sales (Kafka → SALES_DATA)")
    st.caption("Data from pk_kafka_producer.py → pk_kafka_consumer.py → SQLite sink")

    if st.button("🔄 Refresh"):
        st.cache_data.clear()

    df = query("SELECT * FROM SALES_DATA ORDER BY TRANSACTION_TS DESC LIMIT 200")

    if df.empty or "error" in df.columns:
        st.warning("No streaming data. Run: python aws_snowflake_data_eng/09_kafka_streaming/pk_kafka_producer.py")
    else:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total Events",    f"{len(df):,}")
        c2.metric("Total Revenue",   f"₨ {df['AMOUNT_PKR'].sum():,.0f}")
        c3.metric("Cities",          f"{df['CITY'].nunique()}")
        c4.metric("Payment Methods", f"{df['PAYMENT_METHOD'].nunique()}")

        col1, col2 = st.columns(2)
        with col1:
            city_rev = df.groupby("CITY")["AMOUNT_PKR"].sum().sort_values(ascending=False).reset_index()
            fig = px.bar(city_rev, x="CITY", y="AMOUNT_PKR",
                         title="Streaming Revenue by City (PKR)",
                         color="AMOUNT_PKR", color_continuous_scale="Greens",
                         text_auto=".2s")
            fig.update_layout(height=360, coloraxis_showscale=False)
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            pay = df.groupby("PAYMENT_METHOD").size().reset_index(name="EVENTS")
            fig2 = px.pie(pay, names="PAYMENT_METHOD", values="EVENTS",
                          title="Payment Method Split",
                          color_discrete_sequence=px.colors.qualitative.Pastel)
            fig2.update_layout(height=360)
            st.plotly_chart(fig2, use_container_width=True)

        st.subheader("Product Category Revenue (from stream)")
        cat_df = df.groupby("PRODUCT_CATEGORY")["AMOUNT_PKR"].sum().sort_values(ascending=False).reset_index()
        fig3 = px.bar(cat_df, x="PRODUCT_CATEGORY", y="AMOUNT_PKR",
                      color="PRODUCT_CATEGORY", text_auto=".2s",
                      color_discrete_sequence=px.colors.qualitative.Set1)
        fig3.update_layout(height=320, showlegend=False)
        st.plotly_chart(fig3, use_container_width=True)

        st.subheader("Recent Events")
        st.dataframe(df[["TRANSACTION_TS","CUSTOMER_NAME","CITY","PRODUCT_NAME",
                          "QUANTITY","AMOUNT_PKR","PAYMENT_METHOD","COURIER"]]
                     .style.format({"AMOUNT_PKR": "₨ {:,.2f}"}),
                     use_container_width=True, height=350)


# ════════════════════════════════════════════════════════════
# PAGE: ML Forecast
# ════════════════════════════════════════════════════════════
elif page == "ML Forecast":
    st.title("🤖 ML Sales Forecast (Snowpark LinearRegression)")
    st.caption("Model R² = 0.8841 — trained on provincial monthly revenue data")

    df = query("""
        SELECT YEAR, MONTH, S_PROVINCE,
               round(REVENUE_PKR, 0)   as ACTUAL_PKR,
               round(PREDICTED_PKR, 0) as PREDICTED_PKR
        FROM ML_SALES_FORECAST
        ORDER BY S_PROVINCE, YEAR, MONTH
    """)

    if df.empty or "error" in df.columns:
        st.warning("No ML data. Run 11_snowpark/snowpark_simulation.py")
    else:
        df["DATE"] = pd.to_datetime(df[["YEAR","MONTH"]].assign(DAY=1))
        prov = st.selectbox("Province", sorted(df["S_PROVINCE"].unique()))
        sub = df[df["S_PROVINCE"] == prov]

        fig = go.Figure([
            go.Scatter(x=sub["DATE"], y=sub["ACTUAL_PKR"],   name="Actual",
                       mode="lines+markers", line=dict(color="#28a745", width=2)),
            go.Scatter(x=sub["DATE"], y=sub["PREDICTED_PKR"], name="Predicted",
                       mode="lines", line=dict(color="#dc3545", width=2, dash="dash")),
        ])
        fig.update_layout(title=f"{prov} — Actual vs Predicted Revenue (PKR)",
                          xaxis_title="Month", yaxis_title="Revenue PKR", height=400)
        st.plotly_chart(fig, use_container_width=True)

        # All provinces comparison
        fig2 = px.line(df, x="DATE", y="PREDICTED_PKR", color="S_PROVINCE",
                       color_discrete_map=PROVINCE_COLORS,
                       title="Predicted Revenue — All Provinces")
        fig2.update_layout(height=380)
        st.plotly_chart(fig2, use_container_width=True)

        mae = abs(df["ACTUAL_PKR"] - df["PREDICTED_PKR"]).mean()
        st.metric("Mean Absolute Error (PKR)", f"₨ {mae:,.0f}")
        st.dataframe(sub[["DATE","ACTUAL_PKR","PREDICTED_PKR"]].style.format({
            "ACTUAL_PKR": "₨ {:,.0f}", "PREDICTED_PKR": "₨ {:,.0f}"
        }), use_container_width=True)


# ════════════════════════════════════════════════════════════
# PAGE: SQL Explorer
# ════════════════════════════════════════════════════════════
elif page == "SQL Explorer":
    st.title("🔍 SQL Explorer")
    st.caption("Run any SQL query against pk_ecommerce.db (SQLite)")

    tables = get_tables()
    st.sidebar.markdown("**Available Tables**")
    for t in tables:
        st.sidebar.code(t, language=None)

    default_sql = """SELECT
    c.C_PROVINCE,
    li.L_SHIPMODE,
    li.L_COURIER,
    count(*)                                    as SHIPMENTS,
    round(sum(li.L_EXTENDEDPRICE*(1-li.L_DISCOUNT)),0) as NET_REVENUE_PKR
FROM LINEITEM li
JOIN SUPPLIER s ON li.L_SUPPKEY = s.S_SUPPKEY
JOIN ORDERS o   ON li.L_ORDERKEY = o.O_ORDERKEY
JOIN CUSTOMER c ON o.O_CUSTKEY = c.C_CUSTKEY
GROUP BY 1,2,3
ORDER BY NET_REVENUE_PKR DESC
LIMIT 20"""

    sql = st.text_area("SQL Query", value=default_sql, height=220)

    if st.button("▶ Run Query"):
        with st.spinner("Executing..."):
            df = query_live(sql)
        if "error" in df.columns:
            st.error(df["error"].iloc[0])
        else:
            st.success(f"{len(df)} rows returned")
            st.dataframe(df, use_container_width=True)

            # Auto-visualise if result has 2–3 columns
            if 2 <= len(df.columns) <= 4 and len(df) > 0:
                num_cols = df.select_dtypes("number").columns.tolist()
                cat_cols = df.select_dtypes("object").columns.tolist()
                if num_cols and cat_cols:
                    try:
                        fig = px.bar(df.head(30), x=cat_cols[0], y=num_cols[0],
                                     title=f"Auto-chart: {cat_cols[0]} vs {num_cols[0]}",
                                     color=num_cols[0], color_continuous_scale="Viridis",
                                     text_auto=".2s")
                        fig.update_layout(height=350, coloraxis_showscale=False,
                                          xaxis_tickangle=-30)
                        st.plotly_chart(fig, use_container_width=True)
                    except Exception:
                        pass


# ── Footer ────────────────────────────────────────────────────────────────────
st.sidebar.markdown("---")
st.sidebar.caption("🇵🇰 Pakistani AWS + Snowflake\nData Engineering Platform\nBI Dashboard — Streamlit + Plotly")
