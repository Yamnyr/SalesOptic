import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import plotly.express as px
import os

st.set_page_config(page_title="Big Data Analytics Hub", layout="wide")

st.markdown("""
<style>

/* Background global */
.stApp {
    background: linear-gradient(135deg, #0f172a, #020617);
    color: #e5e7eb;
}

/* Sidebar */
section[data-testid="stSidebar"] {
    background: #020617;
    border-right: 1px solid #1e293b;
}

/* Titles */
h1, h2, h3 {
    color: #e5e7eb;
    font-weight: 600;
}

/* Metrics – suppression fond blanc */
[data-testid="metric-container"] {
    background: rgba(255, 255, 255, 0.03);
    border: 1px solid rgba(255, 255, 255, 0.08);
    padding: 20px;
    border-radius: 14px;
    backdrop-filter: blur(6px);
    box-shadow: none;
}

/* Metric labels & values */
[data-testid="metric-container"] label {
    color: #9ca3af;
}

[data-testid="metric-container"] div {
    color: #e5e7eb;
}

/* Tabs */
.stTabs [data-baseweb="tab-list"] {
    gap: 12px;
    background: transparent;
}

.stTabs [data-baseweb="tab"] {
    background: rgba(255, 255, 255, 0.03);
    border-radius: 12px;
    padding: 10px 18px;
    color: #9ca3af;
    border: 1px solid rgba(255, 255, 255, 0.05);
}

.stTabs [aria-selected="true"] {
    background: linear-gradient(135deg, #2563eb, #1d4ed8);
    color: white;
    border: none;
}

/* Dataframes */
[data-testid="stDataFrame"] {
    background: rgba(255, 255, 255, 0.02);
    border-radius: 12px;
}

/* Plotly charts container */
.stPlotlyChart {
    background: rgba(255, 255, 255, 0.02);
    border-radius: 16px;
    padding: 10px;
}

/* Horizontal separator */
hr {
    border: none;
    height: 1px;
    background: linear-gradient(to right, transparent, #334155, transparent);
    margin: 30px 0;
}

</style>
""", unsafe_allow_html=True)


st.title("Big Data Analytics & Data Quality Hub")
st.markdown("Plateforme de monitoring en temps réel pour le pipeline de données.")

# Sidebar info
st.sidebar.title("Configuration & Statut")
st.sidebar.info(f"Dernière synchronisation : {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
st.sidebar.success("Pipeline: Opérationnel")

# Tabs for better organization
tab1, tab2, tab3, tab4 = st.tabs(["📊 Business Intelligence", "🛡️ Qualité & Quarantaine", "⚙️ Monitoring Technique", "📄 Documentation"])

with tab1:
    st.header("Ventes et Performances")
    col1, col2, col3, col4 = st.columns(4)

    try:
        df_ventes = pd.read_parquet("/data/gold/ca_par_produit")
        df_daily = pd.read_parquet("/data/gold/ventes_quotidiennes")
        df_pays = pd.read_parquet("/data/gold/ca_par_pays")
        df_silver_ventes = pd.read_parquet("/data/silver/ventes")

        total_ca = df_ventes['chiffre_affaires'].sum()
        total_transactions = len(df_silver_ventes)
        panier_moyen = total_ca / total_transactions if total_transactions > 0 else 0

        col1.metric("Chiffre d'Affaires", f"{total_ca:,.0f} €")
        col2.metric("Transactions", f"{total_transactions:,}")
        col3.metric("Panier Moyen", f"{panier_moyen:.2f} €")
        col4.metric("Produits Uniques", f"{len(df_ventes)}")

        st.markdown("---")

        c1, c2 = st.columns([2, 1])
        with c1:
            st.subheader("📈 Évolution Quotidienne")
            df_daily['jour'] = pd.to_datetime(df_daily['jour'])
            fig_line = px.line(df_daily, x='jour', y='nb_ventes', title="Nombre de ventes par jour")
            st.plotly_chart(fig_line, width="stretch")

        with c2:
            st.subheader("🌍 Répartition Géographique")
            fig_pie = px.pie(df_pays, values='chiffre_affaires', names='pays', hole=0.4)
            st.plotly_chart(fig_pie, width="stretch")

        st.subheader("📦 Performance par Produit")
        st.bar_chart(df_ventes.set_index("produit"))

    except Exception as e:
        st.warning(f"Données Business non disponibles : {e}")

with tab2:
    st.header("Système de Quarantaine des Données")
    st.info("Le système de quarantaine identifie et isole les données corrompues ou incomplètes pour préserver l'intégrité de la zone Silver/Gold.")

    qcol1, qcol2 = st.columns(2)

    try:
        # Load Silver vs Quarantine stats
        qv = pd.read_parquet("/data/quarantine/ventes") if os.path.exists("/data/quarantine/ventes") else pd.DataFrame()
        sv = pd.read_parquet("/data/silver/ventes") if os.path.exists("/data/silver/ventes") else pd.DataFrame()

        valid_count = len(sv)
        quarantine_count = len(qv)
        total_in = valid_count + quarantine_count

        with qcol1:
            st.metric("Taux de Santé des Données", f"{(valid_count/total_in*100):.1f}%" if total_in > 0 else "100%")
            st.write(f"Données Valides : {valid_count}")
            st.write(f"Données en Quarantaine : {quarantine_count}")

        with qcol2:
            if quarantine_count > 0:
                reasons = qv['quarantine_reason'].value_counts().reset_index()
                reasons.columns = ['Raison', 'Compte']
                fig_q = px.bar(reasons, x='Compte', y='Raison', orientation='h', title="Raisons de Mise en Quarantaine")
                st.plotly_chart(fig_q, width="stretch")
            else:
                st.success("Aucune donnée en quarantaine pour le moment !")

        if quarantine_count > 0:
            st.subheader("Détails des données en quarantaine (échantillon)")
            st.dataframe(qv.head(20), width="stretch")

    except Exception as e:
        st.info("Traitement de la quarantaine en cours ou données non disponibles.")

with tab3:
    st.header("Logs & Observabilité")
    lc1, lc2 = st.columns([1, 2])

    with lc1:
        try:
            df_logs = pd.read_parquet("/data/gold/log_stats")
            st.subheader("Statistiques des Logs")
            fig_logs = px.bar(df_logs, x='level', y='count', color='level',
                             color_discrete_map={'ERROR': '#ff4b4b', 'WARNING': '#ffa500', 'INFO': '#00cc96', 'DEBUG': '#3366cc'})
            st.plotly_chart(fig_logs, width="stretch")
        except:
            st.info("Stats de logs non disponibles")

    with lc2:
        try:
            df_events = pd.read_parquet("/data/silver/events")
            st.subheader("Derniers Événements Utilisateurs")
            st.dataframe(df_events.sort_values("timestamp", ascending=False).head(15), width="stretch")
        except:
            st.info("Événements non disponibles")

with tab4:
    st.header("Architecture du Système")
    st.image("schema_architecture.png", width="stretch")
    st.markdown("""
    ### Pipeline de Données
    1. **Bronze** : Données brutes (CSV, JSON, Logs).
    2. **Silver** : Données nettoyées et filtrées (Parquet). Les erreurs partent en **Quarantaine**.
    3. **Gold** : Données agrégées pour le Business (Parquet).
    """)

