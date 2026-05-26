"""
Dashboard Ejecutivo FleetLogix - Visualización de Métricas del Data Warehouse
===============================================================================
Autor: Matias Damian Gutierrez
Versión: 1.0
Descripción: Dashboard interactivo para monitoreo de métricas clave del sistema
             de gestión de flota y logística FleetLogix.

Este dashboard proporciona visualizaciones en tiempo real de:
- Métricas operativas (entregas, eficiencia, puntualidad)
- Análisis de rendimiento de conductores y vehículos
- Tendencias temporales y forecasting
- Alertas de calidad de datos
- KPIs ejecutivos para toma de decisiones

Uso:
    streamlit run MA_dashboard.py

Dependencias:
    - streamlit: Framework de dashboard interactivo
    - pandas: Manipulación de datos
    - plotly: Gráficos interactivos
    - snowflake-connector-python: Conexión a Snowflake
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import configparser
import os
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine

# ============================================================================
# CONFIGURACIÓN DE LA PÁGINA
# ============================================================================
st.set_page_config(
    page_title="FleetLogix Dashboard",
    page_icon="🚚",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================================================
# CONEXIÓN A SNOWFLAKE
# ============================================================================
@st.cache_resource
def get_snowflake_connection():
    """
    Establece conexión con Snowflake usando credenciales de settings.ini.
    
    Returns:
        sqlalchemy.engine.Engine: Motor de conexión a Snowflake.
    """
    config = configparser.ConfigParser()
    
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_dir)
    config_path = os.path.join(project_root, 'config', 'settings.ini')
    
    if not os.path.exists(config_path):
        st.error(f"Archivo de configuración no encontrado: {config_path}")
        return None
    
    config.read(config_path)
    
    # Buscar sección snowflake (case insensitive)
    snowflake_section = None
    for section in config.sections():
        if section.lower() == 'snowflake':
            snowflake_section = section
            break
    
    if not snowflake_section:
        st.error("Sección [snowflake] no encontrada en configuración")
        return None
    
    try:
        connection_string = URL(
            account=config[snowflake_section]['account'],
            user=config[snowflake_section]['user'],
            password=config[snowflake_section]['password'],
            database=config[snowflake_section]['database'],
            schema=config[snowflake_section]['schema'],
            warehouse=config[snowflake_section]['warehouse'],
            role=config[snowflake_section].get('role', 'ACCOUNTADMIN')
        )
        engine = create_engine(connection_string)
        return engine
    except Exception as e:
        st.error(f"Error conectando a Snowflake: {e}")
        return None

# ============================================================================
# FUNCIONES DE EXTRACCIÓN DE DATOS
# ============================================================================
@st.cache_data(ttl=300)  # Cache por 5 minutos
def get_kpi_metrics(engine):
    """
    Extrae métricas KPI principales del Data Warehouse.
    
    Args:
        engine: Motor de conexión a Snowflake.
    
    Returns:
        dict: Diccionario con métricas KPI.
    """
    try:
        query = """
        SELECT 
            COUNT(*) as total_deliveries,
            SUM(CASE WHEN is_on_time = TRUE THEN 1 ELSE 0 END) as on_time_deliveries,
            AVG(fuel_efficiency_km_per_liter) as avg_fuel_efficiency,
            AVG(delivery_time_minutes) as avg_delivery_time,
            SUM(CASE WHEN delivery_status = 'delivered' THEN 1 ELSE 0 END) as completed_deliveries,
            SUM(revenue_per_delivery) as total_revenue,
            SUM(cost_per_delivery) as total_cost
        FROM ANALYTICS.FACT_DELIVERIES
        WHERE scheduled_datetime >= DATEADD(day, -30, CURRENT_TIMESTAMP())
        """
        
        df = pd.read_sql(query, engine)
        
        if len(df) > 0:
            return {
                'total_deliveries': int(df.iloc[0]['total_deliveries']),
                'on_time_rate': (df.iloc[0]['on_time_deliveries'] / df.iloc[0]['total_deliveries'] * 100) if df.iloc[0]['total_deliveries'] > 0 else 0,
                'avg_fuel_efficiency': float(df.iloc[0]['avg_fuel_efficiency']) if df.iloc[0]['avg_fuel_efficiency'] else 0,
                'avg_delivery_time': float(df.iloc[0]['avg_delivery_time']) if df.iloc[0]['avg_delivery_time'] else 0,
                'completion_rate': (df.iloc[0]['completed_deliveries'] / df.iloc[0]['total_deliveries'] * 100) if df.iloc[0]['total_deliveries'] > 0 else 0,
                'total_revenue': float(df.iloc[0]['total_revenue']) if df.iloc[0]['total_revenue'] else 0,
                'total_cost': float(df.iloc[0]['total_cost']) if df.iloc[0]['total_cost'] else 0,
                'profit_margin': ((df.iloc[0]['total_revenue'] - df.iloc[0]['total_cost']) / df.iloc[0]['total_revenue'] * 100) if df.iloc[0]['total_revenue'] > 0 else 0
            }
        return None
    except Exception as e:
        st.error(f"Error extrayendo KPIs: {e}")
        return None

@st.cache_data(ttl=300)
def get_daily_trends(engine, days=30):
    """
    Extrae tendencias diarias de entregas.
    
    Args:
        engine: Motor de conexión a Snowflake.
        days: Número de días a analizar.
    
    Returns:
        pandas.DataFrame: DataFrame con tendencias diarias.
    """
    try:
        query = f"""
        SELECT 
            DATE(scheduled_datetime) as date,
            COUNT(*) as deliveries,
            SUM(CASE WHEN is_on_time = TRUE THEN 1 ELSE 0 END) as on_time,
            AVG(fuel_efficiency_km_per_liter) as avg_efficiency,
            SUM(revenue_per_delivery) as daily_revenue
        FROM ANALYTICS.FACT_DELIVERIES
        WHERE scheduled_datetime >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
        GROUP BY DATE(scheduled_datetime)
        ORDER BY date ASC
        """
        
        df = pd.read_sql(query, engine)
        df['date'] = pd.to_datetime(df['date'])
        return df
    except Exception as e:
        st.error(f"Error extrayendo tendencias: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=300)
def get_top_performers(engine, metric='efficiency', limit=10):
    """
    Extrae los mejores performers según métrica especificada.
    
    Args:
        engine: Motor de conexión a Snowflake.
        metric: Métrica a ordenar ('efficiency', 'on_time', 'revenue').
        limit: Número de resultados a retornar.
    
    Returns:
        pandas.DataFrame: DataFrame con top performers.
    """
    try:
        if metric == 'efficiency':
            query = f"""
            SELECT 
                dr.full_name as name,
                AVG(f.fuel_efficiency_km_per_liter) as metric_value,
                COUNT(*) as deliveries
            FROM ANALYTICS.FACT_DELIVERIES f
            JOIN ANALYTICS.DIM_DRIVER dr ON f.driver_key = dr.driver_key
            WHERE f.scheduled_datetime >= DATEADD(day, -30, CURRENT_TIMESTAMP())
            GROUP BY dr.full_name
            ORDER BY metric_value DESC
            LIMIT {limit}
            """
        elif metric == 'on_time':
            query = f"""
            SELECT 
                dr.full_name as name,
                AVG(CASE WHEN f.is_on_time = TRUE THEN 1 ELSE 0 END) * 100 as metric_value,
                COUNT(*) as deliveries
            FROM ANALYTICS.FACT_DELIVERIES f
            JOIN ANALYTICS.DIM_DRIVER dr ON f.driver_key = dr.driver_key
            WHERE f.scheduled_datetime >= DATEADD(day, -30, CURRENT_TIMESTAMP())
            GROUP BY dr.full_name
            ORDER BY metric_value DESC
            LIMIT {limit}
            """
        else:  # revenue
            query = f"""
            SELECT 
                dr.full_name as name,
                SUM(f.revenue_per_delivery) as metric_value,
                COUNT(*) as deliveries
            FROM ANALYTICS.FACT_DELIVERIES f
            JOIN ANALYTICS.DIM_DRIVER dr ON f.driver_key = dr.driver_key
            WHERE f.scheduled_datetime >= DATEADD(day, -30, CURRENT_TIMESTAMP())
            GROUP BY dr.full_name
            ORDER BY metric_value DESC
            LIMIT {limit}
            """
        
        df = pd.read_sql(query, engine)
        return df
    except Exception as e:
        st.error(f"Error extrayendo performers: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=300)
def get_route_performance(engine):
    """
    Extrae rendimiento de rutas.
    
    Args:
        engine: Motor de conexión a Snowflake.
    
    Returns:
        pandas.DataFrame: DataFrame con rendimiento de rutas.
    """
    try:
        query = """
        SELECT 
            r.route_code,
            r.origin_city || ' -> ' || r.destination_city as route_name,
            COUNT(*) as deliveries,
            AVG(f.delay_minutes) as avg_delay,
            AVG(f.fuel_efficiency_km_per_liter) as avg_efficiency,
            SUM(f.revenue_per_delivery) as total_revenue
        FROM ANALYTICS.FACT_DELIVERIES f
        JOIN ANALYTICS.DIM_ROUTE r ON f.route_key = r.route_key
        WHERE f.scheduled_datetime >= DATEADD(day, -30, CURRENT_TIMESTAMP())
        GROUP BY r.route_code, r.origin_city, r.destination_city
        ORDER BY total_revenue DESC
        LIMIT 15
        """
        
        df = pd.read_sql(query, engine)
        return df
    except Exception as e:
        st.error(f"Error extrayendo rendimiento de rutas: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=300)
def get_data_quality_alerts(engine):
    """
    Genera alertas de calidad de datos.
    
    Args:
        engine: Motor de conexión a Snowflake.
    
    Returns:
        list: Lista de alertas de calidad de datos.
    """
    alerts = []
    
    try:
        # Verificar datos nulos críticos
        query_nulls = """
        SELECT 
            'NULL values in critical fields' as alert_type,
            COUNT(*) as count
        FROM ANALYTICS.FACT_DELIVERIES
        WHERE delivery_id IS NULL 
           OR vehicle_key IS NULL 
           OR driver_key IS NULL
           OR scheduled_datetime IS NULL
        """
        
        df_nulls = pd.read_sql(query_nulls, engine)
        if df_nulls.iloc[0]['count'] > 0:
            alerts.append({
                'type': 'CRITICAL',
                'message': f"{df_nulls.iloc[0]['count']} registros con valores NULL en campos críticos",
                'severity': 'high'
            })
        
        # Verificar eficiencia de combustible anómala
        query_efficiency = """
        SELECT COUNT(*) as count
        FROM ANALYTICS.FACT_DELIVERIES
        WHERE fuel_efficiency_km_per_liter < 2 OR fuel_efficiency_km_per_liter > 50
        """
        
        df_efficiency = pd.read_sql(query_efficiency, engine)
        if df_efficiency.iloc[0]['count'] > 0:
            alerts.append({
                'type': 'WARNING',
                'message': f"{df_efficiency.iloc[0]['count']} registros con eficiencia de combustible anómala",
                'severity': 'medium'
            })
        
        # Verificar entregas con retrasos extremos
        query_delays = """
        SELECT COUNT(*) as count
        FROM ANALYTICS.FACT_DELIVERIES
        WHERE delay_minutes > 120
        """
        
        df_delays = pd.read_sql(query_delays, engine)
        if df_delays.iloc[0]['count'] > 0:
            alerts.append({
                'type': 'WARNING',
                'message': f"{df_delays.iloc[0]['count']} entregas con retrasos extremos (> 2 horas)",
                'severity': 'medium'
            })
        
        if not alerts:
            alerts.append({
                'type': 'INFO',
                'message': 'No se detectaron problemas de calidad de datos',
                'severity': 'low'
            })
        
        return alerts
    except Exception as e:
        st.error(f"Error verificando calidad de datos: {e}")
        return [{'type': 'ERROR', 'message': f'Error: {e}', 'severity': 'high'}]

# ============================================================================
# INTERFAZ PRINCIPAL DEL DASHBOARD
# ============================================================================
def main():
    """
    Función principal que renderiza el dashboard Streamlit.
    """
    
    # Título y descripción
    st.title("🚚 FleetLogix Dashboard Ejecutivo")
    st.markdown("---")
    st.markdown("""
    **Sistema de Monitoreo de Data Warehouse - FleetLogix**
    
    Este dashboard proporciona visualizaciones en tiempo real de las métricas clave
    del sistema de gestión de flota y logística.
    """)
    
    # Conexión a Snowflake
    engine = get_snowflake_connection()
    
    if engine is None:
        st.error("No se pudo conectar a Snowflake. Verifique la configuración en config/settings.ini")
        st.stop()
    
    # Sidebar con filtros
    st.sidebar.header("⚙️ Configuración")
    
    # Selector de rango de fechas
    date_range = st.sidebar.selectbox(
        "Rango de análisis",
        ["Últimos 7 días", "Últimos 30 días", "Últimos 90 días"],
        index=1
    )
    
    days_map = {"Últimos 7 días": 7, "Últimos 30 días": 30, "Últimos 90 días": 90}
    selected_days = days_map[date_range]
    
    # Auto-refresh
    auto_refresh = st.sidebar.checkbox("Auto-refresh (5 min)", value=False)
    
    if auto_refresh:
        st.rerun()
    
    # ============================================================================
    # KPIs PRINCIPALES
    # ============================================================================
    st.header("📊 KPIs Ejecutivos")
    
    kpis = get_kpi_metrics(engine)
    
    if kpis:
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                label="Total Entregas (30 días)",
                value=f"{kpis['total_deliveries']:,}",
                delta=f"{kpis['completion_rate']:.1f}% completadas"
            )
        
        with col2:
            st.metric(
                label="Tasa de Puntualidad",
                value=f"{kpis['on_time_rate']:.1f}%",
                delta="Objetivo: 95%",
                delta_color="inverse" if kpis['on_time_rate'] < 95 else "normal"
            )
        
        with col3:
            st.metric(
                label="Eficiencia Combustible",
                value=f"{kpis['avg_fuel_efficiency']:.1f} km/L",
                delta="Promedio histórico"
            )
        
        with col4:
            st.metric(
                label="Margen de Profit",
                value=f"{kpis['profit_margin']:.1f}%",
                delta=f"${kpis['total_revenue'] - kpis['total_cost']:,.0f}"
            )
    
    st.markdown("---")
    
    # ============================================================================
    # TENDENCIAS TEMPORALES
    # ============================================================================
    st.header("📈 Tendencias Temporales")
    
    trends_df = get_daily_trends(engine, days=selected_days)
    
    if not trends_df.empty:
        col1, col2 = st.columns(2)
        
        with col1:
            fig_deliveries = px.line(
                trends_df,
                x='date',
                y='deliveries',
                title='Entregas Diarias',
                labels={'deliveries': 'Cantidad', 'date': 'Fecha'},
                template='plotly_white'
            )
            fig_deliveries.update_layout(height=300)
            st.plotly_chart(fig_deliveries, use_container_width=True)
        
        with col2:
            fig_revenue = px.line(
                trends_df,
                x='date',
                y='daily_revenue',
                title='Ingresos Diarios',
                labels={'daily_revenue': 'Ingresos ($)', 'date': 'Fecha'},
                template='plotly_white'
            )
            fig_revenue.update_layout(height=300)
            st.plotly_chart(fig_revenue, use_container_width=True)
    
    st.markdown("---")
    
    # ============================================================================
    # RENDIMIENTO DE CONDUCTORES
    # ============================================================================
    st.header("👨‍✈️ Rendimiento de Conductores")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        metric_type = st.selectbox("Métrica", ["Eficiencia", "Puntualidad", "Ingresos"])
    
    metric_map = {"Eficiencia": "efficiency", "Puntualidad": "on_time", "Ingresos": "revenue"}
    performers_df = get_top_performers(engine, metric=metric_map[metric_type])
    
    if not performers_df.empty:
        with col2:
            st.dataframe(
                performers_df,
                column_config={
                    "name": "Conductor",
                    "metric_value": metric_type,
                    "deliveries": "Entregas"
                },
                hide_index=True,
                use_container_width=True
            )
    
    st.markdown("---")
    
    # ============================================================================
    # RENDIMIENTO DE RUTAS
    # ============================================================================
    st.header("🗺️ Rendimiento de Rutas")
    
    routes_df = get_route_performance(engine)
    
    if not routes_df.empty:
        fig_routes = px.bar(
            routes_df,
            x='route_name',
            y='total_revenue',
            color='avg_efficiency',
            title='Ingresos por Ruta (coloreado por eficiencia)',
            labels={'total_revenue': 'Ingresos ($)', 'route_name': 'Ruta', 'avg_efficiency': 'Eficiencia (km/L)'},
            template='plotly_white',
            color_continuous_scale='RdYlGn'
        )
        fig_routes.update_layout(height=400, xaxis_tickangle=-45)
        st.plotly_chart(fig_routes, use_container_width=True)
        
        with st.expander("Ver datos detallados de rutas"):
            st.dataframe(
                routes_df,
                column_config={
                    "route_code": "Código",
                    "route_name": "Ruta",
                    "deliveries": "Entregas",
                    "avg_delay": "Retraso Promedio (min)",
                    "avg_efficiency": "Eficiencia (km/L)",
                    "total_revenue": "Ingresos Totales ($)"
                },
                hide_index=True,
                use_container_width=True
            )
    
    st.markdown("---")
    
    # ============================================================================
    # ALERTAS DE CALIDAD DE DATOS
    # ============================================================================
    st.header("⚠️ Alertas de Calidad de Datos")
    
    alerts = get_data_quality_alerts(engine)
    
    for alert in alerts:
        if alert['severity'] == 'high':
            st.error(f"🔴 {alert['type']}: {alert['message']}")
        elif alert['severity'] == 'medium':
            st.warning(f"🟡 {alert['type']}: {alert['message']}")
        else:
            st.success(f"🟢 {alert['type']}: {alert['message']}")
    
    st.markdown("---")
    
    # ============================================================================
    # INFORMACIÓN DEL SISTEMA
    # ============================================================================
    st.header("ℹ️ Información del Sistema")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.info(f"""
        **Última actualización**
        {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        """)
    
    with col2:
        st.info(f"""
        **Rango de datos**
        Últimos {selected_days} días
        """)
    
    with col3:
        st.info(f"""
        **Estado de conexión**
        ✅ Conectado a Snowflake
        """)
    
    # Cerrar conexión
    engine.dispose()

if __name__ == "__main__":
    main()
