"""
Script de Extracción para Data Warehouse en Snowflake - FleetLogix ETL Pipeline
OPTIMIZADO para modelo estrella y procesamiento dimensional
Autor: Matias Damian Gutierrez
Version: 2.7: Extrae datos desde PostgreSQL (fuente) hacia archivos staging para Snowflake
MEJORAS APLICADAS:
1. Extracción incremental optimizada por fechas
2. Generación de claves dimensionales (date_key, time_key)
3. Cálculo de métricas derivadas para análisis
4. Formato Parquet para eficiencia en Snowflake
5. Validación de conectividad dual (PostgreSQL y Snowflake)
6. Procesamiento de múltiples días con consolidación
"""

import pandas as pd
import configparser
import os
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import warnings
from snowflake.sqlalchemy import URL

# ------------------------------
# Configuración inicial
# ------------------------------
warnings.filterwarnings('ignore', message='.*pandas only supports SQLAlchemy.*')

# ------------------------------
# Conexión a PostgreSQL (fuente de datos)
# ------------------------------
def get_postgres_connection():
    """
    Establece conexión con PostgreSQL usando SQLAlchemy.
    
    Returns:
        engine: Motor de SQLAlchemy para PostgreSQL
        
    Raises:
        FileNotFoundError: Si no se encuentra el archivo de configuración
        KeyError: Si falta la sección postgres en la configuración
    """
    config = configparser.ConfigParser()
    
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_dir)
    config_path = os.path.join(project_root, 'config', 'settings.ini')
    
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Archivo no encontrado: {config_path}")
    
    config.read(config_path)
    
    # Buscar sección postgres (case insensitive)
    postgres_section = None
    for section in config.sections():
        if section.lower() == 'postgres':
            postgres_section = section
            break
    
    if not postgres_section:
        raise KeyError("Sección [postgres] no encontrada en configuración")
    
    try:
        connection_string = (
            f"postgresql://{config[postgres_section]['user']}:"
            f"{config[postgres_section]['password']}@"
            f"{config[postgres_section]['host']}:"
            f"{config[postgres_section].get('port', '5432')}/"
            f"{config[postgres_section]['database']}"
        )
        engine = create_engine(connection_string, pool_pre_ping=True)
        print("Conexión PostgreSQL establecida")
        return engine
    except Exception as e:
        print(f"Error conectando a PostgreSQL: {e}")
        raise

# ------------------------------
# Conexión a Snowflake (destino)
# ------------------------------
def get_snowflake_connection():
    """
    Crea conexión a Snowflake (data warehouse destino).
    
    Returns:
        engine: Motor de SQLAlchemy para Snowflake
        
    Raises:
        FileNotFoundError: Si no se encuentra el archivo de configuración
        KeyError: Si falta la sección snowflake en la configuración
    """
    config = configparser.ConfigParser()
    
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_dir)
    config_path = os.path.join(project_root, 'config', 'settings.ini')
    
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Archivo no encontrado: {config_path}")
    
    config.read(config_path)
    
    # Buscar sección snowflake (case insensitive)
    snowflake_section = None
    for section in config.sections():
        if section.lower() == 'snowflake':
            snowflake_section = section
            break
    
    if not snowflake_section:
        print("Secciones disponibles en settings.ini:", config.sections())
        raise KeyError("Sección [snowflake] no encontrada en configuración")
    
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
        print("Conexión Snowflake establecida")
        return engine
    except Exception as e:
        print(f"Error conectando a Snowflake: {e}")
        raise

# ------------------------------
# Verificación de conectividad Snowflake
# ------------------------------
def check_snowflake_connectivity():
    """
    Verifica la conectividad con Snowflake y lista las tablas disponibles.
    
    Returns:
        bool: True si la conexión es exitosa, False en caso contrario
    """
    try:
        engine = get_snowflake_connection()
        
        # Consulta simple para verificar conexión
        test_query = "SELECT CURRENT_TIMESTAMP AS current_time, CURRENT_VERSION() AS version"
        result = pd.read_sql(test_query, engine)
        print(f"Snowflake conectado - Versión: {result.iloc[0]['version']}")
        
        # Verificar tablas existentes de manera segura
        try:
            check_query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'STAR_SCHEMA' 
            AND table_type = 'BASE TABLE'
            ORDER BY table_name
            """
            
            tables = pd.read_sql(check_query, engine)
            print("Tablas en Snowflake:")
            if len(tables) > 0:
                for _, row in tables.iterrows():
                    print(f"  - {row['table_name']}")
            else:
                print("  - No hay tablas creadas (esto es normal en la primera ejecución)")
        except Exception as table_error:
            print(f"No se pudieron listar tablas: {table_error}")
        
        engine.dispose()
        return True
        
    except Exception as e:
        print(f"Error conectando a Snowflake: {e}")
        return False

# ------------------------------
# Búsqueda de fechas disponibles
# ------------------------------
def find_available_dates_in_postgres():
    """
    Busca todas las fechas con datos disponibles en PostgreSQL.
    Lista las últimas 10 fechas con conteo de registros.
    
    Returns:
        DataFrame: Fechas disponibles con conteo de registros, o None si no hay datos
    """
    engine = get_postgres_connection()
    
    query = """
    SELECT 
        DATE(t.departure_datetime) as date,
        COUNT(*) as records
    FROM trips t
    INNER JOIN deliveries d ON t.trip_id = d.trip_id
    WHERE t.departure_datetime IS NOT NULL
        AND d.delivery_status IS NOT NULL
    GROUP BY DATE(t.departure_datetime)
    ORDER BY date DESC
    LIMIT 10
    """
    
    try:
        result = pd.read_sql(query, engine)
        if len(result) > 0:
            print("Fechas disponibles en PostgreSQL:")
            for _, row in result.iterrows():
                days_ago = (datetime.now().date() - row['date']).days
                print(f"  - {row['date']}: {row['records']:,} registros ({days_ago} días atrás)")
            return result
        else:
            print("No se encontraron fechas con datos en PostgreSQL")
            return None
    except Exception as e:
        print(f"Error buscando fechas: {e}")
        return None
    finally:
        engine.dispose()

# ------------------------------
# Extracción de datos por fecha
# ------------------------------
def extract_data_by_date(target_date, limit=5000):
    """
    Extrae datos completos para una fecha específica con todas las dimensiones y métricas.
    
    Args:
        target_date: Fecha objetivo en formato YYYY-MM-DD
        limit: Número máximo de registros a extraer
        
    Returns:
        DataFrame: Datos extraídos con claves dimensionales calculadas, o None si no hay datos
    """
    engine = get_postgres_connection()
    
    print(f"Extrayendo datos para: {target_date}")
    
    query = """
    SELECT 
        d.delivery_id,
        d.trip_id,
        d.tracking_number,
        d.customer_id,
        t.vehicle_id,
        t.driver_id,
        t.route_id,
        d.scheduled_datetime,
        d.delivered_datetime,
        t.departure_datetime,
        t.arrival_datetime,
        d.package_weight_kg,
        d.distance_km as delivery_distance_km,
        d.fuel_consumed_liters as delivery_fuel_consumed,
        d.delivery_time_minutes,
        d.delay_minutes,
        d.deliveries_per_hour,
        d.fuel_efficiency_km_per_liter,
        d.cost_per_delivery,
        d.revenue_per_delivery,
        d.is_on_time,
        d.is_damaged,
        d.recipient_signature as has_signature,
        d.delivery_status,
        t.fuel_consumed_liters as trip_fuel_consumed,
        t.total_weight_kg as trip_total_weight,
        t.status as trip_status,
        v.license_plate,
        v.vehicle_type,
        v.capacity_kg,
        v.fuel_type,
        v.acquisition_date,
        v.status as vehicle_status,
        dr.employee_code,
        dr.first_name,
        dr.last_name,
        dr.license_number,
        dr.license_expiry,
        dr.phone as driver_phone,
        dr.hire_date,
        dr.status as driver_status,
        dr.performance_category,
        r.route_code,
        r.origin_city,
        r.destination_city,
        r.distance_km as route_distance_km,
        r.estimated_duration_hours,
        r.toll_cost,
        r.difficulty_level,
        r.route_type,
        c.customer_name,
        c.customer_type,
        c.city as customer_city,
        c.first_delivery_date,
        c.total_deliveries,
        c.customer_category,
        CURRENT_TIMESTAMP as extracted_at
    FROM deliveries d
    INNER JOIN trips t ON d.trip_id = t.trip_id
    INNER JOIN vehicles v ON t.vehicle_id = v.vehicle_id
    INNER JOIN drivers dr ON t.driver_id = dr.driver_id
    INNER JOIN routes r ON t.route_id = r.route_id
    INNER JOIN customers c ON d.customer_id = c.customer_id
    WHERE DATE(t.departure_datetime) = %(target_date)s
        AND t.departure_datetime IS NOT NULL
        AND d.delivery_status IS NOT NULL
    ORDER BY t.departure_datetime DESC
    LIMIT %(limit)s
    """
    
    try:
        df = pd.read_sql(query, engine, params={
            'target_date': target_date,
            'limit': limit
        })
        
        if len(df) > 0:
            print(f"Extraídos {len(df):,} registros del {target_date}")
            
            # Generar claves para dimensiones temporales
            df['scheduled_time_key'] = (
                pd.to_datetime(df['scheduled_datetime']).dt.hour * 100 +
                pd.to_datetime(df['scheduled_datetime']).dt.minute
            ).fillna(0).astype(int)
            
            df['delivered_time_key'] = (
                pd.to_datetime(df['delivered_datetime']).dt.hour * 100 +
                pd.to_datetime(df['delivered_datetime']).dt.minute
            ).fillna(0).astype(int)
            
            df['date_key'] = (
                pd.to_datetime(df['scheduled_datetime']).dt.year * 10000 +
                pd.to_datetime(df['scheduled_datetime']).dt.month * 100 +
                pd.to_datetime(df['scheduled_datetime']).dt.day
            ).fillna(0).astype(int)
            
            # Calcular métricas derivadas para análisis dimensional
            df['vehicle_age_months'] = (
                (pd.to_datetime('today') - pd.to_datetime(df['acquisition_date'])).dt.days / 30
            ).round(0).fillna(0).astype(int)
            
            df['driver_experience_months'] = (
                (pd.to_datetime('today') - pd.to_datetime(df['hire_date'])).dt.days / 30
            ).round(0).fillna(0).astype(int)
            
            print(f"  - Vehículos: {df['vehicle_id'].nunique()}")
            print(f"  - Conductores: {df['driver_id'].nunique()}")
            print(f"  - Rutas: {df['route_id'].nunique()}")
            print(f"  - Clientes: {df['customer_id'].nunique()}")
            
            return df
        else:
            print(f"No hay datos para {target_date}")
            return None
        
    except Exception as e:
        print(f"Error en extracción: {e}")
        return None
    finally:
        engine.dispose()

# ------------------------------
# Guardado en formato Parquet
# ------------------------------
def save_to_parquet(df, output_dir='../data/staging'):
    """
    Guarda datos en formato Parquet optimizado para carga en Snowflake.
    
    Args:
        df: DataFrame con los datos a guardar
        output_dir: Directorio de salida para archivos staging
        
    Returns:
        str: Ruta completa del archivo generado
    """
    os.makedirs(output_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filepath = os.path.join(output_dir, f'staging_{timestamp}.parquet')
    
    df.to_parquet(filepath, index=False, engine='pyarrow')
    print(f"Datos guardados en: {filepath}")
    
    return filepath

# ------------------------------
# Función principal
# ------------------------------
def main():
    """
    Función principal que orquesta el proceso de extracción completo.
    Extrae datos de los últimos 7 días y los consolida en un archivo Parquet.
    
    Returns:
        int: Código de salida (0 = éxito, 1 = error)
    """
    print("=" * 70)
    print("EXTRACCIÓN PARA SNOWFLAKE - FLEETLOGIX ETL")
    print("=" * 70)
    
    # Verificar Snowflake primero
    if not check_snowflake_connectivity():
        print("No se puede conectar a Snowflake")
        return 1
    
    # Buscar fechas disponibles en PostgreSQL
    available_dates = find_available_dates_in_postgres()
    
    if available_dates is None or len(available_dates) == 0:
        print("No se encontraron datos en PostgreSQL")
        return 1
    
    # Tomar las 7 fechas más recientes
    recent_dates = available_dates.head(7)
    
    print(f"\nExtrayendo datos de las últimas {len(recent_dates)} fechas:")
    all_data = []
    
    for _, row in recent_dates.iterrows():
        target_date = row['date']
        records_count = row['records']
        
        print(f"  - {target_date}: {records_count:,} registros")
        
        # Extraer datos para cada fecha
        df = extract_data_by_date(target_date=target_date, limit=10000)
        
        if df is not None and len(df) > 0:
            all_data.append(df)
            print(f"    Datos extraídos: {len(df):,} registros")
        else:
            print(f"    Sin datos para {target_date}")
    
    # Combinar todos los datos
    if not all_data:
        print("No se pudieron extraer datos para ninguna fecha")
        return 1
    
    combined_df = pd.concat(all_data, ignore_index=True)
    
    print(f"\nTOTAL combinado: {len(combined_df):,} registros de {len(recent_dates)} días")
    
    # Guardar en formato optimizado
    filepath = save_to_parquet(combined_df)
    
    # Resumen final
    print("\n" + "=" * 70)
    print("RESUMEN EXTRACCIÓN SNOWFLAKE - 7 DÍAS")
    print("=" * 70)
    print(f"Rango de fechas:        {recent_dates.iloc[-1]['date']} a {recent_dates.iloc[0]['date']}")
    print(f"Días procesados:        {len(recent_dates)}")
    print(f"Registros totales:      {len(combined_df):,}")
    print(f"Archivo staging:        {filepath}")
    print(f"Formato:                Parquet")
    print(f"Tamaño estimado:        {combined_df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")
    print(f"Vehículos únicos:       {combined_df['vehicle_id'].nunique()}")
    print(f"Conductores únicos:     {combined_df['driver_id'].nunique()}")
    print(f"Rutas únicas:           {combined_df['route_id'].nunique()}")
    print(f"Clientes únicos:        {combined_df['customer_id'].nunique()}")
    print(f"Entregas completadas:   {(combined_df['delivery_status'] == 'delivered').sum():,}")
    print("=" * 70)
    
    # Muestra de datos
    print("\nMuestra de datos:")
    sample_cols = ['delivery_id', 'customer_name', 'vehicle_type', 
                  'delivery_status', 'date_key', 'scheduled_time_key']
    print(combined_df[sample_cols].head(3).to_string(index=False))
    
    print(f"\nExtracción completada. Archivo listo para transformación: {filepath}")
    return 0

if __name__ == "__main__":
    exit(main())