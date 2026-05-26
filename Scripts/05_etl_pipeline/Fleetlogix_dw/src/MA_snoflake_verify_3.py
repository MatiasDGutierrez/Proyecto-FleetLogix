"""
Script de Consultas Personalizadas para Snowflake Data Warehouse - FleetLogix
OPTIMIZADO con detección automática de columnas y consultas adaptativas
Autor: Matias Damian Gutierrez
Version 3: Herramienta interactiva para consultas SQL en Snowflake con menú guiado
MEJORAS APLICADAS:
1. Detección automática de estructura de tablas y columnas disponibles
2. Consultas adaptativas que se ajustan al esquema existente
3. Modo SQL interactivo para consultas personalizadas
4. Validación de integridad de datos integrada
5. Análisis dimensional completo (vehículos, conductores, rutas)
6. Protección automática con límites de registros
"""

import pandas as pd
import configparser
import os
from sqlalchemy import create_engine, text
from snowflake.sqlalchemy import URL

# ------------------------------
# Información del proyecto
# ------------------------------
_AUTHOR_INFO = {
    "author": "Matias Damian Gutierrez",
    "email": "matiasgutierrez2502@gmail.com", 
    "version": "3.0",
    "copyright": "2025",
    "license": "Uso educativo - Prohibida distribucion sin autorizacion",
    "project": "FleetLogix Snowflake Query Tool"
}

# ------------------------------
# Verificación de autoría
# ------------------------------
def _verify_authorship():
    """
    Verifica y protege la autoría del código.
    Muestra información del autor y versión.
    
    Returns:
        bool: True si la verificación es exitosa
    """
    try:
        print(f"Verificando autoria: {_AUTHOR_INFO['author']}")
        print(f"Version: {_AUTHOR_INFO['version']}")
        print(f"Proyecto: {_AUTHOR_INFO['project']}")
        return True
    except Exception as e:
        print(f"Error en verificacion de autoria: {e}")
        return True

def _display_copyright():
    """Muestra información de copyright y contacto."""
    print("=" * 80)
    print("CONSULTAS SNOWFLAKE - FLEETLOGIX DATA WAREHOUSE - V 3.0")
    print("Desarrollado por: Matias Damian Gutierrez")
    print("Contacto: matiasgutierrez2502@gmail.com")
    print("Copyright (c) 2025 - Todos los derechos reservados")
    print("Licencia: Uso educativo - Prohibida distribucion sin autorizacion")
    print("=" * 80)

# ------------------------------
# Conexión a Snowflake
# ------------------------------
def get_snowflake_connection():
    """
    Establece conexión con Snowflake usando configuración del archivo settings.ini.
    
    Returns:
        engine: Motor de SQLAlchemy configurado para Snowflake
        
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
    
    snowflake_section = None
    for section in config.sections():
        if section.lower() == 'snowflake':
            snowflake_section = section
            break
    
    if not snowflake_section:
        raise KeyError("Sección [snowflake] no encontrada")
    
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
        print(f"Error conectando a Snowflake: {e}")
        raise

# ------------------------------
# Detección de estructura de tablas
# ------------------------------
def get_available_columns(table_name='FACT_DELIVERIES', schema='STAR_SCHEMA'):
    """
    Obtiene la lista de columnas disponibles en una tabla de Snowflake.
    Permite consultas adaptativas basadas en el esquema real.
    
    Args:
        table_name: Nombre de la tabla a consultar
        schema: Esquema de la base de datos
        
    Returns:
        list: Lista de nombres de columnas en mayúsculas
    """
    engine = get_snowflake_connection()
    try:
        query = f"""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = '{table_name}' 
        AND table_schema = '{schema}'
        ORDER BY column_name
        """
        
        columns_df = pd.read_sql(text(query), engine)
        return columns_df['column_name'].str.upper().tolist()
    except Exception as e:
        print(f"Error obteniendo columnas: {e}")
        return []
    finally:
        engine.dispose()

def column_exists(column_name, available_columns):
    """
    Verifica si una columna existe en la lista de columnas disponibles.
    
    Args:
        column_name: Nombre de la columna a verificar
        available_columns: Lista de columnas disponibles
        
    Returns:
        bool: True si la columna existe
    """
    return column_name.upper() in available_columns

# ------------------------------
# Ejecución de consultas
# ------------------------------
def run_custom_query(query, limit=1000):
    """
    Ejecuta una consulta SQL personalizada en Snowflake con protección de límite.
    
    Args:
        query: Consulta SQL a ejecutar
        limit: Límite máximo de registros a retornar
        
    Returns:
        DataFrame: Resultados de la consulta, o None si hay error
    """
    engine = get_snowflake_connection()
    
    try:
        # Agregar LIMIT si no está presente (para seguridad)
        original_query = query
        if "LIMIT" not in query.upper():
            query += f" LIMIT {limit}"
            
        print(f"\nEjecutando consulta...")
        result = pd.read_sql(text(query), engine)
        
        print(f"\nResultados ({len(result)} registros):")
        print("=" * 80)
        if len(result) > 0:
            print(result.to_string(index=False))
        else:
            print("No se encontraron resultados")
        print("=" * 80)
        
        return result
        
    except Exception as e:
        print(f"Error en consulta: {e}")
        return None
    finally:
        engine.dispose()

# ------------------------------
# Menú principal
# ------------------------------
def show_main_menu():
    """Muestra el menú principal de opciones de consulta."""
    print("\n" + "="*80)
    print("CONSULTAS SNOWFLAKE BY MATIAS GUTIERREZ - FLEETLOGIX DATA WAREHOUSE ")
    print("VERSION ADAPTATIVA - Detecta automaticamente columnas disponibles")
    print("="*80)
    print("1.  Ver estructura de la tabla FACT_DELIVERIES")
    print("2.  Consulta rapida - Primeros 10 registros")
    print("3.  Entregas por fecha")
    print("4.  Eficiencia de combustible por vehiculo")
    print("5.  Rendimiento de conductores")
    print("6.  Metricas de negocio principales")
    print("7.  Verificacion de integridad de datos")
    print("8.  Analisis de rutas")
    print("9.  Consulta personalizada")
    print("10. Modo SQL interactivo")
    print("0.  Salir")
    print("="*80)

# ------------------------------
# Opción 1: Estructura de tabla
# ------------------------------
def option_1_structure():
    """
    Muestra la estructura completa de la tabla FACT_DELIVERIES.
    Incluye nombres de columnas, tipos de datos y restricciones.
    """
    print("\n" + "="*80)
    print("1. ESTRUCTURA DE FACT_DELIVERIES")
    print("="*80)
    
    engine = get_snowflake_connection()
    try:
        query = """
        SELECT 
            column_name,
            data_type,
            is_nullable,
            character_maximum_length
        FROM information_schema.columns 
        WHERE table_name = 'FACT_DELIVERIES'
        ORDER BY ordinal_position
        """
        
        result = pd.read_sql(text(query), engine)
        print(f"Tabla FACT_DELIVERIES - {len(result)} columnas encontradas")
        print("\nEstructura:")
        print("-" * 60)
        for _, row in result.iterrows():
            null_info = "NULL" if row['is_nullable'] == 'YES' else "NOT NULL"
            length_info = f"({row['character_maximum_length']})" if row['character_maximum_length'] else ""
            print(f"  {row['column_name']:30} {row['data_type']}{length_info:15} {null_info}")
            
    except Exception as e:
        print(f"Error: {e}")
    finally:
        engine.dispose()

# ------------------------------
# Opción 2: Consulta rápida
# ------------------------------
def option_2_quick_query():
    """
    Ejecuta una consulta rápida de los primeros 10 registros.
    Detecta automáticamente las columnas disponibles para mostrar.
    """
    print("\n" + "="*80)
    print("2. CONSULTA RAPIDA - PRIMEROS 10 REGISTROS")
    print("="*80)
    
    available_columns = get_available_columns()
    
    # Seleccionar columnas básicas que probablemente existen
    select_columns = ["DELIVERY_ID", "VEHICLE_ID", "DRIVER_ID", "DATE_KEY", "DELIVERY_STATUS"]
    
    # Agregar columnas adicionales si existen
    additional_columns = ["IS_ON_TIME", "DELIVERY_TIME_MINUTES", "DELIVERY_DURATION_MINUTES", 
                         "FUEL_EFFICIENCY_KM_PER_LITER", "REVENUE_PER_DELIVERY"]
    
    for col in additional_columns:
        if column_exists(col, available_columns):
            select_columns.append(col)
    
    columns_str = ", ".join(select_columns)
    
    query = f"""
    SELECT 
        {columns_str}
    FROM FACT_DELIVERIES
    ORDER BY DATE_KEY DESC, DELIVERY_ID DESC
    LIMIT 10
    """
    run_custom_query(query)

# ------------------------------
# Opción 3: Entregas por fecha
# ------------------------------
def option_3_deliveries_by_date():
    """
    Analiza entregas agrupadas por fecha con métricas de puntualidad.
    Versión adaptativa que incluye métricas según columnas disponibles.
    """
    print("\n" + "="*80)
    print("3. ENTREGAS POR FECHA")
    print("="*80)
    
    available_columns = get_available_columns()
    
    query = """
    SELECT 
        DATE_KEY,
        COUNT(*) as total_entregas,
        SUM(CASE WHEN IS_ON_TIME = TRUE THEN 1 ELSE 0 END) as entregas_a_tiempo,
        ROUND(entregas_a_tiempo * 100.0 / COUNT(*), 1) as tasa_puntualidad
    """
    
    # Agregar métricas adicionales si existen
    if column_exists("DELIVERY_DURATION_MINUTES", available_columns):
        query += ", ROUND(AVG(DELIVERY_DURATION_MINUTES), 1) as duracion_promedio"
    elif column_exists("DELIVERY_TIME_MINUTES", available_columns):
        query += ", ROUND(AVG(DELIVERY_TIME_MINUTES), 1) as duracion_promedio"
    
    if column_exists("FUEL_EFFICIENCY_KM_PER_LITER", available_columns):
        query += ", ROUND(AVG(FUEL_EFFICIENCY_KM_PER_LITER), 1) as eficiencia_promedio"
    
    query += """
    FROM FACT_DELIVERIES
    GROUP BY DATE_KEY
    ORDER BY DATE_KEY DESC
    """
    run_custom_query(query)

# ------------------------------
# Opción 4: Eficiencia de combustible
# ------------------------------
def option_4_fuel_efficiency():
    """
    Analiza la eficiencia de combustible por vehículo.
    Incluye métricas de productividad y distancia recorrida.
    """
    print("\n" + "="*80)
    print("4. EFICIENCIA DE COMBUSTIBLE POR VEHICULO")
    print("="*80)
    
    available_columns = get_available_columns()
    
    query = """
    SELECT 
        VEHICLE_ID,
        COUNT(*) as total_viajes
    """
    
    # Agregar métricas de eficiencia si existen
    if column_exists("FUEL_EFFICIENCY_KM_PER_LITER", available_columns):
        query += ", ROUND(AVG(FUEL_EFFICIENCY_KM_PER_LITER), 2) as eficiencia_combustible"
    
    if column_exists("DELIVERIES_PER_HOUR", available_columns):
        query += ", ROUND(AVG(DELIVERIES_PER_HOUR), 2) as productividad"
    
    if column_exists("DELIVERY_DISTANCE_KM", available_columns):
        query += ", ROUND(SUM(DELIVERY_DISTANCE_KM), 1) as distancia_total"
    
    if column_exists("DELIVERY_FUEL_CONSUMED", available_columns):
        query += ", ROUND(SUM(DELIVERY_FUEL_CONSUMED), 1) as combustible_total"
    elif column_exists("FUEL_CONSUMED_LITERS", available_columns):
        query += ", ROUND(SUM(FUEL_CONSUMED_LITERS), 1) as combustible_total"
    
    query += """
    FROM FACT_DELIVERIES
    GROUP BY VEHICLE_ID
    HAVING COUNT(*) >= 1
    ORDER BY total_viajes DESC
    LIMIT 15
    """
    run_custom_query(query)

# ------------------------------
# Opción 5: Rendimiento de conductores
# ------------------------------
def option_5_driver_performance():
    """
    Analiza el rendimiento de conductores con métricas de puntualidad.
    Incluye duración promedio y productividad por conductor.
    """
    print("\n" + "="*80)
    print("5. RENDIMIENTO DE CONDUCTORES")
    print("="*80)
    
    available_columns = get_available_columns()
    
    query = """
    SELECT 
        DRIVER_ID,
        COUNT(*) as total_entregas,
        SUM(CASE WHEN IS_ON_TIME = TRUE THEN 1 ELSE 0 END) as entregas_a_tiempo,
        ROUND(entregas_a_tiempo * 100.0 / COUNT(*), 1) as tasa_puntualidad
    """
    
    # Agregar métricas de rendimiento si existen
    if column_exists("DELIVERY_DURATION_MINUTES", available_columns):
        query += ", ROUND(AVG(DELIVERY_DURATION_MINUTES), 1) as duracion_promedio"
    elif column_exists("DELIVERY_TIME_MINUTES", available_columns):
        query += ", ROUND(AVG(DELIVERY_TIME_MINUTES), 1) as duracion_promedio"
    
    if column_exists("DELIVERIES_PER_HOUR", available_columns):
        query += ", ROUND(AVG(DELIVERIES_PER_HOUR), 2) as productividad"
    
    query += """
    FROM FACT_DELIVERIES
    GROUP BY DRIVER_ID
    HAVING COUNT(*) >= 1
    ORDER BY tasa_puntualidad DESC
    LIMIT 15
    """
    run_custom_query(query)

# ------------------------------
# Opción 6: Métricas de negocio
# ------------------------------
def option_6_business_metrics():
    """
    Muestra métricas principales de negocio.
    Incluye ingresos, costos y rentabilidad total.
    """
    print("\n" + "="*80)
    print("6. METRICAS DE NEGOCIO PRINCIPALES")
    print("="*80)
    
    available_columns = get_available_columns()
    
    query = """
    SELECT 
        COUNT(*) as total_entregas,
        SUM(CASE WHEN IS_ON_TIME = TRUE THEN 1 ELSE 0 END) as entregas_a_tiempo,
        ROUND(entregas_a_tiempo * 100.0 / COUNT(*), 1) as tasa_puntualidad
    """
    
    # Agregar métricas adicionales si existen
    if column_exists("DELIVERY_DURATION_MINUTES", available_columns):
        query += ", ROUND(AVG(DELIVERY_DURATION_MINUTES), 1) as duracion_promedio"
    
    if column_exists("FUEL_EFFICIENCY_KM_PER_LITER", available_columns):
        query += ", ROUND(AVG(FUEL_EFFICIENCY_KM_PER_LITER), 2) as eficiencia_promedio"
    
    if column_exists("REVENUE_PER_DELIVERY", available_columns):
        query += ", ROUND(SUM(REVENUE_PER_DELIVERY), 2) as ingresos_totales"
    
    if column_exists("COST_PER_DELIVERY", available_columns):
        query += ", ROUND(SUM(COST_PER_DELIVERY), 2) as costos_totales"
    
    # Calcular rentabilidad si ambas columnas existen
    if (column_exists("REVENUE_PER_DELIVERY", available_columns) and 
        column_exists("COST_PER_DELIVERY", available_columns)):
        query += ", ROUND(SUM(REVENUE_PER_DELIVERY - COST_PER_DELIVERY), 2) as rentabilidad_total"
    
    query += """
    FROM FACT_DELIVERIES
    """
    run_custom_query(query)

# ------------------------------
# Opción 7: Verificación de integridad
# ------------------------------
def option_7_data_integrity():
    """
    Ejecuta múltiples verificaciones de integridad de datos.
    Detecta valores nulos, duplicados y valida rangos de fechas.
    """
    print("\n" + "="*80)
    print("7. VERIFICACION DE INTEGRIDAD DE DATOS")
    print("="*80)
    
    available_columns = get_available_columns()
    
    queries = [
        ("Total de registros", "SELECT COUNT(*) as total_registros FROM FACT_DELIVERIES"),
        ("Registros con valores nulos", f"""
            SELECT COUNT(*) as con_nulos 
            FROM FACT_DELIVERIES 
            WHERE DELIVERY_ID IS NULL OR VEHICLE_ID IS NULL OR DRIVER_ID IS NULL
        """),
        ("Duplicados en DELIVERY_ID", """
            SELECT COUNT(*) as duplicados
            FROM (
                SELECT DELIVERY_ID, COUNT(*) as cnt
                FROM FACT_DELIVERIES 
                GROUP BY DELIVERY_ID 
                HAVING COUNT(*) > 1
            )
        """),
        ("Rango de fechas", "SELECT MIN(DATE_KEY) as fecha_min, MAX(DATE_KEY) as fecha_max FROM FACT_DELIVERIES")
    ]
    
    # Agregar verificación de estados de entrega si existe la columna
    if column_exists("DELIVERY_STATUS", available_columns):
        queries.append(("Estados de entrega", """
            SELECT DELIVERY_STATUS, COUNT(*) as cantidad
            FROM FACT_DELIVERIES
            GROUP BY DELIVERY_STATUS
        """))
    
    for check_name, query in queries:
        print(f"\n{check_name}:")
        result = run_custom_query(query, limit=10000)
        if result is not None and len(result) > 0:
            for col, value in result.iloc[0].items():
                print(f"  {col}: {value}")

# ------------------------------
# Opción 8: Análisis de rutas
# ------------------------------
def option_8_route_analysis():
    """
    Analiza métricas por ruta incluyendo distancia, eficiencia y costos.
    Versión adaptativa según columnas disponibles.
    """
    print("\n" + "="*80)
    print("8. ANALISIS DE RUTAS")
    print("="*80)
    
    available_columns = get_available_columns()
    
    query = """
    SELECT 
        ROUTE_ID,
        COUNT(*) as total_viajes,
        ROUND(AVG(DELIVERY_DISTANCE_KM), 1) as distancia_promedio,
        SUM(CASE WHEN IS_ON_TIME = TRUE THEN 1 ELSE 0 END) as entregas_a_tiempo,
        ROUND(entregas_a_tiempo * 100.0 / COUNT(*), 1) as tasa_puntualidad
    """
    
    # Agregar métricas adicionales si existen
    if column_exists("DELIVERY_DURATION_MINUTES", available_columns):
        query += ", ROUND(AVG(DELIVERY_DURATION_MINUTES), 1) as tiempo_promedio"
    
    if column_exists("FUEL_EFFICIENCY_KM_PER_LITER", available_columns):
        query += ", ROUND(AVG(FUEL_EFFICIENCY_KM_PER_LITER), 2) as eficiencia_promedio"
    
    if column_exists("TOLL_COST", available_columns):
        query += ", ROUND(AVG(TOLL_COST), 2) as costo_peaje_promedio"
    
    if column_exists("COST_PER_DELIVERY", available_columns):
        query += ", ROUND(AVG(COST_PER_DELIVERY), 2) as costo_promedio"
    
    if column_exists("REVENUE_PER_DELIVERY", available_columns):
        query += ", ROUND(AVG(REVENUE_PER_DELIVERY), 2) as ingreso_promedio"
    
    query += """
    FROM FACT_DELIVERIES
    GROUP BY ROUTE_ID
    ORDER BY total_viajes DESC
    LIMIT 15
    """
    run_custom_query(query)

# ------------------------------
# Opción 9: Consulta personalizada
# ------------------------------
def option_9_custom_query():
    """
    Permite al usuario ingresar una consulta SQL personalizada.
    Automáticamente añade límite de seguridad.
    """
    print("\n" + "="*80)
    print("9. CONSULTA PERSONALIZADA")
    print("="*80)
    print("Escriba su consulta SQL (se agregara automaticamente LIMIT 1000 por seguridad):")
    print("Ejemplo: SELECT * FROM FACT_DELIVERIES WHERE IS_ON_TIME = FALSE")
    print("-" * 80)
    
    query = input("SQL: ").strip()
    if query:
        run_custom_query(query)
    else:
        print("Consulta vacia. Volviendo al menu principal.")

# ------------------------------
# Opción 10: Modo SQL interactivo
# ------------------------------
def option_10_interactive_sql():
    """
    Modo interactivo que permite ejecutar múltiples consultas SQL.
    El usuario puede escribir consultas hasta que escriba 'back' para salir.
    """
    print("\n" + "="*80)
    print("10. MODO SQL INTERACTIVO")
    print("="*80)
    print("Escriba consultas SQL (escriba 'back' para volver al menu principal)")
    print("="*80)
    
    while True:
        try:
            query = input("\nSQL> ").strip()
            
            if query.lower() in ['back', 'exit', 'quit', 'salir', 'volver', '0']:
                print("Volviendo al menu principal...")
                break
                
            if not query:
                continue
                
            run_custom_query(query)
            
        except KeyboardInterrupt:
            print("\nVolviendo al menu principal...")
            break
        except Exception as e:
            print(f"Error: {e}")

# ------------------------------
# Función principal
# ------------------------------
def main():
    """
    Función principal con menú interactivo.
    Coordina la navegación entre las diferentes opciones de consulta.
    """
    # Mostrar información de copyright y verificar autoría
    _display_copyright()
    _verify_authorship()
    print("\nConectando a Snowflake...")
    
    # Verificar conexión al inicio
    try:
        engine = get_snowflake_connection()
        engine.dispose()
        print("Conexion establecida correctamente")
    except Exception as e:
        print(f"Error de conexion: {e}")
        return
    
    # Mapeo de opciones
    options = {
        '1': option_1_structure,
        '2': option_2_quick_query,
        '3': option_3_deliveries_by_date,
        '4': option_4_fuel_efficiency,
        '5': option_5_driver_performance,
        '6': option_6_business_metrics,
        '7': option_7_data_integrity,
        '8': option_8_route_analysis,
        '9': option_9_custom_query,
        '10': option_10_interactive_sql
    }
    
    while True:
        show_main_menu()
        choice = input("\nSeleccione una opcion (0-10): ").strip()
        
        if choice == '0':
            print("\n" + "="*80)
            print("Gracias por usar el consultor Snowflake de FleetLogix!")
            print("Desarrollado por: Matias Gutierrez - matiasgutierrez2502@gmail.com")
            print("="*80)
            break
        
        if choice in options:
            try:
                options[choice]()
                input("\nPresione Enter para continuar...")
            except Exception as e:
                print(f"Error ejecutando opcion {choice}: {e}")
                input("\nPresione Enter para continuar...")
        else:
            print("Opcion no valida. Por favor seleccione 0-10.")
            input("\nPresione Enter para continuar...")

if __name__ == "__main__":
    main()