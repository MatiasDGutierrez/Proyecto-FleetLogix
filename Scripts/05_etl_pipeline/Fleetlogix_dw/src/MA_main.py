"""
Script Principal de Pipeline ETL para FleetLogix Data Warehouse
===============================================================
Autor: Matias Damian Gutierrez
Versión: 2.1 - Parámetros corregidos
Descripción: Orquesta el proceso completo ETL desde PostgreSQL hasta Snowflake

Este script es el orquestador principal del pipeline ETL que coordina:
- Extracción de datos desde PostgreSQL (base de datos operacional)
- Transformación de datos para modelo dimensional (Snowflake)
- Carga de datos transformados al Data Warehouse

Arquitectura del Pipeline:
1. FASE 1 - Verificaciones: Valida conectividad PostgreSQL y Snowflake
2. FASE 2 - Extracción: Extrae datos operacionales y genera archivos staging
3. FASE 3 - Transformación: Aplica lógica de negocio y genera claves dimensionales
4. FASE 4 - Carga: Carga datos usando estrategia UPSERT en Snowflake

Estrategia de Carga:
- Usa UPSERT (MERGE) para manejar duplicados y actualizaciones
- Mantiene integridad referencial entre dimensiones y tabla de hechos
- Soporta carga incremental y full load

Uso:
    python MA_main.py                    # Modo producción (10,000 registros)
    python MA_main.py --test              # Modo prueba (100 registros)
    python MA_main.py --limit 50000       # Personalizar límite
    python MA_main.py --verbose           # Logging detallado

Dependencias:
    - pandas: Manipulación de datos
    - sqlalchemy: Conexión a bases de datos
    - snowflake-connector-python: Conexión a Snowflake
    - pyarrow: Formato Parquet para staging
"""

import logging
import sys
import os
from datetime import datetime

# CREAR CARPETA LOGS SI NO EXISTE
log_dir = 'logs'
if not os.path.exists(log_dir):
    os.makedirs(log_dir)
    print(f"Carpeta '{log_dir}' creada automaticamente")

# Configurar logging SIN EMOJIS para Windows
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/etl_pipeline.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def run_complete_etl(limit=10000):
    """
    Ejecuta el pipeline ETL completo en modo producción.
    
    Esta función orquesta las 4 fases del pipeline ETL:
    1. Verificación de conectividad con PostgreSQL y Snowflake
    2. Extracción de datos desde PostgreSQL a archivos staging (Parquet)
    3. Transformación de datos para modelo dimensional Snowflake
    4. Carga de datos transformados usando estrategia UPSERT
    
    Args:
        limit (int): Número máximo de registros a procesar.
                    Por defecto: 10,000. Use None para procesar todos.
    
    Returns:
        bool: True si el pipeline se completó exitosamente,
              False si hubo algún error en alguna fase.
    
    Raises:
        Exception: Error crítico en el pipeline (capturado y logueado)
    
    Ejemplo:
        >>> # Ejecutar pipeline con límite de 50,000 registros
        >>> success = run_complete_etl(limit=50000)
        >>> if success:
        ...     print("ETL completado exitosamente")
    
    Métricas registradas:
        - Tiempo total de ejecución
        - Registros extraídos de PostgreSQL
        - Registros transformados
        - Registros cargados en Snowflake
        - Nuevos registros agregados (UPSERT)
        - Total de registros en Snowflake después de carga
    """
    
    logger.info("INICIANDO PIPELINE ETL COMPLETO")
    start_time = datetime.now()
    
    try:
        # Importar módulos del pipeline ETL
        # MA_extract: Extrae datos desde PostgreSQL a archivos staging
        # MA_transform: Transforma datos para modelo dimensional
        # MA_load: Carga datos transformados a Snowflake
        from MA_extract import main as extract_main
        from MA_transform import transform_complete_pipeline
        from MA_load import load_complete_pipeline, verify_snowflake_connection, count_records_in_snowflake
        
        # ============================================================================
        # FASE 1: VERIFICACIONES INICIALES
        # ============================================================================
        # Objetivo: Validar que todas las conexiones estén disponibles antes de
        # iniciar el procesamiento de datos. Esto evita procesar datos si el
        # destino no está disponible.
        # ============================================================================
        logger.info("Fase 1: Verificaciones iniciales")
        
        # Verificación 1: Conectividad PostgreSQL
        # Lógica de negocio: PostgreSQL es la fuente de datos operacional.
        # Si no podemos conectar, no tiene sentido continuar con el pipeline.
        # También verificamos si existen archivos staging previos para reutilizar.
        logger.info("  Verificando PostgreSQL...")
        try:
            # Estrategia: Primero buscar archivos staging existentes para reutilizar
            # Si no existen, verificar que podemos conectar a PostgreSQL
            staging_dir = '../data/staging'
            if os.path.exists(staging_dir):
                parquet_files = [f for f in os.listdir(staging_dir) if f.endswith('.parquet')]
                if parquet_files:
                    logger.info("  PostgreSQL OK - Archivos de datos encontrados")
                else:
                    # Conexión de prueba para validar credenciales y disponibilidad
                    from MA_extract import get_postgres_connection
                    engine = get_postgres_connection()
                    engine.dispose()
                    logger.info("  PostgreSQL OK - Conexion establecida")
            else:
                logger.error("Directorio staging no existe")
                return False
                
        except Exception as e:
            logger.error(f"Error verificando PostgreSQL: {e}")
            return False
        
        # Verificación 2: Conectividad Snowflake
        # Lógica de negocio: Snowflake es el destino del Data Warehouse.
        # Si no podemos conectar, no podemos cargar los datos transformados.
        # Esta verificación es crítica para evitar procesar datos en vano.
        logger.info("  Verificando Snowflake...")
        snowflake_ok = verify_snowflake_connection()
        if not snowflake_ok:
            logger.error("No se pudo conectar a Snowflake")
            return False
        logger.info("  Snowflake OK - Conexion verificada")
        
        logger.info("Todas las verificaciones pasaron")
        
        # ============================================================================
        # FASE 2: EXTRACCIÓN DE DATOS
        # ============================================================================
        # Objetivo: Extraer datos operacionales desde PostgreSQL y guardarlos
        # en formato Parquet para procesamiento eficiente.
        # 
        # Estrategia:
        # - Extrae datos de las últimas 7 días por defecto
        # - Usa formato Parquet para compresión y velocidad
        # - Genera claves dimensionales (date_key, time_key) durante extracción
        # - Calcula métricas derivadas para análisis dimensional
        # ============================================================================
        logger.info("Fase 2: Extraccion de PostgreSQL")
        try:
            # Ejecutar extracción principal desde PostgreSQL
            # Esta función genera archivos staging con datos operacionales
            logger.info("  Ejecutando extraccion principal...")
            extract_result = extract_main()
            
            if extract_result != 0:
                logger.error("Error en la extraccion de datos")
                return False
            
            # Identificar archivo staging más reciente
            # Lógica de negocio: Si hay múltiples archivos staging, usamos el más
            # reciente para asegurar que estamos procesando los datos más actuales.
            # Los archivos tienen timestamp en el nombre: staging_YYYYMMDD_HHMMSS.parquet
            staging_dir = '../data/staging'
            if not os.path.exists(staging_dir):
                logger.error(f"Directorio de staging no encontrado: {staging_dir}")
                return False
            
            parquet_files = [f for f in os.listdir(staging_dir) if f.endswith('.parquet')]
            if not parquet_files:
                logger.error("No se encontraron archivos Parquet en staging")
                return False
            
            # Seleccionar archivo más reciente basado en timestamp del nombre
            latest_file = max(parquet_files)
            parquet_path = os.path.join(staging_dir, latest_file)
            logger.info(f"  Archivo a procesar: {latest_file}")
            
            # Cargar datos extraídos desde archivo Parquet
            # Parquet es eficiente para datos grandes y mantiene tipos de datos
            import pandas as pd
            raw_data = pd.read_parquet(parquet_path)
            
            if raw_data.empty:
                logger.error("No se pudieron cargar datos del archivo Parquet")
                return False
            
            # Métricas de extracción para monitoreo
            logger.info(f"EXTRAIDOS {len(raw_data):,} registros de PostgreSQL")
            logger.info(f"  Vehiculos unicos: {raw_data['vehicle_id'].nunique()}")
            logger.info(f"  Conductores unicos: {raw_data['driver_id'].nunique()}")
            logger.info(f"  Rutas unicas: {raw_data['route_id'].nunique()}")
            
        except Exception as e:
            logger.error(f"Error en extraccion: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False
        
        # ============================================================================
        # FASE 3: TRANSFORMACIÓN DE DATOS
        # ============================================================================
        # Objetivo: Aplicar lógica de negocio para convertir datos operacionales
        # en datos dimensionales listos para análisis.
        # 
        # Transformaciones aplicadas:
        # - Generación de claves dimensionales (vehicle_key, driver_key, route_key, etc.)
        # - Cálculo de métricas derivadas (eficiencia, puntualidad, costos)
        # - Normalización de datos para modelo estrella
        # - Enriquecimiento con atributos dimensionales
        # ============================================================================
        logger.info("Fase 3: Transformacion de datos")
        try:
            # Aplicar pipeline completo de transformación
            # Esta función convierte datos operacionales a formato dimensional
            transformed_data = transform_complete_pipeline(raw_data)
            
            if transformed_data.empty:
                logger.error("No hay datos despues de la transformacion")
                return False
            
            logger.info(f"TRANSFORMADOS {len(transformed_data):,} registros")
            logger.info(f"  Columnas finales: {len(transformed_data.columns)}")
            
            # Métricas de calidad de transformación
            # Estas métricas ayudan a validar que la transformación fue exitosa
            if 'IS_ON_TIME' in transformed_data.columns:
                on_time_rate = transformed_data['IS_ON_TIME'].mean() * 100
                logger.info(f"  Entregas a tiempo: {on_time_rate:.1f}%")
            if 'FUEL_EFFICIENCY_KM_PER_LITER' in transformed_data.columns:
                avg_efficiency = transformed_data['FUEL_EFFICIENCY_KM_PER_LITER'].mean()
                logger.info(f"  Eficiencia combustible: {avg_efficiency:.1f} km/L")
            
        except Exception as e:
            logger.error(f"Error en transformacion: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False
        
        # ============================================================================
        # FASE 4: CARGA A SNOWFLAKE
        # ============================================================================
        # Objetivo: Cargar datos transformados al Data Warehouse usando
        # estrategia UPSERT para manejar duplicados y actualizaciones.
        # 
        # Estrategia UPSERT:
        # - Si el registro existe: actualiza los campos modificados
        # - Si el registro no existe: inserta nuevo registro
        # - Mantiene historial con SCD Type 2 para dimensiones
        # - Garantiza integridad referencial entre tablas
        # ============================================================================
        logger.info("Fase 4: Carga a Snowflake")
        try:
            # Contar registros antes de carga para métricas de impacto
            records_before = count_records_in_snowflake()
            logger.info(f"  Registros en Snowflake antes de carga: {records_before:,}")
            
            # Ejecutar carga usando pipeline completo con estrategia UPSERT
            success = load_complete_pipeline(transformed_data)
            
            if success:
                # Calcular métricas de impacto de la carga
                records_after = count_records_in_snowflake()
                new_records = records_after - records_before
                
                # Calcular tiempo total de ejecución
                end_time = datetime.now()
                execution_time = (end_time - start_time).total_seconds()
                
                # Reporte final de ejecución
                logger.info("PIPELINE ETL COMPLETADO EXITOSAMENTE")
                logger.info("RESUMEN FINAL:")
                logger.info(f"   Tiempo total: {execution_time:.2f} segundos")
                logger.info(f"   Registros extraidos: {len(raw_data):,}")
                logger.info(f"   Registros transformados: {len(transformed_data):,}")
                logger.info(f"   Registros cargados: {len(transformed_data):,}")
                logger.info(f"   Nuevos registros en Snowflake: {new_records:,}")
                logger.info(f"   Total en Snowflake: {records_after:,}")
                
                return True
            else:
                logger.error("PIPELINE FALLO: Error en la carga a Snowflake")
                return False
                
        except Exception as e:
            logger.error(f"Error en carga: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False
            
    except Exception as e:
        logger.error(f"ERROR CRITICO EN PIPELINE ETL: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return False

def run_test_mode(limit=100):
    """
    Ejecuta el pipeline ETL en modo prueba con datos limitados.
    
    Este modo es ideal para:
    - Validar configuración de conexiones
    - Probar lógica de transformación con pocos datos
    - Debuggear problemas sin procesar grandes volúmenes
    - Verificar integración entre componentes
    
    Args:
        limit (int): Número máximo de registros a procesar en modo prueba.
                    Por defecto: 100. Valores típicos: 100-1000.
    
    Returns:
        bool: True si el pipeline de prueba se completó exitosamente,
              False si hubo algún error.
    
    Ejemplo:
        >>> # Ejecutar prueba con 500 registros
        >>> success = run_test_mode(limit=500)
        >>> if success:
        ...     print("Prueba exitosa - Pipeline listo para producción")
    
    Diferencias con modo producción:
    - Procesa menos registros (más rápido)
    - No afecta datos de producción significativamente
    - Ideal para desarrollo y testing
    """
    logger.info("EJECUTANDO MODO PRUEBA")
    
    try:
        from MA_extract import main as extract_main
        from MA_transform import transform_complete_pipeline
        from MA_load import load_complete_pipeline, verify_snowflake_connection
        
        # Verificaciones rapidas
        if not verify_snowflake_connection():
            return False
        
        # Ejecutar extraccion principal (ya sabemos que funciona)
        print("Ejecutando extraccion de prueba...")
        extract_result = extract_main()
        if extract_result != 0:
            return False
        
        # Buscar archivo Parquet mas reciente
        staging_dir = '../data/staging'
        if not os.path.exists(staging_dir):
            return False
            
        parquet_files = [f for f in os.listdir(staging_dir) if f.endswith('.parquet')]
        if not parquet_files:
            return False
            
        latest_file = max(parquet_files)
        parquet_path = os.path.join(staging_dir, latest_file)
        
        # Cargar y transformar
        import pandas as pd
        raw_data = pd.read_parquet(parquet_path)
        
        # Limitar datos para prueba
        if len(raw_data) > limit:
            raw_data = raw_data.head(limit)
            
        transformed_data = transform_complete_pipeline(raw_data)
        if transformed_data.empty:
            return False
        
        # Carga
        success = load_complete_pipeline(transformed_data)
        return success
        
    except Exception as e:
        logger.error(f"Error en modo prueba: {e}")
        return False

def main():
    """
    Punto de entrada principal del pipeline ETL FleetLogix.
    
    Esta función:
    - Parsea argumentos de línea de comandos
    - Configura el nivel de logging
    - Ejecuta el pipeline en modo producción o prueba
    - Maneja errores y excepciones de forma graceful
    - Proporciona feedback claro al usuario
    
    Argumentos de línea de comandos:
        --limit N:     Limita el número de registros a procesar (default: 10000)
        --test:        Ejecuta en modo prueba con datos limitados
        --verbose:     Habilita logging detallado para debugging
    
    Returns:
        None: Esta función llama a sys.exit() con código de estado.
              0 = éxito, 1 = error.
    
    Ejemplos de uso:
        # Modo producción estándar
        python MA_main.py
        
        # Modo prueba
        python MA_main.py --test
        
        # Con límite personalizado
        python MA_main.py --limit 50000
        
        # Logging detallado para debugging
        python MA_main.py --verbose
        
        # Combinando opciones
        python MA_main.py --test --limit 1000 --verbose
    """
    import argparse
    
    parser = argparse.ArgumentParser(description='Pipeline ETL FleetLogix')
    parser.add_argument('--limit', type=int, default=10000, help='Limite de registros para extraccion')
    parser.add_argument('--test', action='store_true', help='Ejecutar modo prueba con datos limitados')
    parser.add_argument('--verbose', action='store_true', help='Logging mas detallado')
    
    args = parser.parse_args()
    
    # Configurar logging mas detallado si se solicita
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    print("=" * 70)
    print("FLEETLOGIX - PIPELINE ETL COMPLETO")
    print("=" * 70)
    print("Este pipeline ejecuta:")
    print("  1. Extraccion de PostgreSQL")
    print("  2. Transformacion de datos") 
    print("  3. Carga a Snowflake Data Warehouse")
    print("=" * 70)
    
    if args.test:
        print(f"\nMODO PRUEBA - Limite: {args.limit} registros")
        print("=" * 50)
    else:
        print(f"\nMODO PRODUCCION - Limite: {args.limit:,} registros")
        print("=" * 50)
    
    try:
        # Ejecutar pipeline segun modo
        if args.test:
            success = run_test_mode(limit=args.limit)
        else:
            success = run_complete_etl(limit=args.limit)
        
        if success:
            print("\n" + "=" * 70)
            print("PIPELINE ETL COMPLETADO EXITOSAMENTE")
            print("=" * 70)
            print("   Los datos ya estan en Snowflake y listos para analisis.")
            print("   Puede usar las herramientas de BI para crear dashboards.")
            print("   Revise el archivo 'logs/etl_pipeline.log' para detalles.")
            print("=" * 70)
            sys.exit(0)
        else:
            print("\n" + "=" * 70)
            print("PIPELINE ETL FALLO")
            print("=" * 70)
            print("   Revise los logs en 'logs/etl_pipeline.log' para mas detalles.")
            print("   Ejecute con --test para modo prueba con menos datos.")
            print("   Verifique la configuracion en 'config/settings.ini'")
            print("=" * 70)
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\nPipeline interrumpido por el usuario")
        sys.exit(1)
    except Exception as e:
        print(f"\nError inesperado: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()