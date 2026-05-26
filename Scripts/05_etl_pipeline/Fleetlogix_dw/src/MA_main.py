"""
Script Principal de Pipeline ETL para FleetLogix Data Warehouse - CORREGIDO
Autor: Matias Damian Gutierrez
Descripción: Orquesta el proceso completo ETL desde PostgreSQL hasta Snowflake
Version: 2.1 - Parámetros corregidos
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
    """Ejecutar pipeline ETL completo: PostgreSQL -> Transform -> Snowflake - CORREGIDO"""
    
    logger.info("INICIANDO PIPELINE ETL COMPLETO")
    start_time = datetime.now()
    
    try:
        # Importar modulos con las funciones que sabemos funcionan
        from MA_extract import main as extract_main
        from MA_transform import transform_complete_pipeline
        from MA_load import load_complete_pipeline, verify_snowflake_connection, count_records_in_snowflake
        
        # FASE 1: VERIFICACIONES INICIALES - CORREGIDO
        logger.info("Fase 1: Verificaciones iniciales")
        
        # Verificar PostgreSQL ejecutando extraccion principal
        logger.info("  Verificando PostgreSQL...")
        try:
            # Ejecutar extraccion principal brevemente para verificar
            print("  Probando extraccion de datos...")
            
            # Buscar si ya hay archivos Parquet existentes
            staging_dir = '../data/staging'
            if os.path.exists(staging_dir):
                parquet_files = [f for f in os.listdir(staging_dir) if f.endswith('.parquet')]
                if parquet_files:
                    logger.info("  PostgreSQL OK - Archivos de datos encontrados")
                else:
                    # Si no hay archivos, verificar que podemos conectar
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
        
        # Verificar Snowflake
        logger.info("  Verificando Snowflake...")
        snowflake_ok = verify_snowflake_connection()
        if not snowflake_ok:
            logger.error("No se pudo conectar a Snowflake")
            return False
        logger.info("  Snowflake OK - Conexion verificada")
        
        logger.info("Todas las verificaciones pasaron")
        
        # FASE 2: EXTRACCION - CORREGIDO
        logger.info("Fase 2: Extraccion de PostgreSQL")
        try:
            # Ejecutar el script de extraccion completo
            logger.info("  Ejecutando extraccion principal...")
            extract_result = extract_main()
            
            if extract_result != 0:
                logger.error("Error en la extraccion de datos")
                return False
            
            # Buscar el archivo Parquet mas reciente generado
            staging_dir = '../data/staging'
            if not os.path.exists(staging_dir):
                logger.error(f"Directorio de staging no encontrado: {staging_dir}")
                return False
            
            parquet_files = [f for f in os.listdir(staging_dir) if f.endswith('.parquet')]
            if not parquet_files:
                logger.error("No se encontraron archivos Parquet en staging")
                return False
            
            # Tomar el archivo mas reciente
            latest_file = max(parquet_files)
            parquet_path = os.path.join(staging_dir, latest_file)
            logger.info(f"  Archivo a procesar: {latest_file}")
            
            # Cargar datos extraidos
            import pandas as pd
            raw_data = pd.read_parquet(parquet_path)
            
            if raw_data.empty:
                logger.error("No se pudieron cargar datos del archivo Parquet")
                return False
            
            logger.info(f"EXTRAIDOS {len(raw_data):,} registros de PostgreSQL")
            logger.info(f"  Vehiculos unicos: {raw_data['vehicle_id'].nunique()}")
            logger.info(f"  Conductores unicos: {raw_data['driver_id'].nunique()}")
            logger.info(f"  Rutas unicas: {raw_data['route_id'].nunique()}")
            
        except Exception as e:
            logger.error(f"Error en extraccion: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False
        
        # FASE 3: TRANSFORMACION - CORREGIDO
        logger.info("Fase 3: Transformacion de datos")
        try:
            # Usar el pipeline completo de transformacion probado
            transformed_data = transform_complete_pipeline(raw_data)
            
            if transformed_data.empty:
                logger.error("No hay datos despues de la transformacion")
                return False
            
            logger.info(f"TRANSFORMADOS {len(transformed_data):,} registros")
            logger.info(f"  Columnas finales: {len(transformed_data.columns)}")
            
            # Mostrar metricas clave
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
        
        # FASE 4: CARGA - CORREGIDO
        logger.info("Fase 4: Carga a Snowflake")
        try:
            # Contar registros antes de la carga
            records_before = count_records_in_snowflake()
            logger.info(f"  Registros en Snowflake antes de carga: {records_before:,}")
            
            # Realizar carga usando pipeline completo
            success = load_complete_pipeline(transformed_data)
            
            if success:
                # Contar registros despues de la carga
                records_after = count_records_in_snowflake()
                new_records = records_after - records_before
                
                # Calcular tiempo de ejecucion
                end_time = datetime.now()
                execution_time = (end_time - start_time).total_seconds()
                
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
    """Ejecutar pipeline en modo prueba - CORREGIDO"""
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
    """Funcion principal - CORREGIDA"""
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