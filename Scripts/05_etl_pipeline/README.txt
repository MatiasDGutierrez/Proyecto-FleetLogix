=====================================================================================
FLEETLOGIX DATA WAREHOUSE - GUÍA RÁPIDA DE EJECUCIÓN
Autor: Matias Damian Gutierrez | Email: matiasgutierrez2502@gmail.com
=====================================================================================

ORDEN DE EJECUCIÓN (OBLIGATORIO):

1. CREAR MODELO DIMENSIONAL EN SNOWFLAKE
   - Archivo: sql/04_MA_dimensional_model_V1.1.sql
   - Ejecutar en Snowflake con permisos ACCOUNTADMIN
   - Crea: STAR_SCHEMA, FACT_DELIVERIES y 6 tablas dimensionales

2. CONFIGURAR CREDENCIALES
   - Archivo: config/settings.ini
   - Completar [postgres] y [snowflake] con credenciales válidas


3. INSTALAR DEPENDENCIAS
   - Ejecutar: pip install -r requirements.txt
   - Verificar Python 3.8 o superior

4. EJECUTAR PIPELINE ETL
   - Comando: python scripts/MA_main.py
   - Opciones:
     --test          (modo prueba con datos limitados)
     --limit 5000    (personalizar cantidad de registros)
     --verbose       (logging detallado)
   
5. VERIFICAR DATOS CARGADOS
   - Comando: python scripts/MA_snowflake_verify_3.py
   - Menú interactivo con 10 opciones de consulta
   - Incluye verificación de integridad automática

=====================================================================================
SOLUCIÓN RÁPIDA DE ERRORES COMUNES
=====================================================================================

ERROR: "Sección [postgres] no encontrada"
SOLUCIÓN: Verificar que settings.ini existe en /config y tiene formato correcto

ERROR: "Archivo no encontrado: staging_*.parquet"
SOLUCIÓN: Verificar conexión a PostgreSQL, ejecutar extracción primero

ERROR: "Snowflake connection failed"
SOLUCIÓN: 
  - Verificar formato de account (debe incluir región: xy12345.us-east-1)
  - Confirmar que warehouse está activo
  - Revisar permisos del usuario (mínimo ACCOUNTADMIN)

ERROR: "Table FACT_DELIVERIES does not exist"
SOLUCIÓN: Ejecutar script SQL (paso 1) antes del pipeline ETL

ERROR: "No module named 'snowflake'"
SOLUCIÓN: pip install snowflake-connector-python snowflake-sqlalchemy

ERROR: Pipeline se detiene en transformación
SOLUCIÓN: 
  - Revisar logs/etl_pipeline.log para detalles
  - Ejecutar con --test para validar con menos datos
  - Verificar memoria disponible del sistema

ERROR: "DELIVERY_ID duplicados"
SOLUCIÓN: El sistema usa UPSERT automático, los duplicados se actualizan

=====================================================================================
LOGS Y MONITOREO
=====================================================================================

Archivo de logs: logs/etl_pipeline.log (creado automáticamente)
Datos staging: data/staging/*.parquet (temporales)

Revisar logs si:
  - El pipeline falla sin mensaje claro
  - Los datos no aparecen en Snowflake
  - Hay problemas de rendimiento

=====================================================================================
VERIFICACIÓN POST-CARGA
=====================================================================================

Desde MA_snowflake_verify_3.py ejecutar:
  Opción 1: Ver estructura de FACT_DELIVERIES
  Opción 2: Primeros 10 registros
  Opción 7: Verificación completa de integridad

Consulta SQL rápida en Snowflake:
  SELECT COUNT(*) FROM STAR_SCHEMA.FACT_DELIVERIES;

=====================================================================================
ESTRUCTURA DE ARCHIVOS CRÍTICOS
=====================================================================================

config/settings.ini          ← Credenciales (NO subir a Git)
scripts/MA_main.py           ← Pipeline principal
scripts/MA_extract.py        ← Extracción PostgreSQL
scripts/MA_transform.py      ← Transformación datos
scripts/MA_load.py           ← Carga Snowflake
scripts/MA_snowflake_verify_3.py  ← Verificación
sql/04_MA_dimensional_model_V1.1.sql  ← Crear modelo

=====================================================================================
CONTACTO Y SOPORTE
=====================================================================================

Desarrollador: Matias Damian Gutierrez
Email: matiasgutierrez2502@gmail.com
Linkedin: www.linkedin.com/in/matias-gutierrez-6385a4238
Versión: 2.1

Para consultas técnicas o reporte de errores, contactar directamente.
Consultar manual completo adjunto para información detallada.

=====================================================================================