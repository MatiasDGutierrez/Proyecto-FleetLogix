# Proyecto FleetLogix - Sistema de Gestión de Flota y Data Warehouse

Sistema completo de gestión de flota y logística con arquitectura de Data Warehouse, optimizado para análisis de datos operacionales y toma de decisiones estratégicas.

## 📊 Descripción del Proyecto

FleetLogix es un sistema integral que implementa un pipeline ETL completo desde una base de datos operacional (PostgreSQL) hacia un Data Warehouse en Snowflake, con capacidades avanzadas de análisis, forecasting y detección de anomalías.

### Características Principales

- **Pipeline ETL Robusto**: Extracción, transformación y carga de datos con estrategia UPSERT
- **Modelo Dimensional**: Esquema estrella optimizado para Snowflake con SCD Type 2
- **Dashboard Ejecutivo**: Visualización interactiva de KPIs y métricas clave
- **Forecasting de Demanda**: Predicción de demanda usando ARIMA y Machine Learning
- **Detección de Anomalías**: Identificación automática de patrones inusuales
- **Optimización SQL**: Sistema de benchmarking con 12 índices estratégicos

## 🏗️ Arquitectura del Sistema

```
┌─────────────────┐
│   PostgreSQL    │  (Base de Datos Operacional)
│   (Fuente)      │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Extracción     │  MA_extract.py
│  (Parquet)      │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Transformación  │  MA_transform.py
│ (Dimensional)   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Snowflake DW  │  (Data Warehouse)
│   (Destino)     │
└─────────────────┘
         │
         ▼
┌─────────────────┐
│   Dashboard     │  MA_dashboard.py
│   Forecasting   │  MA_forecasting.py
│   Anomalías     │  MA_anomaly_detection.py
└─────────────────┘
```

## 📁 Estructura del Proyecto

### Scripts Principales

```
Scripts/
├── 01_data_generation/          # Generación de datos sintéticos
│   ├── 01_data_generation_V2.1.py
│   └── MA_fleetlogix_core.sql
│
├── 02_03_queries_analysis_optimization_index_automatization/
│   └── MA_12Queries_Fleetlogix_db_8.sql  # 12 queries de negocio + benchmarking
│
├── 04_dimensional_model/
│   └── 04_MA_dimensional_model_V1.1.sql  # Modelo dimensional Snowflake
│
├── 05_etl_pipeline/
│   ├── Fleetlogix_dw/
│   │   ├── src/
│   │   │   ├── MA_main.py              # Orquestador del pipeline ETL
│   │   │   ├── MA_extract.py           # Extracción desde PostgreSQL
│   │   │   ├── MA_transform.py         # Transformación de datos
│   │   │   ├── MA_load.py              # Carga a Snowflake
│   │   │   ├── MA_dashboard.py         # Dashboard ejecutivo (NUEVO v2.2)
│   │   │   ├── MA_forecasting.py       # Forecasting de demanda (NUEVO v2.2)
│   │   │   └── MA_anomaly_detection.py # Detección de anomalías (NUEVO v2.2)
│   │   ├── config/
│   │   │   └── settings.ini            # Credenciales (NO subir a Git)
│   │   └── data/
│   │       └── staging/                # Archivos Parquet temporales
│   └── requirements.txt               # Dependencias Python
│
├── 06_aws_setup/
│   └── lambda_functions.py            # Funciones AWS Lambda
│
└── Documentacion/                     # Documentación técnica
    ├── AWS_Analisis_Arquitectura.pdf
    ├── Analisis_Snowfleck_ETL.pdf
    └── Manual_Consultas_SQL.pdf
```

## 🚀 Instalación y Configuración

### Prerrequisitos

- Python 3.8 o superior
- PostgreSQL (base de datos operacional)
- Snowflake (Data Warehouse)
- Credenciales de acceso configuradas

### Instalación de Dependencias

```bash
cd Scripts/05_etl_pipeline
pip install -r requirements.txt
```

### Configuración de Credenciales

Editar `Fleetlogix_dw/config/settings.ini`:

```ini
[postgres]
user = tu_usuario_postgres
password = tu_contraseña_postgres
host = tu_host_postgres
port = 5432
database = tu_database_postgres

[snowflake]
account = tu_account.us-east-1
user = tu_usuario_snowflake
password = tu_contraseña_snowflake
database = FLEETLOGIX_DW
schema = ANALYTICS
warehouse = FLEETLOGIX_WH
role = ACCOUNTADMIN
```

## 📋 Orden de Ejecución

### 1. Crear Modelo Dimensional en Snowflake

Ejecutar en Snowflake con permisos ACCOUNTADMIN:

```sql
-- Archivo: Scripts/04_dimensional_model/04_MA_dimensional_model_V1.1.sql
-- Crea: STAR_SCHEMA, FACT_DELIVERIES y 6 tablas dimensionales
```

### 2. Generar Datos Sintéticos (Opcional)

```bash
cd Scripts/01_data_generation
python 01_data_generation_V2.1.py
```

### 3. Ejecutar Pipeline ETL

```bash
cd Scripts/05_etl_pipeline/Fleetlogix_dw/src

# Modo producción (10,000 registros)
python MA_main.py

# Modo prueba (100 registros)
python MA_main.py --test

# Con límite personalizado
python MA_main.py --limit 50000

# Logging detallado
python MA_main.py --verbose
```

### 4. Ejecutar Dashboard Ejecutivo (NUEVO v2.2)

```bash
cd Scripts/05_etl_pipeline/Fleetlogix_dw/src
streamlit run MA_dashboard.py
```

El dashboard estará disponible en: `http://localhost:8501`

### 5. Usar Módulo de Forecasting (NUEVO v2.2)

```python
from MA_forecasting import DemandForecaster
from sqlalchemy import create_engine

# Conectar a Snowflake
engine = create_engine('snowflake://...')

# Crear forecaster
forecaster = DemandForecaster(engine)

# Cargar datos históricos
forecaster.load_historical_data(days=90)

# Generar forecast (ensemble de ARIMA + Random Forest)
result = forecaster.forecast_demand(days_ahead=7, method='ensemble')

# Ver resultados
print(result['forecast'])
```

### 6. Usar Módulo de Detección de Anomalías (NUEVO v2.2)

```python
from MA_anomaly_detection import AnomalyDetector

# Crear detector
detector = AnomalyDetector(engine)

# Generar reporte completo de anomalías
report = detector.generate_anomaly_report(days=30)

# Ver resumen
print(f"Total anomalías: {report['summary']['total_anomalies']}")
print(f"Estado: {report['summary']['analysis_status']}")
```

## 📊 Dashboard Ejecutivo (NUEVO v2.2)

El dashboard `MA_dashboard.py` proporciona visualizaciones en tiempo real de:

### KPIs Ejecutivos
- Total de entregas (30 días)
- Tasa de puntualidad
- Eficiencia de combustible
- Margen de profit

### Tendencias Temporales
- Entregas diarias
- Ingresos diarios
- Eficiencia promedio

### Rendimiento por Entidad
- Top conductores por eficiencia/puntualidad/ingresos
- Rendimiento de rutas
- Análisis de vehículos

### Alertas de Calidad de Datos
- Valores NULL en campos críticos
- Eficiencia de combustible anómala
- Entregas con retrasos extremos

## 🔮 Forecasting de Demanda (NUEVO v2.2)

El módulo `MA_forecasting.py` implementa:

### Modelos Disponibles
- **ARIMA**: Modelos AutoRegressive Integrated Moving Average para series temporales
- **Random Forest**: Regresión con múltiples features (día de semana, mes, etc.)
- **Ensemble**: Combinación de ambos modelos para mayor precisión

### Funcionalidades
- Descomposición estacional (tendencia, estacionalidad, residuo)
- Predicción de demanda diaria
- Detección de picos de demanda
- Análisis de patrones temporales

## 🚨 Detección de Anomalías (NUEVO v2.2)

El módulo `MA_anomaly_detection.py` implementa:

### Tipos de Anomalías Detectadas
- **Eficiencia de Combustible**: Isolation Forest para outliers multivariados
- **Rutas Anómalas**: Análisis estadístico de rendimiento por ruta
- **Conductores Atípicos**: DBSCAN clustering para comportamiento inusual
- **Tiempos de Entrega**: Z-score analysis para valores extremos

### Funcionalidades
- Detección automática de outliers
- Reporte consolidado de anomalías
- Alertas por severidad
- Análisis de patrones

## 📈 Optimización SQL

El script `MA_12Queries_Fleetlogix_db_8.sql` incluye:

### 12 Queries de Negocio
- **Básicas (1-3)**: Conteo de vehículos, licencias próximas a vencer, viajes por estado
- **Intermedias (4-8)**: Entregas por ciudad, conductores activos, consumo de combustible
- **Complejas (9-12)**: Costo de mantenimiento, ranking de conductores, tendencias

### Sistema de Benchmarking
- Ejecución sin índices vs con índices
- Captura de planes de ejecución EXPLAIN ANALYZE
- Métricas detalladas de performance
- Mejora del 63.6% en queries complejas

## 📚 Documentación

- `Documentacion/AWS_Analisis_Arquitectura.pdf`: Análisis de arquitectura AWS
- `Documentacion/Analisis_Snowfleck_ETL.pdf`: Análisis del pipeline ETL
- `Documentacion/Manual_Consultas_SQL.pdf`: Manual de consultas SQL
- `Scripts/05_etl_pipeline/README.txt`: Guía rápida de ejecución

## 🔧 Solución de Problemas

### Error: "Sección [postgres] no encontrada"
**Solución**: Verificar que `Fleetlogix_dw/config/settings.ini` existe y tiene el formato correcto

### Error: "Archivo no encontrado: staging_*.parquet"
**Solución**: Verificar conexión a PostgreSQL, ejecutar extracción primero

### Error: "Snowflake connection failed"
**Solución**: 
- Verificar formato de account (debe incluir región: tu_account.us-east-1)
- Confirmar que warehouse está activo
- Revisar permisos del usuario (mínimo ACCOUNTADMIN)

### Error: "No module named 'streamlit'"
**Solución**: `pip install streamlit plotly scikit-learn statsmodels`

## 🤝 Contribución

Este proyecto fue desarrollado por Matias Damian Gutierrez como parte del proyecto M2 de Ingeniería de Datos.

## 📄 Licencia

Proyecto académico para fines educativos.

## 📞 Contacto

- **Desarrollador**: Matias Damian Gutierrez
- **Email**: matiasgutierrez2502@gmail.com
- **LinkedIn**: www.linkedin.com/in/matias-gutierrez-6385a4238

## 🎯 Mejoras Implementadas (Versión 2.2)

Basado en feedback recibido, se implementaron las siguientes mejoras:

### ✅ Comentarios en Código
- Docstrings detallados con parámetros y valores de retorno
- Explicación de lógica de negocio en secciones complejas
- Ejemplos de uso para funciones principales
- Comentarios inline para clarificar código complejo

### ✅ Dashboard Ejecutivo
- Dashboard interactivo con Streamlit
- Visualización de KPIs en tiempo real
- Gráficos de tendencias temporales
- Análisis de rendimiento por entidad
- Alertas automatizadas de calidad de datos

### ✅ Análisis Predictivo
- Módulo de forecasting con ARIMA y Random Forest
- Descomposición estacional de series temporales
- Predicción de demanda diaria
- Ensemble de modelos para mayor precisión

### ✅ Detección de Anomalías
- Isolation Forest para outliers multivariados
- DBSCAN clustering para patrones inusuales
- Análisis estadístico de valores extremos
- Reporte consolidado de anomalías

### ✅ Dependencias Actualizadas
- Streamlit para dashboard interactivo
- Plotly para gráficos interactivos
- Scikit-learn para modelos de ML
- Statsmodels para series temporales

---

**Versión**: 2.2  
**Última Actualización**: Mayo 2026  
**Estado**: ✅ Producción con mejoras avanzadas
