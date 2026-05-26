-- ============================================================================
-- SCRIPT DE IMPLEMENTACIÓN DATA WAREHOUSE FLEETLOGIX - V 1.1
-- ============================================================================
-- Script: Implementación Completa de Data Warehouse Dimensional
-- Base de Datos: Snowflake Cloud Data Warehouse
-- Autor: Matias Gutierrez
-- Versión: 1.1 (Modelo Estrella Optimizado)
-- Descripción: Implementa modelo dimensional completo para análisis de logística
--              y gestión de flota, optimizado para Snowflake Cloud Data Warehouse.
-- Características Principales:
--   - Modelo estrella optimizado para Snowflake
--   - Dimensiones SCD Type 2 para seguimiento histórico
--   - Vistas seguras por roles de negocio
--   - Configuración de Time Travel para recuperación de datos
--   - Estructura preparada para procesos ETL/ELT
-- ============================================================================

-- ============================================================================
-- SECCIÓN 1: SCRIPT DE LIMPIEZA - ELIMINACIÓN DE DATOS EXISTENTES
-- ============================================================================

-- Configurar contexto administrativo para operaciones de limpieza
USE ROLE ACCOUNTADMIN;
USE DATABASE FLEETLOGIX_DW;
USE SCHEMA ANALYTICS;

-- Eliminar vistas seguras primero (dependencias de tablas)
DROP VIEW IF EXISTS v_sales_deliveries;
DROP VIEW IF EXISTS v_operations_deliveries;

-- Eliminar roles de negocio (requiere remoción previa de permisos)
DROP ROLE IF EXISTS SALES_ANALYST;
DROP ROLE IF EXISTS OPERATIONS_ANALYST;

-- Eliminar tablas de hechos (dependen de las dimensiones)
DROP TABLE IF EXISTS fact_deliveries;
DROP TABLE IF EXISTS staging_daily_load;

-- Eliminar tablas de dimensiones en orden adecuado
DROP TABLE IF EXISTS dim_date;
DROP TABLE IF EXISTS dim_time;
DROP TABLE IF EXISTS dim_vehicle;
DROP TABLE IF EXISTS dim_driver;
DROP TABLE IF EXISTS dim_route;
DROP TABLE IF EXISTS dim_customer;

-- Comandos opcionales para limpieza completa (descomentar si es necesario)
-- DROP SCHEMA IF EXISTS ANALYTICS;
-- DROP DATABASE IF EXISTS FLEETLOGIX_DW;
-- DROP WAREHOUSE IF EXISTS FLEETLOGIX_WH;

-- ============================================================================
-- SECCIÓN 2: CREACIÓN DE INFRAESTRUCTURA SNOWFLAKE
-- ============================================================================

-- Crear warehouse para procesamiento con configuración optimizada
USE ROLE ACCOUNTADMIN;
CREATE WAREHOUSE IF NOT EXISTS FLEETLOGIX_WH 
WITH 
    WAREHOUSE_SIZE = 'XSMALL' 
    AUTO_SUSPEND = 60 
    AUTO_RESUME = TRUE;

-- Crear database principal para el data warehouse
CREATE DATABASE IF NOT EXISTS FLEETLOGIX_DW;

-- Configurar contexto de trabajo
USE DATABASE FLEETLOGIX_DW;

-- Crear esquema analytics para modelos dimensionales
CREATE SCHEMA IF NOT EXISTS ANALYTICS;

-- Establecer esquema por defecto para siguientes operaciones
USE SCHEMA ANALYTICS;

-- ============================================================================
-- SECCIÓN 3: IMPLEMENTACIÓN DE DIMENSIONES
-- ============================================================================

-- Dimensión Fecha: Tabla de dimensiones para análisis temporal granular
-- Incluye atributos calendario estándar y fiscal
CREATE OR REPLACE TABLE dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE NOT NULL,
    day_of_week INT,
    day_name VARCHAR(10),
    day_of_month INT,
    day_of_year INT,
    week_of_year INT,
    month_num INT,
    month_name VARCHAR(10),
    quarter INT,
    year INT,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN,
    holiday_name VARCHAR(50),
    fiscal_quarter INT,
    fiscal_year INT
);

-- Dimensión Tiempo: Tabla para análisis horario y por turnos
-- Optimizada para análisis de eficiencia operativa por hora del día
CREATE OR REPLACE TABLE dim_time (
    time_key INT PRIMARY KEY,
    hour INT,
    minute INT,
    second INT,
    time_of_day VARCHAR(20),  -- Categorías: 'Madrugada', 'Mañana', 'Tarde', 'Noche'
    hour_24 VARCHAR(5),       -- Formato 24 horas: '14:30'
    hour_12 VARCHAR(8),       -- Formato 12 horas: '02:30 PM'
    am_pm VARCHAR(2),
    is_business_hour BOOLEAN,
    shift VARCHAR(20)         -- Turnos: 'Turno 1', 'Turno 2', 'Turno 3'
);

-- Dimensión Vehículo: Implementa SCD Type 2 para seguimiento histórico
-- Permite rastrear cambios en el estado y características de vehículos
CREATE OR REPLACE TABLE dim_vehicle (
    vehicle_key INT PRIMARY KEY,
    vehicle_id INT NOT NULL,
    license_plate VARCHAR(20),
    vehicle_type VARCHAR(50),
    capacity_kg DECIMAL(10,2),
    fuel_type VARCHAR(20),
    acquisition_date DATE,
    age_months INT,
    status VARCHAR(20),
    last_maintenance_date DATE,
    valid_from DATE,          -- Fecha inicio vigencia (SCD Type 2)
    valid_to DATE,            -- Fecha fin vigencia (SCD Type 2)
    is_current BOOLEAN        -- Indicador de versión actual (SCD Type 2)
);

-- Dimensión Conductor: Implementa SCD Type 2 para historial de conductores
-- Incluye métricas de performance y estado de documentación
CREATE OR REPLACE TABLE dim_driver (
    driver_key INT PRIMARY KEY,
    driver_id INT NOT NULL,
    employee_code VARCHAR(20),
    full_name VARCHAR(200),
    license_number VARCHAR(50),
    license_expiry DATE,
    phone VARCHAR(20),
    hire_date DATE,
    experience_months INT,
    status VARCHAR(20),
    performance_category VARCHAR(20),  -- Categorías: 'Alto', 'Medio', 'Bajo'
    valid_from DATE,                   -- Fecha inicio vigencia (SCD Type 2)
    valid_to DATE,                     -- Fecha fin vigencia (SCD Type 2)
    is_current BOOLEAN                 -- Indicador de versión actual (SCD Type 2)
);

-- Dimensión Ruta: Tabla de dimensiones para análisis de rutas y trayectos
-- Incluye métricas de dificultad y tipo para análisis de performance
CREATE OR REPLACE TABLE dim_route (
    route_key INT PRIMARY KEY,
    route_id INT NOT NULL,
    route_code VARCHAR(20),
    origin_city VARCHAR(100),
    destination_city VARCHAR(100),
    distance_km DECIMAL(10,2),
    estimated_duration_hours DECIMAL(5,2),
    toll_cost DECIMAL(10,2),
    difficulty_level VARCHAR(20),  -- Niveles: 'Fácil', 'Medio', 'Difícil'
    route_type VARCHAR(20)         -- Tipos: 'Urbana', 'Interurbana', 'Rural'
);

-- Dimensión Cliente: Tabla de dimensiones para análisis de clientes
-- Clasifica clientes por tipo y categoría para segmentación
CREATE OR REPLACE TABLE dim_customer (
    customer_key INT PRIMARY KEY,
    customer_id INT IDENTITY,
    customer_name VARCHAR(200),
    customer_type VARCHAR(50),     -- Tipos: 'Individual', 'Empresa', 'Gobierno'
    city VARCHAR(100),
    first_delivery_date DATE,
    total_deliveries INT,
    customer_category VARCHAR(20)  -- Categorías: 'Premium', 'Regular', 'Ocasional'
);

-- ============================================================================
-- SECCIÓN 4: IMPLEMENTACIÓN DE TABLA DE HECHOS PRINCIPAL
-- ============================================================================

-- Tabla de Hechos Entregas: Corazón del modelo dimensional
-- Contiene métricas de negocio y relaciones con todas las dimensiones
CREATE OR REPLACE TABLE fact_deliveries (
    -- Claves primarias y foráneas
    delivery_key INT IDENTITY PRIMARY KEY,
    date_key INT REFERENCES dim_date(date_key),
    scheduled_time_key INT REFERENCES dim_time(time_key),
    delivered_time_key INT REFERENCES dim_time(time_key),
    vehicle_key INT REFERENCES dim_vehicle(vehicle_key),
    driver_key INT REFERENCES dim_driver(driver_key),
    route_key INT REFERENCES dim_route(route_key),
    customer_key INT REFERENCES dim_customer(customer_key),
    
    -- Dimensiones degeneradas (atributos originales preservados)
    delivery_id INT NOT NULL,
    trip_id INT NOT NULL,
    tracking_number VARCHAR(50),
    
    -- Métricas cuantitativas de operaciones
    package_weight_kg DECIMAL(10,2),
    distance_km DECIMAL(10,2),
    fuel_consumed_liters DECIMAL(10,2),
    delivery_time_minutes INT,
    delay_minutes INT,
    
    -- Métricas calculadas para análisis de eficiencia
    deliveries_per_hour DECIMAL(5,2),
    fuel_efficiency_km_per_liter DECIMAL(5,2),
    cost_per_delivery DECIMAL(10,2),
    revenue_per_delivery DECIMAL(10,2),
    
    -- Indicadores booleanos para análisis de calidad
    is_on_time BOOLEAN,
    is_damaged BOOLEAN,
    has_signature BOOLEAN,
    delivery_status VARCHAR(20),
    
    -- Campos de auditoría y trazabilidad ETL
    etl_batch_id INT,
    etl_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================================
-- SECCIÓN 5: CONFIGURACIÓN AVANZADA SNOWFLAKE
-- ============================================================================

-- Configurar Time Travel para recuperación de datos (30 días de retención)
-- Permite recuperación de datos accidentalmente eliminados o modificados
ALTER TABLE fact_deliveries SET DATA_RETENTION_TIME_IN_DAYS = 30;
ALTER TABLE dim_date SET DATA_RETENTION_TIME_IN_DAYS = 30;
ALTER TABLE dim_vehicle SET DATA_RETENTION_TIME_IN_DAYS = 30;
ALTER TABLE dim_driver SET DATA_RETENTION_TIME_IN_DAYS = 30;
ALTER TABLE dim_route SET DATA_RETENTION_TIME_IN_DAYS = 30;
ALTER TABLE dim_customer SET DATA_RETENTION_TIME_IN_DAYS = 30;
ALTER TABLE dim_time SET DATA_RETENTION_TIME_IN_DAYS = 30;

-- Crear tabla de staging para procesos ETL/ELT
-- Almacena datos en formato VARIANT para flexibilidad en ingesta
CREATE OR REPLACE TABLE staging_daily_load (
    raw_data VARIANT,
    load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================================
-- SECCIÓN 6: IMPLEMENTACIÓN DE VISTAS SEGURAS Y ROLES
-- ============================================================================

-- Vista Segura para Analistas de Ventas: Acceso restringido a datos comerciales
-- Filtra información sensible y muestra solo datos relevantes para ventas
CREATE OR REPLACE SECURE VIEW v_sales_deliveries AS
SELECT 
    d.full_date,
    c.customer_name,
    c.customer_type,
    f.package_weight_kg,
    f.delivery_status,
    f.revenue_per_delivery
FROM fact_deliveries f
JOIN dim_date d ON f.date_key = d.date_key
JOIN dim_customer c ON f.customer_key = c.customer_key
WHERE c.customer_type != 'Gobierno';  -- Restricción de seguridad empresarial

-- Vista Segura para Analistas de Operaciones: Acceso completo a datos operativos
-- Proporciona visión integral de la eficiencia operativa
CREATE OR REPLACE SECURE VIEW v_operations_deliveries AS
SELECT 
    d.full_date,
    t.hour_24 as hora,
    v.license_plate,
    dr.full_name as conductor,
    r.route_code,
    c.customer_name,
    f.delivery_time_minutes,
    f.delay_minutes,
    f.is_on_time,
    f.fuel_consumed_liters
FROM fact_deliveries f
JOIN dim_date d ON f.date_key = d.date_key
JOIN dim_time t ON f.scheduled_time_key = t.time_key
JOIN dim_vehicle v ON f.vehicle_key = v.vehicle_key
JOIN dim_driver dr ON f.driver_key = dr.driver_key
JOIN dim_route r ON f.route_key = r.route_key
JOIN dim_customer c ON f.customer_key = c.customer_key;

-- Crear roles de negocio para gestión de accesos
CREATE ROLE IF NOT EXISTS SALES_ANALYST;
CREATE ROLE IF NOT EXISTS OPERATIONS_ANALYST;

-- Asignar permisos de lectura a vistas seguras según roles
GRANT SELECT ON VIEW v_sales_deliveries TO ROLE SALES_ANALYST;
GRANT SELECT ON VIEW v_operations_deliveries TO ROLE OPERATIONS_ANALYST;

-- ============================================================================
-- SECCIÓN 7: VERIFICACIÓN COMPLETA DE IMPLEMENTACIÓN
-- ============================================================================

-- Verificación de creación de objetos del Data Warehouse
-- Confirma que todos los componentes se hayan creado exitosamente
SELECT 'WAREHOUSE' as object_type, COUNT(*) as count 
FROM INFORMATION_SCHEMA.OBJECT_PRIVILEGES 
WHERE OBJECT_NAME = 'FLEETLOGIX_WH'
UNION ALL
SELECT 'DATABASE', COUNT(*) 
FROM INFORMATION_SCHEMA.DATABASES 
WHERE DATABASE_NAME = 'FLEETLOGIX_DW'
UNION ALL
SELECT 'SCHEMA', COUNT(*) 
FROM INFORMATION_SCHEMA.SCHEMATA 
WHERE SCHEMA_NAME = 'ANALYTICS' AND CATALOG_NAME = 'FLEETLOGIX_DW'
UNION ALL
SELECT 'TABLES', COUNT(*) 
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA = 'ANALYTICS' AND TABLE_CATALOG = 'FLEETLOGIX_DW'
UNION ALL
SELECT 'VIEWS', COUNT(*) 
FROM INFORMATION_SCHEMA.VIEWS 
WHERE TABLE_SCHEMA = 'ANALYTICS' AND TABLE_CATALOG = 'FLEETLOGIX_DW'
UNION ALL
SELECT 'ROLES', COUNT(*) 
FROM INFORMATION_SCHEMA.APPLICABLE_ROLES 
WHERE ROLE_NAME IN ('SALES_ANALYST', 'OPERATIONS_ANALYST');

-- Análisis detallado de estructura de tablas creadas
-- Proporciona metadata sobre columnas y tipos de datos
SELECT 
    TABLE_NAME,
    COUNT(*) as column_count,
    SUM(CASE WHEN DATA_TYPE IN ('INT', 'NUMBER', 'DECIMAL') THEN 1 ELSE 0 END) as numeric_columns,
    SUM(CASE WHEN DATA_TYPE IN ('VARCHAR', 'TEXT') THEN 1 ELSE 0 END) as text_columns,
    SUM(CASE WHEN DATA_TYPE = 'BOOLEAN' THEN 1 ELSE 0 END) as boolean_columns,
    SUM(CASE WHEN DATA_TYPE = 'DATE' THEN 1 ELSE 0 END) as date_columns,
    SUM(CASE WHEN DATA_TYPE LIKE 'TIMESTAMP%' THEN 1 ELSE 0 END) as timestamp_columns
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_SCHEMA = 'ANALYTICS' 
AND TABLE_CATALOG = 'FLEETLOGIX_DW'
GROUP BY TABLE_NAME
ORDER BY TABLE_NAME;

-- Verificación de configuración de Time Travel
-- Confirma que las tablas tengan retención de datos habilitada
SELECT 
    TABLE_NAME,
    RETENTION_TIME
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA = 'ANALYTICS' 
AND TABLE_CATALOG = 'FLEETLOGIX_DW'
AND RETENTION_TIME > 0;

-- Resumen ejecutivo final de implementación
-- Proporciona confirmación de finalización exitosa
SELECT 
    'IMPLEMENTACION COMPLETADA EXITOSAMENTE' as status,
    CURRENT_TIMESTAMP() as verification_timestamp,
    'FleetLogix Data Warehouse - Modelo Estrella' as project_name,
    'Matias Damian Gutierrez' as author,
    'V 1.2' as version;