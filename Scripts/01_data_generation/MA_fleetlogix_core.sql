-- =============================================
-- Script PostgreSQL para FleetLogix Database
-- Optimizado para Data Warehouse y Análisis
-- Incluye verificaciones de calidad de datos
-- =============================================
-- 
-- INSTRUCCIONES:
-- 1. Conectarse a PostgreSQL con DBeaver (En Dbeaver, presionar Control + 9 y seleccionar como fuente de datos PostgreSql)
-- 2. Ejecutar solo el PASO 1 (crear BD)
-- 3. En DBeaver: Click derecho > Refresh en la conexión
-- 4. Cambiar a la nueva BD 'fleetlogix_db' 
-- 5. Ejecutar PASO 2 en adelante
--
-- =============================================
-- PASO 1: CREAR LA BASE DE DATOS (Ejecutar PRIMERO)
-- =============================================

CREATE DATABASE fleetlogix_db
    WITH 
    OWNER = postgres
    ENCODING = 'UTF8'
    CONNECTION LIMIT = -1;

-- ⚠️ DETENER EJECUCIÓN AQUÍ ⚠️
-- Ahora cambie manualmente a la BD 'fleetlogix_db' en DBeaver (Click derecho sobre dicho nombre e indicar "establecer por defecto")
-- Luego continúe con el PASO 2

-- PASO 2: VERIFICAR QUE ESTAMOS EN LA BD CORRECTA
-- =============================================
-- Esta consulta confirma que estamos en la BD correcta
SELECT 
    current_database() as base_datos_actual,
    current_user as usuario_actual,
    version() as version_postgres;

-- Si la consulta anterior muestra 'fleetlogix_db', continuar
-- Si no muestra 'fleetlogix_db', cambiar manualmente en DBeaver

-- =============================================
-- 1. CREACIÓN DE TABLAS PRINCIPALES
-- =============================================

-- Tabla de logs de ingesta de datos
CREATE TABLE IF NOT EXISTS data_ingestion_logs (
    log_id SERIAL PRIMARY KEY,
    table_name VARCHAR(50) NOT NULL,
    operation_type VARCHAR(20) NOT NULL,
    records_affected INTEGER NOT NULL,
    status VARCHAR(20) NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    duration_seconds DECIMAL(10,2),
    error_message TEXT,
    executed_by VARCHAR(100) DEFAULT CURRENT_USER,
    ingestion_date DATE DEFAULT CURRENT_DATE
);

-- Tabla vehicles con soporte para SCD Type 2 - ACTUALIZADA
CREATE TABLE vehicles (
    vehicle_id SERIAL PRIMARY KEY,
    license_plate VARCHAR(20) UNIQUE NOT NULL,
    vehicle_type VARCHAR(50) NOT NULL,
    capacity_kg DECIMAL(10,2),
    fuel_type VARCHAR(20),
    acquisition_date DATE,
    status VARCHAR(20) DEFAULT 'active',
    -- Campos para SCD Type 2 (AGREGADOS)
    valid_from DATE NOT NULL,
    valid_to DATE,
    is_current BOOLEAN DEFAULT TRUE,
    -- Metadata para calidad de datos
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabla drivers con soporte para SCD Type 2 - ACTUALIZADA
CREATE TABLE drivers (
    driver_id SERIAL PRIMARY KEY,
    employee_code VARCHAR(20) UNIQUE NOT NULL,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    license_number VARCHAR(50) UNIQUE NOT NULL,
    license_expiry DATE,
    phone VARCHAR(20),
    hire_date DATE,
    status VARCHAR(20) DEFAULT 'active',
    -- Campos adicionales para análisis dimensional (AGREGADOS)
    performance_category VARCHAR(20),
    -- Campos para SCD Type 2 (AGREGADOS)
    valid_from DATE NOT NULL,
    valid_to DATE,
    is_current BOOLEAN DEFAULT TRUE,
    -- Metadata para calidad de datos
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabla routes optimizada para dim_route - ACTUALIZADA
CREATE TABLE routes (
    route_id SERIAL PRIMARY KEY,
    route_code VARCHAR(20) UNIQUE NOT NULL,
    origin_city VARCHAR(100) NOT NULL,
    destination_city VARCHAR(100) NOT NULL,
    distance_km DECIMAL(10,2),
    estimated_duration_hours DECIMAL(5,2),
    toll_cost DECIMAL(10,2) DEFAULT 0,
    -- Campos adicionales para análisis dimensional (AGREGADOS)
    difficulty_level VARCHAR(20),
    route_type VARCHAR(20),
    -- Metadata para calidad de datos
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabla customers para dim_customer - ACTUALIZADA
CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    customer_name VARCHAR(200) NOT NULL,
    customer_type VARCHAR(20),
    city VARCHAR(100),
    first_delivery_date DATE,
    total_deliveries INTEGER,
    customer_category VARCHAR(20),
    -- Metadata para calidad de datos
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabla trips con métricas extendidas - ACTUALIZADA
CREATE TABLE trips (
    trip_id SERIAL PRIMARY KEY,
    vehicle_id INTEGER REFERENCES vehicles(vehicle_id),
    driver_id INTEGER REFERENCES drivers(driver_id),
    route_id INTEGER REFERENCES routes(route_id),
    departure_datetime TIMESTAMP NOT NULL,
    arrival_datetime TIMESTAMP,
    -- Métricas adicionales (AGREGADAS)
    fuel_consumed_liters DECIMAL(10,2),
    total_weight_kg DECIMAL(10,2),
    status VARCHAR(20) DEFAULT 'pending',
    -- Metadata para calidad de datos
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabla deliveries con métricas para fact_deliveries - ACTUALIZADA
CREATE TABLE deliveries (
    delivery_id SERIAL PRIMARY KEY,
    trip_id INTEGER REFERENCES trips(trip_id),
    customer_id INTEGER REFERENCES customers(customer_id),
    tracking_number VARCHAR(50) UNIQUE NOT NULL,
    package_weight_kg DECIMAL(10,2),
    scheduled_datetime TIMESTAMP,
    delivered_datetime TIMESTAMP,
    delivery_status VARCHAR(20) DEFAULT 'pending',
    recipient_signature BOOLEAN DEFAULT FALSE,
    -- Métricas extendidas para análisis (AGREGADAS)
    distance_km DECIMAL(10,2),
    fuel_consumed_liters DECIMAL(10,2),
    delivery_time_minutes INTEGER,
    delay_minutes INTEGER,
    deliveries_per_hour DECIMAL(5,2),
    fuel_efficiency_km_per_liter DECIMAL(5,2),
    cost_per_delivery DECIMAL(10,2),
    revenue_per_delivery DECIMAL(10,2),
    is_on_time BOOLEAN,
    is_damaged BOOLEAN DEFAULT FALSE,
    -- Metadata para calidad de datos
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabla maintenance - ACTUALIZADA
CREATE TABLE maintenance (
    maintenance_id SERIAL PRIMARY KEY,
    vehicle_id INTEGER REFERENCES vehicles(vehicle_id),
    maintenance_date DATE NOT NULL,
    maintenance_type VARCHAR(50) NOT NULL,
    description TEXT,
    cost DECIMAL(10,2),
    next_maintenance_date DATE,
    performed_by VARCHAR(200),
    -- Metadata para calidad de datos
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================
-- 2. CREACIÓN DE ÍNDICES PARA OPTIMIZACIÓN
-- =============================================

-- Índices básicos proporcionados
CREATE INDEX idx_trips_departure ON trips(departure_datetime);
CREATE INDEX idx_deliveries_status ON deliveries(delivery_status);
CREATE INDEX idx_vehicles_status ON vehicles(status);

-- Índices adicionales para optimización de consultas
CREATE INDEX idx_trips_vehicle ON trips(vehicle_id);
CREATE INDEX idx_trips_driver ON trips(driver_id);
CREATE INDEX idx_trips_route ON trips(route_id);
CREATE INDEX idx_trips_status ON trips(status);
CREATE INDEX idx_deliveries_trip ON deliveries(trip_id);
CREATE INDEX idx_deliveries_customer ON deliveries(customer_id);
CREATE INDEX idx_deliveries_scheduled ON deliveries(scheduled_datetime);
CREATE INDEX idx_deliveries_delivered ON deliveries(delivered_datetime);
CREATE INDEX idx_maintenance_vehicle ON maintenance(vehicle_id);
CREATE INDEX idx_maintenance_date ON maintenance(maintenance_date);
CREATE INDEX idx_drivers_employee_code ON drivers(employee_code);
CREATE INDEX idx_drivers_status ON drivers(status);
CREATE INDEX idx_routes_cities ON routes(origin_city, destination_city);
CREATE INDEX idx_customers_category ON customers(customer_category);

-- Índices compuestos para consultas complejas
CREATE INDEX idx_trips_daterange ON trips(departure_datetime, arrival_datetime);
CREATE INDEX idx_deliveries_performance ON deliveries(delivery_status, is_on_time, is_damaged);
CREATE INDEX idx_vehicles_fulldata ON vehicles(vehicle_type, fuel_type, status);

-- =============================================
-- 3. VISTAS PARA CALIDAD DE DATOS
-- =============================================

-- Vista para monitoreo de integridad referencial
CREATE OR REPLACE VIEW data_quality_integrity AS
SELECT 
    'vehicles' as table_name,
    COUNT(*) as total_records,
    COUNT(DISTINCT vehicle_id) as unique_ids,
    SUM(CASE WHEN license_plate IS NULL THEN 1 ELSE 0 END) as null_license_plates,
    SUM(CASE WHEN status NOT IN ('active', 'inactive', 'maintenance') THEN 1 ELSE 0 END) as invalid_status
FROM vehicles
UNION ALL
SELECT 
    'drivers' as table_name,
    COUNT(*) as total_records,
    COUNT(DISTINCT driver_id) as unique_ids,
    SUM(CASE WHEN employee_code IS NULL THEN 1 ELSE 0 END) as null_employee_codes,
    SUM(CASE WHEN status NOT IN ('active', 'inactive') THEN 1 ELSE 0 END) as invalid_status
FROM drivers
UNION ALL
SELECT 
    'trips' as table_name,
    COUNT(*) as total_records,
    COUNT(DISTINCT trip_id) as unique_ids,
    SUM(CASE WHEN vehicle_id NOT IN (SELECT vehicle_id FROM vehicles) THEN 1 ELSE 0 END) as orphaned_vehicles,
    SUM(CASE WHEN driver_id NOT IN (SELECT driver_id FROM drivers) THEN 1 ELSE 0 END) as orphaned_drivers
FROM trips
UNION ALL
SELECT 
    'deliveries' as table_name,
    COUNT(*) as total_records,
    COUNT(DISTINCT delivery_id) as unique_ids,
    SUM(CASE WHEN trip_id NOT IN (SELECT trip_id FROM trips) THEN 1 ELSE 0 END) as orphaned_trips,
    SUM(CASE WHEN customer_id NOT IN (SELECT customer_id FROM customers) THEN 1 ELSE 0 END) as orphaned_customers
FROM deliveries;

-- Vista para métricas de completitud de datos
CREATE OR REPLACE VIEW data_quality_completeness AS
SELECT 
    'vehicles' as table_name,
    ROUND(100.0 * COUNT(license_plate) / COUNT(*), 2) as license_plate_completeness,
    ROUND(100.0 * COUNT(acquisition_date) / COUNT(*), 2) as acquisition_date_completeness,
    ROUND(100.0 * COUNT(fuel_type) / COUNT(*), 2) as fuel_type_completeness
FROM vehicles
UNION ALL
SELECT 
    'drivers' as table_name,
    ROUND(100.0 * COUNT(employee_code) / COUNT(*), 2) as employee_code_completeness,
    ROUND(100.0 * COUNT(license_number) / COUNT(*), 2) as license_completeness,
    ROUND(100.0 * COUNT(hire_date) / COUNT(*), 2) as hire_date_completeness
FROM drivers
UNION ALL
SELECT 
    'trips' as table_name,
    ROUND(100.0 * COUNT(departure_datetime) / COUNT(*), 2) as departure_completeness,
    ROUND(100.0 * COUNT(arrival_datetime) / COUNT(*), 2) as arrival_completeness,
    ROUND(100.0 * COUNT(status) / COUNT(*), 2) as status_completeness
FROM trips
UNION ALL
SELECT 
    'deliveries' as table_name,
    ROUND(100.0 * COUNT(tracking_number) / COUNT(*), 2) as tracking_completeness,
    ROUND(100.0 * COUNT(delivered_datetime) / COUNT(*), 2) as delivery_time_completeness,
    ROUND(100.0 * COUNT(delivery_status) / COUNT(*), 2) as status_completeness
FROM deliveries;

-- =============================================
-- 4. FUNCIONES PARA VALIDACIÓN DE DATOS
-- =============================================

-- Función para validar fechas lógicas en trips
CREATE OR REPLACE FUNCTION validate_trip_dates()
RETURNS TABLE(trip_id INT, issue_type TEXT) AS $$
BEGIN
    RETURN QUERY
    SELECT t.trip_id, 'Departure after arrival' as issue_type
    FROM trips t
    WHERE t.arrival_datetime IS NOT NULL 
      AND t.departure_datetime > t.arrival_datetime
    
    UNION ALL
    
    SELECT t.trip_id, 'Future departure date' as issue_type
    FROM trips t
    WHERE t.departure_datetime > CURRENT_TIMESTAMP + INTERVAL '1 day'
    
    UNION ALL
    
    SELECT t.trip_id, 'Arrival before departure' as issue_type
    FROM trips t
    WHERE t.arrival_datetime IS NOT NULL 
      AND t.arrival_datetime < t.departure_datetime;
END;
$$ LANGUAGE plpgsql;

-- Función para validar métricas de deliveries
CREATE OR REPLACE FUNCTION validate_delivery_metrics()
RETURNS TABLE(delivery_id INT, issue_type TEXT) AS $$
BEGIN
    RETURN QUERY
    SELECT d.delivery_id, 'Negative delivery time' as issue_type
    FROM deliveries d
    WHERE d.delivery_time_minutes < 0
    
    UNION ALL
    
    SELECT d.delivery_id, 'Negative fuel consumed' as issue_type
    FROM deliveries d
    WHERE d.fuel_consumed_liters < 0
    
    UNION ALL
    
    SELECT d.delivery_id, 'Revenue less than cost' as issue_type
    FROM deliveries d
    WHERE d.revenue_per_delivery < d.cost_per_delivery;
END;
$$ LANGUAGE plpgsql;

-- =============================================
-- 5. CONSULTAS DE VERIFICACIÓN DE CALIDAD
-- =============================================

-- Consulta 1: Verificación de integridad referencial
SELECT 'Integridad Referencial - Vehículos en trips sin existencia' as check_type,
       COUNT(*) as issues_count
FROM trips t
LEFT JOIN vehicles v ON t.vehicle_id = v.vehicle_id
WHERE v.vehicle_id IS NULL

UNION ALL

SELECT 'Integridad Referencial - Conductores en trips sin existencia' as check_type,
       COUNT(*) as issues_count
FROM trips t
LEFT JOIN drivers d ON t.driver_id = d.driver_id
WHERE d.driver_id IS NULL

UNION ALL

SELECT 'Integridad Referencial - Rutas en trips sin existencia' as check_type,
       COUNT(*) as issues_count
FROM trips t
LEFT JOIN routes r ON t.route_id = r.route_id
WHERE r.route_id IS NULL;

-- Consulta 2: Verificación de valores nulos críticos
SELECT 'Valores Nulos - Placas en vehicles' as check_type,
       COUNT(*) as issues_count
FROM vehicles
WHERE license_plate IS NULL

UNION ALL

SELECT 'Valores Nulos - Código empleado en drivers' as check_type,
       COUNT(*) as issues_count
FROM drivers
WHERE employee_code IS NULL

UNION ALL

SELECT 'Valores Nulos - Fecha salida en trips' as check_type,
       COUNT(*) as issues_count
FROM trips
WHERE departure_datetime IS NULL;

-- Consulta 3: Verificación de duplicados
SELECT 'Duplicados - Licencias en drivers' as check_type,
       COUNT(*) - COUNT(DISTINCT license_number) as issues_count
FROM drivers

UNION ALL

SELECT 'Duplicados - Placas en vehicles' as check_type,
       COUNT(*) - COUNT(DISTINCT license_plate) as issues_count
FROM vehicles

UNION ALL

SELECT 'Duplicados - Tracking en deliveries' as check_type,
       COUNT(*) - COUNT(DISTINCT tracking_number) as issues_count
FROM deliveries;

-- Consulta 4: Verificación de rangos lógicos
SELECT 'Rangos Lógicos - Peso negativo en deliveries' as check_type,
       COUNT(*) as issues_count
FROM deliveries
WHERE package_weight_kg < 0

UNION ALL

SELECT 'Rangos Lógicos - Distancia negativa en routes' as check_type,
       COUNT(*) as issues_count
FROM routes
WHERE distance_km < 0

UNION ALL

SELECT 'Rangos Lógicos - Costo mantenimiento negativo' as check_type,
       COUNT(*) as issues_count
FROM maintenance
WHERE cost < 0;

-- Consulta 5: Verificación de consistencia temporal
SELECT 'Consistencia Temporal - Viajes con llegada antes de salida' as check_type,
       COUNT(*) as issues_count
FROM trips
WHERE arrival_datetime < departure_datetime

UNION ALL

SELECT 'Consistencia Temporal - Entregas antes de programación' as check_type,
       COUNT(*) as issues_count
FROM deliveries
WHERE delivered_datetime < scheduled_datetime

UNION ALL

SELECT 'Consistencia Temporal - Mantenimiento futuro' as check_type,
       COUNT(*) as issues_count
FROM maintenance
WHERE maintenance_date > CURRENT_DATE;

-- Consulta 6: Verificación de dominios de valores
SELECT 'Dominios - Tipo vehículo inválido' as check_type,
       COUNT(*) as issues_count
FROM vehicles
WHERE vehicle_type NOT IN ('Camión Grande', 'Camión Mediano', 'Van', 'Motocicleta')

UNION ALL

SELECT 'Dominios - Estado trip inválido' as check_type,
       COUNT(*) as issues_count
FROM trips
WHERE status NOT IN ('pending', 'in_progress', 'completed', 'cancelled')

UNION ALL

SELECT 'Dominios - Estado delivery inválido' as check_type,
       COUNT(*) as issues_count
FROM deliveries
WHERE delivery_status NOT IN ('pending', 'in_transit', 'delivered', 'failed', 'cancelled');

-- Consulta 7: Verificación de métricas de negocio
SELECT 'Métricas Negocio - Eficiencia combustible imposible' as check_type,
       COUNT(*) as issues_count
FROM deliveries
WHERE fuel_efficiency_km_per_liter > 50  -- Valor máximo realista

UNION ALL

SELECT 'Métricas Negocio - Entregas por hora imposibles' as check_type,
       COUNT(*) as issues_count
FROM deliveries
WHERE deliveries_per_hour > 20  -- Máximo realista

UNION ALL

SELECT 'Métricas Negocio - Tiempo entrega imposible' as check_type,
       COUNT(*) as issues_count
FROM deliveries
WHERE delivery_time_minutes > 480;  -- 8 horas máximo

-- Consulta 8: Resumen general de calidad de datos
SELECT 
    'RESUMEN CALIDAD DATOS' as metric,
    (SELECT COUNT(*) FROM vehicles) as total_vehicles,
    (SELECT COUNT(*) FROM drivers) as total_drivers,
    (SELECT COUNT(*) FROM trips) as total_trips,
    (SELECT COUNT(*) FROM deliveries) as total_deliveries,
    (SELECT COUNT(*) FROM validate_trip_dates()) as trip_date_issues,
    (SELECT COUNT(*) FROM validate_delivery_metrics()) as delivery_metric_issues,
    ROUND(100.0 * (SELECT COUNT(*) FROM trips WHERE arrival_datetime IS NOT NULL) / 
          (SELECT COUNT(*) FROM trips), 2) as trips_completeness_percent;

-- =============================================
-- 6. PROCEDIMIENTOS PARA MANTENIMIENTO
-- =============================================

-- Procedimiento para actualizar estadísticas
CREATE OR REPLACE PROCEDURE refresh_database_stats()
LANGUAGE plpgsql
AS $$
BEGIN
    ANALYZE vehicles;
    ANALYZE drivers;
    ANALYZE routes;
    ANALYZE customers;
    ANALYZE trips;
    ANALYZE deliveries;
    ANALYZE maintenance;
    
    INSERT INTO data_ingestion_logs 
    (table_name, operation_type, records_affected, status, start_time, end_time)
    VALUES ('ALL_TABLES', 'ANALYZE', 0, 'SUCCESS', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
    
    RAISE NOTICE 'Estadísticas de la base de datos actualizadas correctamente';
END;
$$;

-- =============================================
-- 7. VERIFICACIÓN FINAL DE ESTRUCTURA
-- =============================================

-- Verificar que todas las columnas necesarias existen
DO $$
DECLARE
    missing_columns TEXT := '';
BEGIN
    -- Verificar columnas en vehicles
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'vehicles' AND column_name = 'valid_from') THEN
        missing_columns := missing_columns || 'vehicles.valid_from, ';
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'vehicles' AND column_name = 'valid_to') THEN
        missing_columns := missing_columns || 'vehicles.valid_to, ';
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'vehicles' AND column_name = 'is_current') THEN
        missing_columns := missing_columns || 'vehicles.is_current, ';
    END IF;
    
    -- Verificar columnas en drivers
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'drivers' AND column_name = 'performance_category') THEN
        missing_columns := missing_columns || 'drivers.performance_category, ';
    END IF;
    
    -- Si hay columnas faltantes, mostrar error
    IF length(missing_columns) > 0 THEN
        missing_columns := RTRIM(missing_columns, ', ');
        RAISE EXCEPTION 'Columnas faltantes detectadas: %', missing_columns;
    ELSE
        RAISE NOTICE '✅ Todas las columnas necesarias existen en las tablas';
    END IF;
END $$;

-- Ejecutar procedimiento de estadísticas
CALL refresh_database_stats();

-- Mostrar resumen inicial
SELECT * FROM data_quality_integrity;
SELECT * FROM data_quality_completeness;

-- Mensaje final
DO $$
BEGIN
    RAISE NOTICE '=========================================';
    RAISE NOTICE 'Base de datos FleetLogix creada exitosamente';
    RAISE NOTICE 'Tablas: 7 principales + 1 de logs';
    RAISE NOTICE 'Índices: 15 índices optimizados';
    RAISE NOTICE 'Vistas: 2 vistas de calidad de datos';
    RAISE NOTICE 'Funciones: 2 funciones de validación';
    RAISE NOTICE 'Consultas: 8 consultas de verificación';
    RAISE NOTICE 'Estructura compatible con script Python V2.1';
    RAISE NOTICE '=========================================';
END $$;

--Usar para limpiar tablas 
-- Borrar tablas en orden inverso para respetar las dependencias
-- DROP TABLE IF EXISTS maintenance CASCADE;
-- DROP TABLE IF EXISTS deliveries CASCADE;
-- DROP TABLE IF EXISTS trips CASCADE;
-- DROP TABLE IF EXISTS customers CASCADE;
-- DROP TABLE IF EXISTS routes CASCADE;
-- DROP TABLE IF EXISTS drivers CASCADE;
-- DROP TABLE IF EXISTS vehicles CASCADE;
-- DROP TABLE IF EXISTS data_ingestion_logs CASCADE;