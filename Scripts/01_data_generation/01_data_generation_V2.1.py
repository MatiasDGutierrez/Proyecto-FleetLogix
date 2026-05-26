"""
Script de Generación de Datos Sintéticos para FleetLogix Database - V 2.0
OPTIMIZADO para Data Warehouse en Snowflake (Modelo Estrella)
Autor: Matias Damian Gutierrez
Descripción: Genera datos de prueba optimizados para el modelo dimensional de FleetLogix
MEJORAS APLICADAS:
1. Compatibilidad completa con modelo estrella de Snowflake
2. Datos optimizados para ETL y análisis dimensional
3. Estructura alineada con dim_date, dim_time, fact_deliveries
4. Métricas calculadas para tabla de hechos
5. SCD Type 2 para cambios históricos
"""

import os
import logging
from dotenv import load_dotenv
import psycopg2
from faker import Faker
import random
import pandas as pd
import numpy as np
from datetime import timedelta, datetime
import time

# ------------------------------
# Configuración de logging CON RUTAS RELATIVAS
# ------------------------------

# Cargar variables de entorno
load_dotenv()

# Configuración de logging MEJORADA
try:
    # Usar ruta relativa en lugar de absoluta
    script_dir = os.path.dirname(os.path.abspath(__file__))
    log_file_path = os.path.join(script_dir, 'data_generation.log')
    log_level = os.getenv('LOG_LEVEL', 'INFO')
    
    # Crear directorio si no existe
    os.makedirs(os.path.dirname(log_file_path), exist_ok=True)
    
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file_path, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    logging.info("Logging configurado correctamente con variables de entorno")
    logging.info(f"Archivo de log creado en: {log_file_path}")
    
except Exception as e:
    # Configuración fallback simplificada
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler()]
    )
    logging.warning(f"Error en configuracion de logging de archivo, usando solo consola: {e}")

# ------------------------------
# Configuración de Faker y semillas para reproducibilidad
# ------------------------------
fake = Faker("es_ES")  # LINEA CRÍTICA
random.seed(42)
np.random.seed(42)

# ------------------------------
# Conexión a PostgreSQL CON VARIABLES DE ENTORNO
# ------------------------------
def get_db_connection():
    """
    Establece conexión con la base de datos PostgreSQL usando variables de entorno.
    """
    try:
        conn = psycopg2.connect(
            dbname=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT')
        )
        logging.info("Conexion a PostgreSQL exitosa (usando variables de entorno)")
        return conn
    except Exception as e:
        logging.error(f"Error de conexion a la base de datos: {e}")
        raise

# LINEAS CRÍTICAS - Inicializar conexión global
conn = get_db_connection()
cur = conn.cursor()

# ------------------------------
# NUEVA: Configuración para modelo dimensional de Snowflake
# ------------------------------
def get_dimensional_config():
    """
    Configuración específica para el modelo dimensional de Snowflake.
    Define categorías y parámetros alineados con las dimensiones del DW.
    """
    return {
        'performance_categories': ['Alto', 'Medio', 'Bajo'],
        'customer_categories': ['Premium', 'Regular', 'Ocasional'],
        'difficulty_levels': ['Fácil', 'Medio', 'Difícil'],
        'route_types': ['Urbana', 'Interurbana', 'Rural'],
        'time_shifts': ['Turno 1', 'Turno 2', 'Turno 3'],
        'time_of_day_categories': ['Madrugada', 'Mañana', 'Tarde', 'Noche']
    }

# ------------------------------
# NUEVA: Definición de distancias consistentes MEJORADA
# ------------------------------
def get_consistent_distances():
    """
    Define distancias realistas y consistentes entre ciudades.
    Basado en distancias reales de Argentina.
    Ahora incluye dificultad y tipo de ruta para dim_route.
    """
    distance_matrix = {
        # Origen -> Destino: (distancia_km, dificultad, tipo_ruta)
        ('Buenos Aires', 'Córdoba'): (700, 'Medio', 'Interurbana'),
        ('Buenos Aires', 'Rosario'): (300, 'Fácil', 'Interurbana'),
        ('Buenos Aires', 'Mendoza'): (1050, 'Difícil', 'Interurbana'),
        ('Buenos Aires', 'La Plata'): (60, 'Fácil', 'Urbana'),
        
        ('Córdoba', 'Buenos Aires'): (700, 'Medio', 'Interurbana'),
        ('Córdoba', 'Rosario'): (400, 'Fácil', 'Interurbana'),
        ('Córdoba', 'Mendoza'): (600, 'Medio', 'Interurbana'),
        ('Córdoba', 'La Plata'): (760, 'Medio', 'Interurbana'),
        
        ('Rosario', 'Buenos Aires'): (300, 'Fácil', 'Interurbana'),
        ('Rosario', 'Córdoba'): (400, 'Fácil', 'Interurbana'),
        ('Rosario', 'Mendoza'): (900, 'Difícil', 'Interurbana'),
        ('Rosario', 'La Plata'): (360, 'Fácil', 'Interurbana'),
        
        ('Mendoza', 'Buenos Aires'): (1050, 'Difícil', 'Interurbana'),
        ('Mendoza', 'Córdoba'): (600, 'Medio', 'Interurbana'),
        ('Mendoza', 'Rosario'): (900, 'Difícil', 'Interurbana'),
        ('Mendoza', 'La Plata'): (1110, 'Difícil', 'Interurbana'),
        
        ('La Plata', 'Buenos Aires'): (60, 'Fácil', 'Urbana'),
        ('La Plata', 'Córdoba'): (760, 'Medio', 'Interurbana'),
        ('La Plata', 'Rosario'): (360, 'Fácil', 'Interurbana'),
        ('La Plata', 'Mendoza'): (1110, 'Difícil', 'Interurbana')
    }
    return distance_matrix

# ------------------------------
# Función para logs en base de datos
# ------------------------------
def log_to_database(conn, table_name, operation_type, records_affected, status, start_time, end_time=None, error_message=None):
    """
    Registra un evento de ingesta en la tabla de logs de la base de datos.
    
    Args:
        conn: Conexión a la base de datos
        table_name: Nombre de la tabla afectada
        operation_type: Tipo de operación (INSERT, UPDATE, etc.)
        records_affected: Número de registros afectados
        status: Estado de la operación (SUCCESS, ERROR)
        start_time: Timestamp de inicio
        end_time: Timestamp de finalización
        error_message: Mensaje de error en caso de fallo
    """
    try:
        duration = (end_time - start_time).total_seconds() if end_time else None
        
        query = """
            INSERT INTO data_ingestion_logs 
            (table_name, operation_type, records_affected, status, start_time, end_time, duration_seconds, error_message)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        with conn.cursor() as cur:
            cur.execute(query, (
                table_name, operation_type, records_affected, status,
                start_time, end_time, duration, error_message
            ))
        conn.commit()
        logging.debug(f"Log guardado en BD para la tabla {table_name}")
        
    except Exception as e:
        logging.error(f"Error guardando log en base de datos: {e}")

# ------------------------------
# Función para inserción masiva
# ------------------------------
def insert_data_massive(cur, data_tuples, columns, table_name):
    """
    Inserta datos de forma masiva usando executemany para mejor performance.
    
    Args:
        cur: Cursor de la base de datos
        data_tuples: Lista de tuplas con los datos a insertar
        columns: Lista de nombres de columnas
        table_name: Nombre de la tabla destino
    """
    try:
        start_time = datetime.now()
        placeholders = ', '.join(['%s'] * len(columns))
        columns_str = ', '.join(columns)
        
        query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
        cur.executemany(query, data_tuples)
        
        duration = (datetime.now() - start_time).total_seconds()
        logging.info(f"Insertados {len(data_tuples)} registros en {table_name} en {duration:.2f} segundos")
        
        return True
    except Exception as e:
        logging.error(f"Error insertando datos en {table_name}: {e}")
        raise

# ==============================
# 1. Generación de datos para vehicles - MEJORADO para SCD Type 2
# ==============================
def generate_vehicles():
    """Genera datos sintéticos para la tabla vehicles con soporte para cambios históricos."""
    start_time = datetime.now()
    logging.info("Iniciando generación de datos para vehicles...")
    
    try:
        vehicle_types = ["Camión Grande", "Camión Mediano", "Van", "Motocicleta"]
        fuel_types = ["Diesel", "Nafta", "Eléctrico"]
        status_options = ["active", "inactive", "maintenance"]

        vehicles_data = []
        
        # Generar 200 vehículos base con SCD Type 2
        for i in range(200):
            acquisition_date = fake.date_between(start_date="-10y", end_date="-2y")
            
            # Versión inicial del vehículo
            vehicles_data.append((
                fake.unique.license_plate(),
                random.choice(vehicle_types),
                round(random.uniform(200, 20000), 2),
                random.choice(fuel_types),
                acquisition_date,
                "active",  # Estado inicial
                acquisition_date,  # valid_from (SCD Type 2)
                None,  # valid_to (null indica versión actual)
                True   # is_current (SCD Type 2)
            ))

        columns = ['license_plate', 'vehicle_type', 'capacity_kg', 'fuel_type', 
                  'acquisition_date', 'status', 'valid_from', 'valid_to', 'is_current']
        insert_data_massive(cur, vehicles_data, columns, 'vehicles')
        
        log_to_database(conn, 'vehicles', 'INSERT', len(vehicles_data), 'SUCCESS', start_time, datetime.now())
        logging.info(f"Generación de vehicles completada: {len(vehicles_data)} registros (preparados para SCD Type 2)")
        
        return vehicles_data
        
    except Exception as e:
        log_to_database(conn, 'vehicles', 'INSERT', 0, 'ERROR', start_time, datetime.now(), str(e))
        logging.error(f"Error en la generación de vehicles: {e}")
        raise

# ==============================
# 2. Generación de datos para drivers - MEJORADO para SCD Type 2
# ==============================
def generate_drivers():
    """Genera datos sintéticos para la tabla drivers con soporte para cambios históricos."""
    start_time = datetime.now()
    logging.info("Iniciando generación de datos para drivers...")
    
    try:
        # Garantizar códigos únicos para empleados y licencias
        used_emp_codes = set()
        used_license_numbers = set()
        drivers_data = []
        
        config = get_dimensional_config()
        
        for i in range(400):
            # Generar employee_code único
            while True:
                emp_code = f"EMP{random.randint(100, 999)}"
                if emp_code not in used_emp_codes:
                    used_emp_codes.add(emp_code)
                    break
            
            # Generar license_number único
            while True:
                license_num = f"LIC{random.randint(10000, 99999)}"
                if license_num not in used_license_numbers:
                    used_license_numbers.add(license_num)
                    break
            
            hire_date = fake.date_between(start_date="-10y", end_date="-1y")
            performance = random.choice(config['performance_categories'])
            
            # Versión inicial del conductor (SCD Type 2)
            drivers_data.append((
                emp_code,
                fake.first_name(),
                fake.last_name(),
                license_num,
                fake.date_between(start_date="today", end_date="+5y"),
                fake.phone_number(),
                hire_date,
                "active",
                performance,
                hire_date,  # valid_from (SCD Type 2)
                None,  # valid_to (null indica versión actual)
                True   # is_current (SCD Type 2)
            ))

        columns = ['employee_code', 'first_name', 'last_name', 'license_number', 
                  'license_expiry', 'phone', 'hire_date', 'status', 'performance_category',
                  'valid_from', 'valid_to', 'is_current']
        insert_data_massive(cur, drivers_data, columns, 'drivers')
        
        log_to_database(conn, 'drivers', 'INSERT', len(drivers_data), 'SUCCESS', start_time, datetime.now())
        logging.info(f"Generación de drivers completada: {len(drivers_data)} registros (preparados para SCD Type 2)")
        
        return drivers_data
        
    except Exception as e:
        log_to_database(conn, 'drivers', 'INSERT', 0, 'ERROR', start_time, datetime.now(), str(e))
        logging.error(f"Error en la generación de drivers: {e}")
        raise

# ==============================
# 3. Generación de datos para routes - MEJORADO para dim_route
# ==============================
def generate_routes():
    """Genera datos sintéticos para la tabla routes optimizados para dim_route."""
    start_time = datetime.now()
    logging.info("Iniciando generación de datos para routes...")
    
    try:
        cities = ["Buenos Aires", "Rosario", "Córdoba", "Mendoza", "La Plata"]
        distance_matrix = get_consistent_distances()
        config = get_dimensional_config()
        routes_data = []
        
        # Generar combinaciones únicas
        combinations = []
        for origin in cities:
            for destination in cities:
                if origin != destination:
                    combinations.append((origin, destination))
        
        # Tomar 20 combinaciones únicas
        unique_routes = random.sample(combinations, min(20, len(combinations)))
        
        # Generar rutas con atributos para dim_route
        for i in range(50):
            if i < len(unique_routes):
                origin, destination = unique_routes[i]
            else:
                # Repetir rutas existentes con variaciones menores
                origin, destination = random.choice(unique_routes)
            
            # Obtener datos de la matriz de distancias
            base_distance, difficulty, route_type = distance_matrix.get((origin, destination), (500, 'Medio', 'Interurbana'))
            distance_variation = random.uniform(0.98, 1.02)  # Variación muy pequeña
            final_distance = round(base_distance * distance_variation, 2)
            estimated_hours = round(final_distance / 70, 2)  # Asumiendo 70 km/h promedio
            
            routes_data.append((
                f"R{i+1:03d}",
                origin,
                destination,
                final_distance,
                estimated_hours,
                round(random.uniform(0, 500), 2),  # toll_cost
                difficulty,  # difficulty_level para dim_route
                route_type   # route_type para dim_route
            ))

        columns = ['route_code', 'origin_city', 'destination_city', 'distance_km', 
                  'estimated_duration_hours', 'toll_cost', 'difficulty_level', 'route_type']
        insert_data_massive(cur, routes_data, columns, 'routes')
        
        log_to_database(conn, 'routes', 'INSERT', len(routes_data), 'SUCCESS', start_time, datetime.now())
        logging.info(f"Generación de routes completada: {len(routes_data)} registros (optimizados para dim_route)")
        
        # Verificar consistencia
        logging.info("Verificando consistencia de distancias...")
        cur.execute("""
            SELECT origin_city, destination_city, COUNT(*) as rutas, 
                   MIN(distance_km) as min_km, MAX(distance_km) as max_km,
                   ROUND(AVG(distance_km), 2) as avg_km
            FROM routes 
            GROUP BY origin_city, destination_city 
            HAVING COUNT(*) > 1
        """)
        inconsistencias = cur.fetchall()
        if inconsistencias:
            logging.warning(f"Se encontraron {len(inconsistencias)} rutas con múltiples registros")
            for inc in inconsistencias:
                logging.warning(f"  {inc[0]} → {inc[1]}: {inc[2]} rutas, km: {inc[3]} - {inc[4]} (avg: {inc[5]})")
        else:
            logging.info("Todas las rutas tienen distancias consistentes")
        
        return routes_data
        
    except Exception as e:
        log_to_database(conn, 'routes', 'INSERT', 0, 'ERROR', start_time, datetime.now(), str(e))
        logging.error(f"Error en la generación de routes: {e}")
        raise

# ==============================
# 4. Generación de datos para customers - NUEVA para dim_customer
# ==============================
def generate_customers():
    """Genera datos sintéticos para la tabla customers optimizada para dim_customer."""
    start_time = datetime.now()
    logging.info("Iniciando generación de datos para customers...")
    
    try:
        config = get_dimensional_config()
        customer_types = ['Individual', 'Empresa', 'Gobierno']
        cities = ["Buenos Aires", "Rosario", "Córdoba", "Mendoza", "La Plata"]
        
        customers_data = []
        
        for i in range(1000):  # 1000 clientes para análisis dimensional
            customer_type = random.choice(customer_types)
            first_delivery = fake.date_between(start_date='-3y', end_date='today')
            total_deliveries = random.randint(1, 500)
            
            # Determinar categoría basada en volumen de entregas (para dim_customer)
            if total_deliveries > 100:
                category = 'Premium'
            elif total_deliveries > 20:
                category = 'Regular'
            else:
                category = 'Ocasional'
            
            customers_data.append((
                fake.company() if customer_type != 'Individual' else fake.name(),
                customer_type,
                random.choice(cities),
                first_delivery,
                total_deliveries,
                category
            ))

        columns = ['customer_name', 'customer_type', 'city', 'first_delivery_date', 
                  'total_deliveries', 'customer_category']
        insert_data_massive(cur, customers_data, columns, 'customers')
        
        log_to_database(conn, 'customers', 'INSERT', len(customers_data), 'SUCCESS', start_time, datetime.now())
        logging.info(f"Generación de customers completada: {len(customers_data)} registros (optimizados para dim_customer)")
        
        return customers_data
        
    except Exception as e:
        log_to_database(conn, 'customers', 'INSERT', 0, 'ERROR', start_time, datetime.now(), str(e))
        logging.error(f"Error en la generación de customers: {e}")
        raise

# ------------------------------
# Función auxiliar: distribución horaria MEJORADA
# ------------------------------
def get_hourly_distribution(peaks=None, peak_weights=None, spread=1):
    """
    Genera distribución de probabilidades para las horas del día.
    Optimizada para análisis temporal en dim_time.
    
    Args:
        peaks: Lista de horas pico
        peak_weights: Pesos para las horas pico
        spread: Factor de dispersión
        
    Returns:
        probs: Array de probabilidades para cada hora
    """
    w = np.ones(24, dtype=float)
    if peaks:
        if peak_weights is None:
            peak_weights = [3] * len(peaks)
        for h, wt in zip(peaks, peak_weights):
            h = int(h) % 24
            w[h] += wt
    if spread > 0:
        kernel = np.ones(2 * spread + 1, dtype=float)
        w = np.convolve(w, kernel, mode='same')
    probs = w / w.sum()
    return probs

# ------------------------------
# Función principal: generar trips - MEJORADA para fact_deliveries
# ------------------------------
def generate_trips(n_trips, start_date, end_date, vehicle_ids, driver_ids, routes_df, hourly_dist=None, seed=None):
    """
    Genera viajes sintéticos optimizados para fact_deliveries.
    
    Args:
        n_trips: Número de viajes a generar
        start_date: Fecha de inicio del período
        end_date: Fecha de fin del período
        vehicle_ids: Lista de IDs de vehículos disponibles
        driver_ids: Lista de IDs de conductores disponibles
        routes_df: DataFrame con información de rutas
        hourly_dist: Distribución horaria personalizada
        seed: Semilla para reproducibilidad
        
    Returns:
        df: DataFrame con los viajes generados
    """
    if seed is not None:
        np.random.seed(seed)
        random.seed(seed)
    
    start = pd.to_datetime(start_date)
    
    # Validación de fechas para coherencia temporal
    if end_date is None:
        end = pd.to_datetime(datetime.now().date())
    else:
        end = pd.to_datetime(end_date)
        if end > pd.to_datetime(datetime.now().date()):
            end = pd.to_datetime(datetime.now().date())
    
    total_days = max(1, (end - start).days)
    
    if total_days <= 0:
        raise ValueError("La fecha de inicio debe ser anterior a la fecha actual")
    
    vehicle_ids = np.array(vehicle_ids)
    driver_ids = np.array(driver_ids)
    route_ids = routes_df['route_id'].to_numpy()
    
    if hourly_dist is None:
        hourly_probs = np.ones(24) / 24
    else:
        hourly_probs = np.array(hourly_dist)
        hourly_probs = hourly_probs / hourly_probs.sum()
    
    route_to_duration = dict(zip(routes_df['route_id'], routes_df['estimated_duration_hours']))
    route_to_distance = dict(zip(routes_df['route_id'], routes_df['distance_km']))
    
    chosen_routes = np.random.choice(route_ids, size=n_trips)
    chosen_vehicles = np.random.choice(vehicle_ids, size=n_trips)
    chosen_drivers = np.random.choice(driver_ids, size=n_trips)
    
    day_offsets = np.random.randint(0, total_days, size=n_trips)
    chosen_hours = np.random.choice(np.arange(24), size=n_trips, p=hourly_probs)
    chosen_minutes = np.random.randint(0, 60, size=n_trips)
    chosen_seconds = np.random.randint(0, 60, size=n_trips)
    
    dep_dates = (pd.to_datetime(start) + 
                 pd.to_timedelta(day_offsets, unit='d') +
                 pd.to_timedelta(chosen_hours, unit='h') +
                 pd.to_timedelta(chosen_minutes, unit='m') +
                 pd.to_timedelta(chosen_seconds, unit='s'))
    
    durations = np.array([float(route_to_duration[r]) for r in chosen_routes])
    
    # Variación realista en duraciones
    noise_hours = np.random.uniform(-0.2, 1.0, size=n_trips)
    arrival_dates = dep_dates + pd.to_timedelta(np.maximum(0.1, durations + noise_hours), unit='h')
    
    # Validación de fechas futuras
    current_datetime = pd.to_datetime(datetime.now())
    arrival_dates = [min(arrival, current_datetime) for arrival in arrival_dates]
    
    distances = np.array([float(route_to_distance[r]) for r in chosen_routes])
    
    # Consumo de combustible más realista para métricas
    fuel_per_km = np.random.uniform(0.15, 0.30, size=n_trips)
    fuel_consumed = np.round(distances * fuel_per_km, 2)
    total_weight = np.round(np.random.uniform(50, 20000, size=n_trips), 2)
    
    # Lógica de estados mejorada para análisis
    statuses = []
    for i, (departure, arrival) in enumerate(zip(dep_dates, arrival_dates)):
        if arrival.date() < datetime.now().date():
            # Viajes pasados: 90% completados, 10% cancelados
            statuses.append(np.random.choice(['completed', 'cancelled'], p=[0.90, 0.10]))
        elif departure.date() > datetime.now().date():
            # Viajes futuros: pendientes
            statuses.append('pending')
        else:
            # Viajes hoy: mezcla realista
            statuses.append(np.random.choice(['completed', 'in_progress', 'pending'], p=[0.40, 0.30, 0.30]))
    
    df = pd.DataFrame({
        'trip_id': np.arange(1, n_trips + 1),
        'vehicle_id': chosen_vehicles,
        'driver_id': chosen_drivers,
        'route_id': chosen_routes,
        'departure_datetime': dep_dates,
        'arrival_datetime': arrival_dates,
        'fuel_consumed_liters': fuel_consumed,
        'total_weight_kg': total_weight,
        'status': statuses
    })
    
    return df

# ==============================
# 5. Generación de datos para trips - ACTUALIZADA para ETL
# ==============================
def generate_and_insert_trips():
    """Genera e inserta datos de trips optimizados para el proceso ETL."""
    start_time = datetime.now()
    logging.info("Iniciando generación de datos para trips...")
    
    try:
        # Obtener datos existentes para relaciones
        cur.execute("SELECT vehicle_id FROM vehicles")
        vehicle_ids = [row[0] for row in cur.fetchall()]

        cur.execute("SELECT driver_id FROM drivers")  
        driver_ids = [row[0] for row in cur.fetchall()]

        cur.execute("SELECT route_id, distance_km, estimated_duration_hours FROM routes")
        routes_data = cur.fetchall()
        routes_df = pd.DataFrame({
            'route_id': [row[0] for row in routes_data],
            'distance_km': [row[1] for row in routes_data],
            'estimated_duration_hours': [row[2] for row in routes_data]
        })

        # Configurar distribución horaria realista para análisis temporal
        hourly_dist = get_hourly_distribution(peaks=[8, 9, 17], peak_weights=[4, 3, 5], spread=1)
        
        df_trips = generate_trips(
            n_trips=100000,
            start_date='2023-01-01',
            end_date=datetime.now().date(),
            vehicle_ids=vehicle_ids,
            driver_ids=driver_ids,
            routes_df=routes_df,
            hourly_dist=hourly_dist,
            seed=42
        )

        trip_tuples = [tuple(x) for x in df_trips.to_numpy()]
        columns = ['trip_id', 'vehicle_id', 'driver_id', 'route_id', 'departure_datetime', 
                  'arrival_datetime', 'fuel_consumed_liters', 'total_weight_kg', 'status']
        
        insert_data_massive(cur, trip_tuples, columns, 'trips')
        
        log_to_database(conn, 'trips', 'INSERT', len(trip_tuples), 'SUCCESS', start_time, datetime.now())
        logging.info(f"Generación de trips completada: {len(trip_tuples)} registros")
        
        # Mostrar estadísticas de las fechas generadas
        min_date = df_trips['departure_datetime'].min()
        max_date = df_trips['arrival_datetime'].max()
        logging.info(f"Rango de fechas de trips: {min_date} hasta {max_date}")
        
        return df_trips
        
    except Exception as e:
        log_to_database(conn, 'trips', 'INSERT', 0, 'ERROR', start_time, datetime.now(), str(e))
        logging.error(f"Error en la generación de trips: {e}")
        raise

# ==============================
# 6. Generación de datos para deliveries - MEJORADA para fact_deliveries
# ==============================
def generate_and_insert_deliveries():
    """Genera e inserta datos de deliveries con métricas para fact_deliveries."""
    start_time = datetime.now()
    logging.info("Iniciando generación de datos para deliveries...")
    
    try:
        cur.execute("SELECT trip_id, departure_datetime, arrival_datetime FROM trips ORDER BY trip_id")
        trips = cur.fetchall()

        cur.execute("SELECT customer_id FROM customers")
        customer_ids = [row[0] for row in cur.fetchall()]

        delivery_data = []
        delivery_count = 0
        
        for trip in trips:
            trip_id, departure, arrival = trip
            num_deliveries = np.random.choice([2, 3, 4, 5, 6], p=[0.1, 0.2, 0.4, 0.2, 0.1])
            
            for delivery_num in range(num_deliveries):
                if delivery_count >= 400000:
                    break
                    
                trip_duration = arrival - departure
                delivery_time = departure + (trip_duration * (delivery_num + 1) / (num_deliveries + 1))
                
                # Validación de fechas futuras
                current_time = datetime.now()
                if delivery_time > current_time:
                    # Si la entrega programada es en el futuro, ajustar al pasado reciente
                    delivery_time = current_time - timedelta(hours=random.randint(1, 24))
                
                # Seleccionar cliente aleatorio
                customer_id = random.choice(customer_ids)
                
                # CALCULAR MÉTRICAS PARA fact_deliveries
                package_weight = round(random.uniform(1, 500), 2)
                delivery_time_minutes = random.randint(15, 120)  # Tiempo de entrega en minutos
                delay_minutes = max(0, random.randint(-10, 45))  # Retraso (puede ser negativo = temprano)
                
                # Métricas calculadas para fact_deliveries
                is_on_time = delay_minutes <= 15
                deliveries_per_hour = round(60 / delivery_time_minutes, 2) if delivery_time_minutes > 0 else 0
                
                # Calcular eficiencia de combustible para esta entrega
                fuel_for_delivery = round(random.uniform(5, 50), 2)  # Litros por entrega
                distance_for_delivery = round(random.uniform(5, 100), 2)  # Km por entrega
                fuel_efficiency = round(distance_for_delivery / fuel_for_delivery, 2) if fuel_for_delivery > 0 else 0
                
                # Costos e ingresos para análisis de profitability
                cost_per_delivery = round(random.uniform(50, 500), 2)
                revenue_per_delivery = round(cost_per_delivery * random.uniform(1.1, 2.0), 2)
                
                # Lógica de estados para análisis de performance
                if delivery_time <= current_time:
                    if random.random() > 0.15:  # 85% entregados
                        status = "delivered"
                        delivered_time = delivery_time + timedelta(minutes=delay_minutes)
                        signature = random.choice([True, False])
                        is_damaged = random.random() < 0.02  # 2% de paquetes dañados
                    else:  # 15% con problemas
                        status = random.choice(["failed", "cancelled"])
                        delivered_time = None
                        signature = False
                        is_damaged = False
                else:
                    status = "pending"
                    delivered_time = None
                    signature = False
                    is_damaged = False
                
                delivery_data.append((
                    trip_id,
                    customer_id,
                    f"TRK{trip_id:06d}-{delivery_num+1:02d}",
                    package_weight,
                    delivery_time,
                    delivered_time,
                    status,
                    signature,
                    # NUEVAS MÉTRICAS PARA fact_deliveries
                    distance_for_delivery,
                    fuel_for_delivery,
                    delivery_time_minutes,
                    delay_minutes,
                    deliveries_per_hour,
                    fuel_efficiency,
                    cost_per_delivery,
                    revenue_per_delivery,
                    is_on_time,
                    is_damaged
                ))
                
                delivery_count += 1
                
                if delivery_count % 50000 == 0:
                    logging.info(f"Progreso de deliveries: {delivery_count}/400000")
                
            if delivery_count >= 400000:
                break

        columns = [
            'trip_id', 'customer_id', 'tracking_number', 'package_weight_kg',
            'scheduled_datetime', 'delivered_datetime', 'delivery_status', 'recipient_signature',
            'distance_km', 'fuel_consumed_liters', 'delivery_time_minutes', 'delay_minutes',
            'deliveries_per_hour', 'fuel_efficiency_km_per_liter', 'cost_per_delivery',
            'revenue_per_delivery', 'is_on_time', 'is_damaged'
        ]
        
        insert_data_massive(cur, delivery_data, columns, 'deliveries')
        
        log_to_database(conn, 'deliveries', 'INSERT', len(delivery_data), 'SUCCESS', start_time, datetime.now())
        logging.info(f"Generación de deliveries completada: {len(delivery_data)} registros con métricas para fact_deliveries")
        
        return delivery_data
        
    except Exception as e:
        log_to_database(conn, 'deliveries', 'INSERT', 0, 'ERROR', start_time, datetime.now(), str(e))
        logging.error(f"Error en la generación de deliveries: {e}")
        raise

# ==============================
# 7. Generación de datos para maintenance - MANTENIDA
# ==============================
def generate_and_insert_maintenance():
    """Genera e inserta datos de maintenance en la base de datos."""
    start_time = datetime.now()
    logging.info("Iniciando generación de datos para maintenance...")
    
    try:
        cur.execute("SELECT vehicle_id FROM vehicles")
        vehicle_ids = [row[0] for row in cur.fetchall()]

        maintenance_types = [
            "Cambio de aceite", "Revisión de frenos", "Cambio de llantas",
            "Mantenimiento general", "Revisión de motor", "Alineación y balanceo"
        ]

        maintenance_data = [
            (
                random.choice(vehicle_ids),
                fake.date_between(start_date="-2y", end_date="today"),
                random.choice(maintenance_types),
                fake.sentence(),
                round(random.uniform(5000, 50000), 2),
                fake.date_between(start_date="today", end_date="+1y"),
                fake.company()
            )
            for _ in range(5000)
        ]

        columns = ['vehicle_id', 'maintenance_date', 'maintenance_type', 'description', 'cost', 'next_maintenance_date', 'performed_by']
        insert_data_massive(cur, maintenance_data, columns, 'maintenance')
        
        log_to_database(conn, 'maintenance', 'INSERT', len(maintenance_data), 'SUCCESS', start_time, datetime.now())
        logging.info(f"Generación de maintenance completada: {len(maintenance_data)} registros")
        
        return maintenance_data
        
    except Exception as e:
        log_to_database(conn, 'maintenance', 'INSERT', 0, 'ERROR', start_time, datetime.now(), str(e))
        logging.error(f"Error en la generación de maintenance: {e}")
        raise

# ==============================
# FUNCIÓN PRINCIPAL - ACTUALIZADA para Snowflake DW
# ==============================
def main():
    """Función principal que orquesta la generación de datos optimizados para Snowflake DW."""
    global_start_time = datetime.now()
    logging.info("INICIANDO CARGA MASIVA DE DATOS FLEETLOGIX - VERSION OPTIMIZADA PARA SNOWFLAKE DW")
    print(f"Directorio de trabajo actual: {os.getcwd()}")
    print("Verificación de permisos de escritura...")
    
    # Test de permisos de escritura MEJORADO - usa directorio temporal del sistema
    try:
        import tempfile
        temp_dir = tempfile.gettempdir()
        test_path = os.path.join(temp_dir, 'test_permisos_fleetlogix.txt')
        with open(test_path, 'w') as f:
            f.write('Test de permisos de escritura - FleetLogix\n')
        print(f"Permisos de escritura verificados correctamente en: {temp_dir}")
        os.remove(test_path)
    except Exception as e:
        print(f"Error de permisos de escritura: {e}")
        # Continuar de todos modos, no es crítico
    
    try:
        # Crear tabla de logs si no existe
        try:
            cur.execute("""
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
                )
            """)
            conn.commit()
            logging.info("Tabla de logs creada/verificada exitosamente")
        except Exception as e:
            logging.warning(f"No se pudo crear la tabla de logs: {e}")

        # Ejecutar generación de datos en orden de dependencias
        generate_vehicles()      # Con SCD Type 2 para dim_vehicle
        generate_drivers()       # Con SCD Type 2 para dim_driver
        generate_routes()        # Optimizado para dim_route
        generate_customers()     # Optimizado para dim_customer
        generate_and_insert_trips()  # Para relaciones temporales
        generate_and_insert_deliveries()  # Con métricas para fact_deliveries
        generate_and_insert_maintenance() # Datos adicionales

        # Verificación final y métricas
        total_time = (datetime.now() - global_start_time).total_seconds()
        logging.info("=============================================")
        logging.info(f"CARGA COMPLETADA EN {total_time:.2f} SEGUNDOS")
        logging.info("✅ Todos los datos generados exitosamente")
        logging.info("✅ Estructura optimizada para Snowflake DW")
        logging.info("✅ SCD Type 2 implementado en vehículos y conductores")
        logging.info("✅ Métricas calculadas para fact_deliveries")
        logging.info("✅ Datos listos para proceso ETL")
        
    except Exception as e:
        logging.error(f"ERROR CRITICO EN LA CARGA DE DATOS: {e}")
        logging.error("Traceback completo:", exc_info=True)
        conn.rollback()
        raise
        
    finally:
        try:
            total_time = (datetime.now() - global_start_time).total_seconds()
            logging.info("=============================================")
            logging.info(f"CARGA COMPLETADA EN {total_time:.2f} SEGUNDOS")
        except NameError:
            logging.info("=============================================")
            logging.info("CARGA COMPLETADA")
        
        # Cerrar conexión a la base de datos
        if conn:
            conn.close()
            logging.info("Conexión a base de datos cerrada correctamente")

if __name__ == "__main__":
    main()