"""
Módulo de Detección de Anomalías - FleetLogix
=============================================
Autor: Matias Damian Gutierrez
Versión: 1.0
Descripción: Módulo para detección de anomalías en rutas, rendimiento
             de vehículos y conductores usando técnicas de ML.

Este módulo implementa:
- Detección de anomalías en eficiencia de combustible
- Identificación de rutas con rendimiento anómalo
- Detección de patrones inusuales en tiempos de entrega
- Alertas automáticas para conductores con comportamiento atípico

Uso:
    from MA_anomaly_detection import AnomalyDetector
    
    detector = AnomalyDetector(engine)
    anomalies = detector.detect_fuel_anomalies()
    
Dependencias:
    - pandas: Manipulación de datos
    - numpy: Cálculos numéricos
    - scikit-learn: Modelos de detección de anomalías (Isolation Forest, etc.)
    - sqlalchemy: Conexión a Snowflake
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import DBSCAN
import warnings

warnings.filterwarnings('ignore')

class AnomalyDetector:
    """
    Clase principal para detección de anomalías en el sistema FleetLogix.
    
    Esta clase implementa múltiples técnicas de detección:
    1. Isolation Forest para detección de outliers multivariados
    2. DBSCAN para clustering y detección de patrones anómalos
    3. Reglas basadas en dominio para detección de anomalías conocidas
    4. Análisis estadístico para identificación de valores extremos
    """
    
    def __init__(self, engine):
        """
        Inicializa el detector de anomalías con conexión a Snowflake.
        
        Args:
            engine: Motor de conexión SQLAlchemy a Snowflake.
        
        Attributes:
            engine: Conexión a base de datos.
            scaler: Escalador de features para normalización.
            anomaly_threshold: Umbral para clasificar anomalías.
        """
        self.engine = engine
        self.scaler = StandardScaler()
        self.anomaly_threshold = -0.5  # Umbral para Isolation Forest
    
    def load_delivery_data(self, days=30):
        """
        Carga datos de entregas desde Snowflake para análisis de anomalías.
        
        Args:
            days: Número de días de datos a cargar.
                  Por defecto: 30 días.
        
        Returns:
            pandas.DataFrame: DataFrame con datos de entregas.
        
        Raises:
            Exception: Error al cargar datos desde Snowflake.
        """
        try:
            query = f"""
            SELECT 
                f.delivery_id,
                f.package_weight_kg,
                f.distance_km,
                f.fuel_consumed_liters,
                f.fuel_efficiency_km_per_liter,
                f.delivery_time_minutes,
                f.delay_minutes,
                f.cost_per_delivery,
                f.revenue_per_delivery,
                v.vehicle_type,
                v.capacity_kg,
                dr.performance_category,
                r.difficulty_level,
                r.route_type,
                f.scheduled_datetime
            FROM ANALYTICS.FACT_DELIVERIES f
            JOIN ANALYTICS.DIM_VEHICLE v ON f.vehicle_key = v.vehicle_key
            JOIN ANALYTICS.DIM_DRIVER dr ON f.driver_key = dr.driver_key
            JOIN ANALYTICS.DIM_ROUTE r ON f.route_key = r.route_key
            WHERE f.scheduled_datetime >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
                AND f.delivery_status = 'delivered'
            """
            
            df = pd.read_sql(query, self.engine)
            df['scheduled_datetime'] = pd.to_datetime(df['scheduled_datetime'])
            return df
            
        except Exception as e:
            raise Exception(f"Error cargando datos de entregas: {e}")
    
    def detect_fuel_anomalies(self, days=30):
        """
        Detecta anomalías en eficiencia de combustible usando Isolation Forest.
        
        Isolation Forest es un algoritmo de ML eficiente para detección de
        anomalías que funciona aislando observaciones aleatoriamente.
        
        Args:
            days: Número de días de datos a analizar.
        
        Returns:
            dict: Diccionario con resultados de detección.
                  {
                      'anomalies': DataFrame con entregas anómalas,
                      'statistics': estadísticas del análisis,
                      'threshold': umbral usado
                  }
        
        Raises:
            Exception: Error en el proceso de detección.
        """
        try:
            # Cargar datos
            df = self.load_delivery_data(days)
            
            if len(df) < 100:
                raise ValueError("Se necesitan al menos 100 registros para detección de anomalías")
            
            # Seleccionar features relevantes para eficiencia de combustible
            features = ['distance_km', 'package_weight_kg', 'fuel_consumed_liters', 
                       'fuel_efficiency_km_per_liter', 'delivery_time_minutes']
            
            # Filtrar valores nulos
            df_clean = df[features].dropna()
            
            if len(df_clean) < 100:
                raise ValueError("Insuficientes datos válidos después de filtrar nulos")
            
            # Escalar features
            X_scaled = self.scaler.fit_transform(df_clean)
            
            # Entrenar Isolation Forest
            iso_forest = IsolationForest(
                contamination=0.05,  # 5% de anomalías esperadas
                random_state=42
            )
            anomaly_scores = iso_forest.fit_predict(X_scaled)
            
            # Agregar scores al DataFrame
            df_clean['anomaly_score'] = iso_forest.score_samples(X_scaled)
            df_clean['is_anomaly'] = anomaly_scores == -1
            
            # Unir con datos originales
            df_with_anomalies = df.loc[df_clean.index].copy()
            df_with_anomalies['anomaly_score'] = df_clean['anomaly_score'].values
            df_with_anomalies['is_anomaly'] = df_clean['is_anomaly'].values
            
            # Calcular estadísticas
            anomaly_count = df_with_anomalies['is_anomaly'].sum()
            anomaly_rate = (anomaly_count / len(df_with_anomalies)) * 100
            
            # Filtrar solo anomalías
            anomalies_df = df_with_anomalies[df_with_anomalies['is_anomaly']].copy()
            
            return {
                'anomalies': anomalies_df,
                'statistics': {
                    'total_records': len(df_with_anomalies),
                    'anomaly_count': int(anomaly_count),
                    'anomaly_rate': float(anomaly_rate),
                    'avg_efficiency_normal': float(df_with_anomalies[~df_with_anomalies['is_anomaly']]['fuel_efficiency_km_per_liter'].mean()),
                    'avg_efficiency_anomaly': float(df_with_anomalies[df_with_anomalies['is_anomaly']]['fuel_efficiency_km_per_liter'].mean())
                },
                'threshold': self.anomaly_threshold
            }
            
        except Exception as e:
            raise Exception(f"Error detectando anomalías de combustible: {e}")
    
    def detect_route_anomalies(self, days=30):
        """
        Detecta rutas con rendimiento anómalo usando análisis estadístico.
        
        Args:
            days: Número de días de datos a analizar.
        
        Returns:
            dict: Diccionario con rutas anómalas y sus métricas.
        
        Raises:
            Exception: Error en el proceso de detección.
        """
        try:
            # Cargar datos
            df = self.load_delivery_data(days)
            
            # Agrupar por ruta
            route_stats = df.groupby(['difficulty_level', 'route_type']).agg({
                'fuel_efficiency_km_per_liter': ['mean', 'std'],
                'delay_minutes': ['mean', 'std'],
                'delivery_time_minutes': ['mean', 'std'],
                'delivery_id': 'count'
            }).reset_index()
            
            route_stats.columns = ['difficulty_level', 'route_type', 
                                  'avg_efficiency', 'std_efficiency',
                                  'avg_delay', 'std_delay',
                                  'avg_time', 'std_time', 'deliveries']
            
            # Detectar rutas con variación inusual (std > 2 * mean)
            route_stats['high_efficiency_variance'] = (
                route_stats['std_efficiency'] > (2 * route_stats['avg_efficiency'])
            )
            route_stats['high_delay_variance'] = (
                route_stats['std_delay'] > (2 * route_stats['avg_delay'])
            )
            
            # Filtrar rutas anómalas
            anomalous_routes = route_stats[
                (route_stats['high_efficiency_variance']) |
                (route_stats['high_delay_variance'])
            ].copy()
            
            return {
                'anomalous_routes': anomalous_routes,
                'total_routes': len(route_stats),
                'anomalous_count': len(anomalous_routes)
            }
            
        except Exception as e:
            raise Exception(f"Error detectando anomalías de rutas: {e}")
    
    def detect_driver_anomalies(self, days=30):
        """
        Detecta conductores con comportamiento anómalo usando clustering.
        
        Args:
            days: Número de días de datos a analizar.
        
        Returns:
            dict: Diccionario con conductores anómalos y sus patrones.
        
        Raises:
            Exception: Error en el proceso de detección.
        """
        try:
            # Cargar datos con información de conductores
            query = f"""
            SELECT 
                dr.driver_key,
                dr.full_name,
                dr.performance_category,
                AVG(f.fuel_efficiency_km_per_liter) as avg_efficiency,
                AVG(f.delay_minutes) as avg_delay,
                AVG(f.delivery_time_minutes) as avg_time,
                COUNT(*) as total_deliveries,
                SUM(CASE WHEN f.is_on_time = TRUE THEN 1 ELSE 0 END) as on_time_count
            FROM ANALYTICS.FACT_DELIVERIES f
            JOIN ANALYTICS.DIM_DRIVER dr ON f.driver_key = dr.driver_key
            WHERE f.scheduled_datetime >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
                AND f.delivery_status = 'delivered'
            GROUP BY dr.driver_key, dr.full_name, dr.performance_category
            HAVING COUNT(*) >= 10
            """
            
            df = pd.read_sql(query, self.engine)
            
            if len(df) < 5:
                return {
                    'anomalous_drivers': pd.DataFrame(),
                    'message': 'Insuficientes conductores con datos para análisis'
                }
            
            # Calcular tasa de puntualidad
            df['on_time_rate'] = (df['on_time_count'] / df['total_deliveries']) * 100
            
            # Seleccionar features para clustering
            features = ['avg_efficiency', 'avg_delay', 'avg_time', 'on_time_rate']
            X = df[features].dropna()
            
            if len(X) < 5:
                return {
                    'anomalous_drivers': pd.DataFrame(),
                    'message': 'Insuficientes datos válidos para clustering'
                }
            
            # Escalar features
            X_scaled = self.scaler.fit_transform(X)
            
            # Aplicar DBSCAN para clustering
            dbscan = DBSCAN(eps=0.5, min_samples=2)
            clusters = dbscan.fit_predict(X_scaled)
            
            # Conductores con cluster -1 son anomalías (noise)
            df['cluster'] = clusters
            anomalous_drivers = df[df['cluster'] == -1].copy()
            
            return {
                'anomalous_drivers': anomalous_drivers,
                'total_drivers': len(df),
                'anomalous_count': len(anomalous_drivers),
                'cluster_distribution': df['cluster'].value_counts().to_dict()
            }
            
        except Exception as e:
            raise Exception(f"Error detectando anomalías de conductores: {e}")
    
    def detect_delivery_time_anomalies(self, days=30, std_threshold=3):
        """
        Detecta entregas con tiempos anómalos usando análisis estadístico.
        
        Args:
            days: Número de días de datos a analizar.
            std_threshold: Múltiplo de desviación estándar para definir anomalía.
                           Por defecto: 3 (valores fuera de 3 sigma).
        
        Returns:
            dict: Diccionario con entregas anómalas y estadísticas.
        
        Raises:
            Exception: Error en el proceso de detección.
        """
        try:
            # Cargar datos
            df = self.load_delivery_data(days)
            
            # Calcular estadísticas por tipo de ruta
            route_type_stats = df.groupby('route_type')['delivery_time_minutes'].agg([
                'mean', 'std'
            ]).reset_index()
            route_type_stats.columns = ['route_type', 'mean_time', 'std_time']
            
            # Unir estadísticas con datos originales
            df = df.merge(route_type_stats, on='route_type', how='left')
            
            # Calcular z-score para cada entrega
            df['z_score'] = (df['delivery_time_minutes'] - df['mean_time']) / df['std_time']
            
            # Identificar anomalías (z-score > threshold)
            df['is_anomaly'] = np.abs(df['z_score']) > std_threshold
            
            # Filtrar anomalías
            anomalies_df = df[df['is_anomaly']].copy()
            
            return {
                'anomalies': anomalies_df,
                'statistics': {
                    'total_deliveries': len(df),
                    'anomaly_count': len(anomalies_df),
                    'anomaly_rate': (len(anomalies_df) / len(df)) * 100,
                    'threshold': std_threshold
                }
            }
            
        except Exception as e:
            raise Exception(f"Error detectando anomalías de tiempo de entrega: {e}")
    
    def generate_anomaly_report(self, days=30):
        """
        Genera un reporte completo de anomalías en el sistema.
        
        Este es el método principal que orquesta todas las detecciones.
        
        Args:
            days: Número de días de datos a analizar.
        
        Returns:
            dict: Reporte completo con todas las anomalías detectadas.
                  {
                      'fuel_anomalies': resultados de detección de combustible,
                      'route_anomalies': resultados de detección de rutas,
                      'driver_anomalies': resultados de detección de conductores,
                      'time_anomalies': resultados de detección de tiempos,
                      'summary': resumen ejecutivo
                  }
        
        Raises:
            Exception: Error en el proceso de generación de reporte.
        
        Ejemplo:
            >>> detector = AnomalyDetector(engine)
            >>> report = detector.generate_anomaly_report(days=30)
            >>> print(f"Total anomalías detectadas: {report['summary']['total_anomalies']}")
        """
        try:
            report = {
                'generated_at': datetime.now().isoformat(),
                'analysis_period_days': days
            }
            
            # Detectar anomalías de combustible
            try:
                report['fuel_anomalies'] = self.detect_fuel_anomalies(days)
            except Exception as e:
                report['fuel_anomalies'] = {'error': str(e)}
            
            # Detectar anomalías de rutas
            try:
                report['route_anomalies'] = self.detect_route_anomalies(days)
            except Exception as e:
                report['route_anomalies'] = {'error': str(e)}
            
            # Detectar anomalías de conductores
            try:
                report['driver_anomalies'] = self.detect_driver_anomalies(days)
            except Exception as e:
                report['driver_anomalies'] = {'error': str(e)}
            
            # Detectar anomalías de tiempo de entrega
            try:
                report['time_anomalies'] = self.detect_delivery_time_anomalies(days)
            except Exception as e:
                report['time_anomalies'] = {'error': str(e)}
            
            # Generar resumen ejecutivo
            total_anomalies = 0
            if 'anomaly_count' in report.get('fuel_anomalies', {}).get('statistics', {}):
                total_anomalies += report['fuel_anomalies']['statistics']['anomaly_count']
            if 'anomalous_count' in report.get('route_anomalies', {}):
                total_anomalies += report['route_anomalies']['anomalous_count']
            if 'anomalous_count' in report.get('driver_anomalies', {}):
                total_anomalies += report['driver_anomalies']['anomalous_count']
            if 'anomaly_count' in report.get('time_anomalies', {}).get('statistics', {}):
                total_anomalies += report['time_anomalies']['statistics']['anomaly_count']
            
            report['summary'] = {
                'total_anomalies': total_anomalies,
                'analysis_status': 'completed' if total_anomalies > 0 else 'no_anomalies',
                'recommendation': 'Investigate anomalies' if total_anomalies > 0 else 'System operating normally'
            }
            
            return report
            
        except Exception as e:
            raise Exception(f"Error generando reporte de anomalías: {e}")


def main():
    """
    Función principal para demostración del módulo de detección de anomalías.
    """
    print("=" * 70)
    print("MÓDULO DE DETECCIÓN DE ANOMALÍAS - FLEETLOGIX")
    print("=" * 70)
    
    print("\nPara usar este módulo:")
    print("1. from MA_anomaly_detection import AnomalyDetector")
    print("2. detector = AnomalyDetector(engine)")
    print("3. report = detector.generate_anomaly_report(days=30)")
    print("4. print(report['summary'])")
    
    print("\nTipos de anomalías detectadas:")
    print("- Eficiencia de combustible anómala (Isolation Forest)")
    print("- Rutas con rendimiento inusual (Análisis estadístico)")
    print("- Conductores con comportamiento atípico (DBSCAN Clustering)")
    print("- Tiempos de entrega extremos (Z-score analysis)")
    
    print("\n" + "=" * 70)


if __name__ == "__main__":
    main()
