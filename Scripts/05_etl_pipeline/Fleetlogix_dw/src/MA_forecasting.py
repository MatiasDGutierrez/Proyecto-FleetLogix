"""
Módulo de Forecasting para Demanda de Entregas - FleetLogix
===========================================================
Autor: Matias Damian Gutierrez
Versión: 1.0
Descripción: Módulo de predicción de demanda de entregas usando técnicas
             de series temporales y machine learning.

Este módulo implementa:
- Forecasting de demanda diaria de entregas
- Análisis de tendencias y estacionalidad
- Predicción de picos de demanda
- Modelos de regresión para factores externos

Uso:
    from MA_forecasting import DemandForecaster
    
    forecaster = DemandForecaster(engine)
    forecast = forecaster.forecast_demand(days_ahead=7)
    
Dependencias:
    - pandas: Manipulación de datos
    - statsmodels: Modelos de series temporales (SARIMA, etc.)
    - scikit-learn: Modelos de ML para regresión
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from statsmodels.tsa.seasonal import seasonal_decompose
from statsmodels.tsa.arima.model import ARIMA
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import StandardScaler
import warnings

warnings.filterwarnings('ignore')

class DemandForecaster:
    """
    Clase principal para forecasting de demanda de entregas.
    
    Esta clase implementa múltiples técnicas de forecasting:
    1. Descomposición estacional para identificar patrones
    2. Modelos ARIMA para series temporales
    3. Random Forest para regresión con múltiples features
    4. Ensemble de modelos para mejor precisión
    """
    
    def __init__(self, engine):
        """
        Inicializa el forecaster con conexión a Snowflake.
        
        Args:
            engine: Motor de conexión SQLAlchemy a Snowflake.
        
        Attributes:
            engine: Conexión a base de datos.
            historical_data: DataFrame con datos históricos cargados.
            model: Modelo de ML entrenado.
            scaler: Escalador de features para normalización.
        """
        self.engine = engine
        self.historical_data = None
        self.model = None
        self.scaler = StandardScaler()
    
    def load_historical_data(self, days=90):
        """
        Carga datos históricos de entregas desde Snowflake.
        
        Args:
            days: Número de días de datos históricos a cargar.
                  Por defecto: 90 días (3 meses).
        
        Returns:
            pandas.DataFrame: DataFrame con datos históricos agregados por día.
                            Columnas: date, deliveries, on_time_rate, avg_efficiency,
                                       total_revenue, day_of_week, is_weekend, month.
        
        Raises:
            Exception: Error al cargar datos desde Snowflake.
        """
        try:
            query = f"""
            SELECT 
                DATE(scheduled_datetime) as date,
                COUNT(*) as deliveries,
                AVG(CASE WHEN is_on_time = TRUE THEN 1 ELSE 0 END) as on_time_rate,
                AVG(fuel_efficiency_km_per_liter) as avg_efficiency,
                SUM(revenue_per_delivery) as total_revenue
            FROM ANALYTICS.FACT_DELIVERIES
            WHERE scheduled_datetime >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
            GROUP BY DATE(scheduled_datetime)
            ORDER BY date ASC
            """
            
            df = pd.read_sql(query, self.engine)
            df['date'] = pd.to_datetime(df['date'])
            
            # Agregar features temporales
            df['day_of_week'] = df['date'].dt.dayofweek
            df['is_weekend'] = df['day_of_week'].isin([5, 6]).astype(int)
            df['month'] = df['date'].dt.month
            df['day_of_month'] = df['date'].dt.day
            
            self.historical_data = df
            return df
            
        except Exception as e:
            raise Exception(f"Error cargando datos históricos: {e}")
    
    def decompose_time_series(self, period=7):
        """
        Descompone la serie temporal en tendencia, estacionalidad y residuo.
        
        Este análisis ayuda a identificar patrones en la demanda:
        - Tendencia: Dirección general de la demanda (creciente/decreciente)
        - Estacionalidad: Patrones que se repiten periódicamente
        - Residuo: Variación aleatoria no explicada
        
        Args:
            period: Período para descomposición estacional.
                    Por defecto: 7 (semanal).
        
        Returns:
            dict: Diccionario con componentes de la descomposición.
                  {'trend': DataFrame, 'seasonal': DataFrame, 'residual': DataFrame}
        
        Raises:
            ValueError: Si no hay datos históricos cargados.
        """
        if self.historical_data is None or len(self.historical_data) < 14:
            raise ValueError("Se necesitan al menos 14 días de datos históricos para descomposición")
        
        try:
            # Descomposición aditiva
            decomposition = seasonal_decompose(
                self.historical_data['deliveries'],
                model='additive',
                period=period
            )
            
            return {
                'trend': decomposition.trend,
                'seasonal': decomposition.seasonal,
                'residual': decomposition.resid,
                'observed': decomposition.observed
            }
            
        except Exception as e:
            raise Exception(f"Error en descomposición: {e}")
    
    def train_arima_model(self, order=(1, 1, 1)):
        """
        Entrena un modelo ARIMA para forecasting de series temporales.
        
        ARIMA (AutoRegressive Integrated Moving Average) es un modelo
        estadístico para análisis y forecasting de series temporales.
        
        Args:
            order: Orden del modelo ARIMA (p, d, q).
                   p: orden autorregresivo
                   d: orden de diferenciación
                   q: orden de media móvil
                   Por defecto: (1, 1, 1).
        
        Returns:
            statsmodels.tsa.arima.model.ARIMAResults: Modelo ARIMA entrenado.
        
        Raises:
            ValueError: Si no hay datos históricos suficientes.
        """
        if self.historical_data is None or len(self.historical_data) < 30:
            raise ValueError("Se necesitan al menos 30 días de datos para ARIMA")
        
        try:
            model = ARIMA(self.historical_data['deliveries'], order=order)
            self.arima_model = model.fit()
            return self.arima_model
            
        except Exception as e:
            raise Exception(f"Error entrenando ARIMA: {e}")
    
    def forecast_arima(self, days_ahead=7):
        """
        Genera forecast usando modelo ARIMA.
        
        Args:
            days_ahead: Número de días a predecir.
                        Por defecto: 7 días.
        
        Returns:
            pandas.DataFrame: DataFrame con predicciones.
                            Columnas: date, predicted_deliveries, conf_lower, conf_upper.
        
        Raises:
            ValueError: Si el modelo ARIMA no está entrenado.
        """
        if not hasattr(self, 'arima_model'):
            raise ValueError("Modelo ARIMA no entrenado. Ejecute train_arima_model() primero")
        
        try:
            forecast = self.arima_model.forecast(steps=days_ahead)
            conf_int = self.arima_model.conf_int()
            
            last_date = self.historical_data['date'].max()
            forecast_dates = pd.date_range(
                start=last_date + timedelta(days=1),
                periods=days_ahead,
                freq='D'
            )
            
            result_df = pd.DataFrame({
                'date': forecast_dates,
                'predicted_deliveries': forecast.values,
                'conf_lower': conf_int.iloc[:, 0].values,
                'conf_upper': conf_int.iloc[:, 1].values
            })
            
            return result_df
            
        except Exception as e:
            raise Exception(f"Error en forecast ARIMA: {e}")
    
    def train_ml_model(self):
        """
        Entrena modelo de Random Forest para forecasting con múltiples features.
        
        Este modelo usa features adicionales como día de la semana, mes,
        indicador de fin de semana, etc. para mejorar las predicciones.
        
        Returns:
            sklearn.ensemble.RandomForestRegressor: Modelo entrenado.
        
        Raises:
            ValueError: Si no hay datos históricos suficientes.
        """
        if self.historical_data is None or len(self.historical_data) < 30:
            raise ValueError("Se necesitan al menos 30 días de datos para ML")
        
        try:
            # Preparar features
            feature_cols = ['day_of_week', 'is_weekend', 'month', 'day_of_month']
            X = self.historical_data[feature_cols]
            y = self.historical_data['deliveries']
            
            # Escalar features
            X_scaled = self.scaler.fit_transform(X)
            
            # Entrenar modelo
            self.model = RandomForestRegressor(
                n_estimators=100,
                max_depth=10,
                random_state=42
            )
            self.model.fit(X_scaled, y)
            
            return self.model
            
        except Exception as e:
            raise Exception(f"Error entrenando modelo ML: {e}")
    
    def forecast_ml(self, days_ahead=7):
        """
        Genera forecast usando modelo de Machine Learning.
        
        Args:
            days_ahead: Número de días a predecir.
                        Por defecto: 7 días.
        
        Returns:
            pandas.DataFrame: DataFrame con predicciones.
                            Columnas: date, predicted_deliveries.
        
        Raises:
            ValueError: Si el modelo ML no está entrenado.
        """
        if self.model is None:
            raise ValueError("Modelo ML no entrenado. Ejecute train_ml_model() primero")
        
        try:
            last_date = self.historical_data['date'].max()
            forecast_dates = pd.date_range(
                start=last_date + timedelta(days=1),
                periods=days_ahead,
                freq='D'
            )
            
            # Crear features para predicción
            forecast_df = pd.DataFrame({'date': forecast_dates})
            forecast_df['day_of_week'] = forecast_df['date'].dt.dayofweek
            forecast_df['is_weekend'] = forecast_df['day_of_week'].isin([5, 6]).astype(int)
            forecast_df['month'] = forecast_df['date'].dt.month
            forecast_df['day_of_month'] = forecast_df['date'].dt.day
            
            # Escalar features y predecir
            X_forecast = forecast_df[['day_of_week', 'is_weekend', 'month', 'day_of_month']]
            X_forecast_scaled = self.scaler.transform(X_forecast)
            predictions = self.model.predict(X_forecast_scaled)
            
            forecast_df['predicted_deliveries'] = predictions
            
            return forecast_df[['date', 'predicted_deliveries']]
            
        except Exception as e:
            raise Exception(f"Error en forecast ML: {e}")
    
    def forecast_demand(self, days_ahead=7, method='ensemble'):
        """
        Genera forecast de demanda usando el método especificado.
        
        Este es el método principal que orquesta el proceso completo de forecasting.
        
        Args:
            days_ahead: Número de días a predecir. Por defecto: 7.
            method: Método de forecasting a usar.
                   Opciones: 'arima', 'ml', 'ensemble'.
                   Por defecto: 'ensemble' (promedio de ambos modelos).
        
        Returns:
            dict: Diccionario con resultados del forecasting.
                  {
                      'forecast': DataFrame con predicciones,
                      'method': método usado,
                      'confidence': nivel de confianza (si aplica),
                      'decomposition': componentes de descomposición (si aplica)
                  }
        
        Raises:
            ValueError: Si el método especificado no es válido.
            Exception: Error en el proceso de forecasting.
        
        Ejemplo:
            >>> forecaster = DemandForecaster(engine)
            >>> forecaster.load_historical_data(days=90)
            >>> result = forecaster.forecast_demand(days_ahead=7, method='ensemble')
            >>> print(result['forecast'])
        """
        try:
            # Cargar datos si no están cargados
            if self.historical_data is None:
                self.load_historical_data()
            
            # Descomponer serie temporal
            decomposition = self.decompose_time_series()
            
            # Generar forecast según método
            if method == 'arima':
                self.train_arima_model()
                forecast_df = self.forecast_arima(days_ahead)
                return {
                    'forecast': forecast_df,
                    'method': 'ARIMA',
                    'decomposition': decomposition
                }
            
            elif method == 'ml':
                self.train_ml_model()
                forecast_df = self.forecast_ml(days_ahead)
                return {
                    'forecast': forecast_df,
                    'method': 'Random Forest',
                    'decomposition': decomposition
                }
            
            elif method == 'ensemble':
                # Entrenar ambos modelos
                self.train_arima_model()
                self.train_ml_model()
                
                # Generar predicciones
                arima_forecast = self.forecast_arima(days_ahead)
                ml_forecast = self.forecast_ml(days_ahead)
                
                # Ensemble: promedio de ambos modelos
                ensemble_forecast = arima_forecast[['date', 'predicted_deliveries']].copy()
                ensemble_forecast['predicted_deliveries'] = (
                    arima_forecast['predicted_deliveries'] + ml_forecast['predicted_deliveries']
                ) / 2
                
                return {
                    'forecast': ensemble_forecast,
                    'method': 'Ensemble (ARIMA + Random Forest)',
                    'arima_forecast': arima_forecast,
                    'ml_forecast': ml_forecast,
                    'decomposition': decomposition
                }
            
            else:
                raise ValueError(f"Método no válido: {method}. Use 'arima', 'ml' o 'ensemble'")
                
        except Exception as e:
            raise Exception(f"Error en forecast: {e}")
    
    def detect_demand_peaks(self, threshold_percentile=90):
        """
        Detecta picos de demanda históricos para planificación.
        
        Args:
            threshold_percentile: Percentil para definir pico.
                                 Por defecto: 90 (top 10%).
        
        Returns:
            pandas.DataFrame: DataFrame con días de pico.
                            Columnas: date, deliveries, is_peak.
        
        Raises:
            ValueError: Si no hay datos históricos cargados.
        """
        if self.historical_data is None:
            raise ValueError("Datos históricos no cargados")
        
        threshold = np.percentile(self.historical_data['deliveries'], threshold_percentile)
        
        peak_days = self.historical_data.copy()
        peak_days['is_peak'] = peak_days['deliveries'] >= threshold
        
        return peak_days[peak_days['is_peak']]


def main():
    """
    Función principal para demostración del módulo de forecasting.
    """
    print("=" * 70)
    print("MÓDULO DE FORECASTING - FLEETLOGIX")
    print("=" * 70)
    
    # Ejemplo de uso (requiere conexión a Snowflake)
    print("\nPara usar este módulo:")
    print("1. from MA_forecasting import DemandForecaster")
    print("2. forecaster = DemandForecaster(engine)")
    print("3. forecaster.load_historical_data(days=90)")
    print("4. result = forecaster.forecast_demand(days_ahead=7)")
    print("5. print(result['forecast'])")
    
    print("\nMétodos disponibles:")
    print("- 'arima': Modelo ARIMA para series temporales")
    print("- 'ml': Random Forest con múltiples features")
    print("- 'ensemble': Combinación de ambos modelos (recomendado)")
    
    print("\n" + "=" * 70)


if __name__ == "__main__":
    main()
