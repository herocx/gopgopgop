from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pickle
import pandas as pd
from sklearn.base import BaseEstimator
from sklearn.neighbors import KNeighborsClassifier  # Замени на свою библиотеку модели

MODEL_PATH = '/home/user1/Загрузки/Data/Data/test_model.pkl'
USER_DATA_PATH = '/home/user1/Загрузки/Data/Data/lolic.csv'
RETRAINED_MODEL_PATH = '/home/user1/Загрузки/Data/Data/test_new.pkl'

def retrain_model():
    # Загрузка модели из .pkl файла
    with open(MODEL_PATH, 'rb') as f:
        model = pickle.load(f)

    # Проверка, что модель поддерживает дообучение
    if not hasattr(model, 'partial_fit'):
        raise ValueError("Модель не поддерживает метод partial_fit для дообучения!")

    # Загрузка данных, загруженных пользователем
    user_data = pd.read_csv(USER_DATA_PATH)

    # Предположим, что данные уже предобработаны
    X_new = user_data.drop('target', axis=1)  # Признаки
    y_new = user_data['target']  # Целевая переменная

    model.partial_fit(X_new, y_new)  
    with open(RETRAINED_MODEL_PATH, 'wb') as f:
        pickle.dump(model, f)

    print("Модель успешно дообучена на пользовательских данных и сохранена!")

# Создание DAG
dag = DAG(
    'retrain_model_with_user_data_dag',
    description='DAG для дообучения модели на основе данных, загруженных пользователем',
    schedule=None,  #
    start_date=datetime(2025, 3, 1),
    catchup=False
)

retrain_task = PythonOperator(
    task_id='retrain_model_with_user_data_task',
    python_callable=retrain_model,
    dag=dag
)

retrain_task



