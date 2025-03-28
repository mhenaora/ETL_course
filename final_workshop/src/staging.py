import yaml
import psycopg2
from psycopg2 import sql
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd
import numpy as np

# Función para cargar la configuración
def load_config(file_path):
    with open(file_path, "r") as file:
        return yaml.safe_load(file)

# Función para conectar a DB
def connect_to_db(db_config):
    conn = psycopg2.connect(
        dbname="postgres",
        user=db_config["user"],
        password=db_config["password"],
        host=db_config["host"],
        port=db_config["port"]
    )
    conn.autocommit = True
    return conn

# Función para crear la base de datos
def create_database(conn, db_name):
    try:
        with conn.cursor() as cur:
            cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name)))
            print(f"Base de datos '{db_name}' creada exitosamente.")
    except psycopg2.errors.DuplicateDatabase:
        print(f"La base de datos '{db_name}' ya existe.")
    finally:
        conn.close()

# Función para cargar datos y definir tipos
def load_data(file_path, dtypes):
    return pd.read_csv(file_path, dtype=dtypes)

# Función para guardar datos en la base de datos
def save_to_db(df, db_config, db_name, table_name):
    try:
        engine = create_engine(f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_name}")
        df.to_sql(table_name, engine, if_exists='append', index=False)
        print(f"DataFrame guardado exitosamente en la tabla {table_name} en PostgreSQL.")
    except SQLAlchemyError as e:
        print(f"Error al guardar el DataFrame en la tabla {table_name}: {e}")

# Función para eliminar tabla en la base de datos
def drop_table(db_config, db_name, table_name):
    try:
        engine = create_engine(f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_name}")
        with engine.connect() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
            print(f"Tabla '{table_name}' eliminada exitosamente.")
    except SQLAlchemyError as e:
        print(f"Error al eliminar la tabla {table_name}: {e}")

# Diccionario de tipos de datos
dtypes = {
    "summary_code": np.uint16,
    "http_code": np.uint16,
    "total_time_ms": np.uint16,
    **{col: np.float32 for col in [
        "time_ms_accessory_detector_near_img", "time_ms_accessory_detector_far_img",
        "queue_time_ms_accessory_detector_near_img", "queue_time_ms_accessory_detector_far_img",
        "time_ms_face_detector_near_img", "time_ms_face_detector_far_img",
        "queue_time_ms_face_detector_near_img", "queue_time_ms_face_detector_far_img",
        "time_ms_liqe_near_img", "time_ms_liqe_far_img",
        "queue_time_ms_liqe_near_img", "queue_time_ms_liqe_far_img",
        "time_ms_topiq_near_img", "time_ms_topiq_far_img",
        "queue_time_ms_topiq_near_img", "queue_time_ms_topiq_far_img",
        "time_ms_classic_metrics_near_img", "time_ms_classic_metrics_far_img",
        "time_ms_as_35_selfies_near_img", "time_ms_as_35_selfies_far_img",
        "queue_time_ms_as_35_selfies_near_img", "queue_time_ms_as_35_selfies_far_img",
        "time_ms_ibeta2_crops_near_img", "time_ms_ibeta2_crops_far_img",
        "queue_time_ms_ibeta2_crops_near_img", "queue_time_ms_ibeta2_crops_far_img",
        "time_ms_ibeta2_full_near_img", "time_ms_ibeta2_full_far_img",
        "queue_time_ms_ibeta2_full_near_img", "queue_time_ms_ibeta2_full_far_img",
        "time_ms_ibeta2_clip_near_img", "time_ms_ibeta2_clip_far_img",
        "queue_time_ms_ibeta2_clip_near_img", "queue_time_ms_ibeta2_clip_far_img",
        "time_ms_geometry_check", "queue_time_ms_geometry_check"
    ]},
    **{col: np.float32 for col in [
        "score_liqe_near_img", "score_liqe_far_img",
        "score_topiq_near_img", "score_topiq_far_img",
        "score_sharpness_classic_metrics_near_img", "score_sharpness_classic_metrics_far_img",
        "score_colorfulness_classic_metrics_near_img", "score_colorfulness_classic_metrics_far_img",
        "score_contrast_classic_metrics_near_img", "score_contrast_classic_metrics_far_img",
        "score_brightness_classic_metrics_near_img", "score_brightness_classic_metrics_far_img",
        "score_blur_score_classic_metrics_near_img", "score_blur_score_classic_metrics_far_img",
        "score_svd_score_classic_metrics_near_img", "score_svd_score_classic_metrics_far_img",
        "prob_as_35_selfies_near_img", "prob_as_35_selfies_far_img",
        "prob_ibeta2_crops_near_img", "prob_ibeta2_crops_far_img",
        "prob_ibeta2_full_near_img", "prob_ibeta2_full_far_img",
        "prob_ibeta2_clip_near_img", "prob_ibeta2_clip_far_img",
        "prob_as_heuristics_near_img", "prob_as_heuristics_far_img"
    ]},
    "summary_status": "category",
    "summary_desc": "category",
    "service": "category",
    "convenio": "category",
    "api_hash": str,
    "user_name": str,
    "file_token": str,
    "procesoConvenioGuid": str,
    "documento": str,
    "date": str,
}