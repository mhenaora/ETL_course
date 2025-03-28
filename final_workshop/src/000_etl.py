import argparse
from staging import *
# from staging import load_config, connect_to_db, create_database, load_data, save_to_db, dtypes
from transform import *
#from transform import show_most_frequent_values, transform_imgsize_aspect_ratio, transform_img_quality, transform_date_by_range, sum_columns, subtract_columns, groupby_tail_sorted, fillna_columns, compare_columns

# Función principal para ejecutar el proceso ETL
def run_etl(input_csv):
    config = load_config("config.yaml")
    db_config = config["database"]
    
    conn = connect_to_db(db_config)
    create_database(conn, "etl_db_final_workshop")
    
    #Staging
    df = load_data(input_csv, dtypes)
    save_to_db(df, db_config, "etl_db_final_workshop", "etl_staging_table_final_workshop")

    #Transform
    df = transform_date_by_range(df, 'date')
    df['aspect_ratio_near_img'] = df.apply(transform_imgsize_aspect_ratio, axis=1, width_col='img_size_near_img_width', height_col='img_size_near_img_height')
    df['aspect_ratio_far_img'] = df.apply(transform_imgsize_aspect_ratio, axis=1, width_col='img_size_far_img_width', height_col='img_size_far_img_height')
    df['same_aspect_ratio'] = df.apply(compare_columns, axis=1, col1='aspect_ratio_far_img', col2='aspect_ratio_near_img')
    df['image_quality_near_img'] = df.apply(transform_img_quality, axis=1, width_col='img_size_near_img_width', height_col='img_size_near_img_height')
    df['image_quality_far_img'] = df.apply(transform_img_quality, axis=1, width_col='img_size_far_img_width', height_col='img_size_near_img_height')
    df['same_image_quality'] = df.apply(compare_columns, axis=1, col1='image_quality_far_img', col2='image_quality_near_img')
    columns_to_fill = df.filter(regex='time').columns.tolist()
    df = fillna_columns(df, columns_to_fill, value=0)
    columns_to_fill = df.filter(regex='prob').columns.tolist()
    df = fillna_columns(df, columns_to_fill, value=-1) # -1 means that the value is unknown due to this is probability is not calculated
    columns_to_fill = df.filter(regex='score').columns.tolist()
    df = fillna_columns(df, columns_to_fill, value=-2) # -2 means that the value is unknown due to this is score is not calculated (this value in order to not affect score metrics)

    columns_to_sum = df.filter(regex='time_ms_accessory_detector').columns.tolist()
    df['total_time_ms_accessory_detector'] = df.apply(lambda row: sum_columns(row, columns_to_sum), axis=1)
    df = df.drop(columns=columns_to_sum)

    columns_to_sum = df.filter(regex='time_ms_face_detector').columns.tolist()
    df['total_time_ms_face_detector'] = df.apply(lambda row: sum_columns(row, columns_to_sum), axis=1)
    df = df.drop(columns=columns_to_sum)

    columns_to_sum = df.filter(regex='time_ms_liqe').columns.tolist()
    df['total_time_ms_liqe'] = df.apply(lambda row: sum_columns(row, columns_to_sum), axis=1)
    df = df.drop(columns=columns_to_sum)

    columns_to_sum = df.filter(regex='time_ms_topiq').columns.tolist()
    df['total_time_ms_topiq'] = df.apply(lambda row: sum_columns(row, columns_to_sum), axis=1)
    df = df.drop(columns=columns_to_sum)

    columns_to_sum = df.filter(regex='time_ms_classic_metrics').columns.tolist()
    df['total_time_ms_classic_metrics'] = df.apply(lambda row: sum_columns(row, columns_to_sum), axis=1)
    df = df.drop(columns=columns_to_sum)

    columns_to_sum = df.filter(regex='time_ms_as_35_selfies').columns.tolist()
    df['total_time_ms_as_35_selfies'] = df.apply(lambda row: sum_columns(row, columns_to_sum), axis=1)
    df = df.drop(columns=columns_to_sum)

    columns_to_sum = df.filter(regex='time_ms_ibeta2_crops').columns.tolist()
    df['total_time_ms_ibeta2_crops'] = df.apply(lambda row: sum_columns(row, columns_to_sum), axis=1)
    df = df.drop(columns=columns_to_sum)

    columns_to_sum = df.filter(regex='time_ms_ibeta2_full').columns.tolist()
    df['total_time_ms_ibeta2_full'] = df.apply(lambda row: sum_columns(row, columns_to_sum), axis=1)
    df = df.drop(columns=columns_to_sum)

    columns_to_sum = df.filter(regex='time_ms_ibeta2_clip').columns.tolist()
    df['total_time_ms_ibeta2_clip'] = df.apply(lambda row: sum_columns(row, columns_to_sum), axis=1)
    df = df.drop(columns=columns_to_sum)

    columns_to_sum = df.filter(regex='time_ms_geometry_check').columns.tolist()
    df['total_time_ms_geometry_check'] = df.apply(lambda row: sum_columns(row, columns_to_sum), axis=1)
    df = df.drop(columns=columns_to_sum)

    columns_to_sum = df.filter(regex='total_time_ms_').columns.tolist()
    df['total_time_ms_all_mircroservices'] = df.apply(lambda row: sum_columns(row, columns_to_sum), axis=1)

    df = subtract_columns(df, 'total_time_ms', 'total_time_ms_all_mircroservices', 'total_time_ms_other_mircroservices')

    column_order = ['api_hash', 'user_name', 'file_token', 'service', 'summary_code',
        'summary_status', 'summary_desc', 'http_code',
        'procesoConvenioGuid', 'convenio', 'documento',
        'img_size_near_img_width', 'img_size_near_img_height',
        'img_size_far_img_width', 'img_size_far_img_height','image_quality_near_img', 'image_quality_far_img','same_image_quality',
        'aspect_ratio_near_img', 'aspect_ratio_far_img','same_aspect_ratio',
        'score_liqe_near_img', 'score_liqe_far_img', 'score_topiq_near_img',
        'score_topiq_far_img', 'score_sharpness_classic_metrics_near_img',
        'score_sharpness_classic_metrics_far_img',
        'score_colorfulness_classic_metrics_near_img',
        'score_colorfulness_classic_metrics_far_img',
        'score_contrast_classic_metrics_near_img',
        'score_contrast_classic_metrics_far_img',
        'score_brightness_classic_metrics_near_img',
        'score_brightness_classic_metrics_far_img',
        'score_blur_score_classic_metrics_near_img',
        'score_blur_score_classic_metrics_far_img',
        'score_svd_score_classic_metrics_near_img',
        'score_svd_score_classic_metrics_far_img',
        'prob_as_35_selfies_near_img', 'prob_as_35_selfies_far_img',
        'prob_ibeta2_crops_near_img', 'prob_ibeta2_crops_far_img',
        'prob_ibeta2_full_near_img', 'prob_ibeta2_full_far_img',
        'prob_ibeta2_clip_near_img', 'prob_ibeta2_clip_far_img',
        'prob_as_heuristics_near_img', 'prob_as_heuristics_far_img','date',
        'date_year', 'date_month', 'date_day','date_day_off_week', 'date_hour', 'date_minute',
        'total_time_ms_accessory_detector','total_time_ms_face_detector', 'total_time_ms_liqe',
        'total_time_ms_topiq', 'total_time_ms_classic_metrics',
        'total_time_ms_as_35_selfies', 'total_time_ms_ibeta2_crops',
        'total_time_ms_ibeta2_full', 'total_time_ms_ibeta2_clip',
        'total_time_ms_geometry_check', 'total_time_ms_all_mircroservices',
        'total_time_ms_other_mircroservices', 'total_time_ms']
    
    df = df.reindex(columns=column_order)
    print(f"DataFrame Transform with retries len: {len(df)}")
    # df.head()

    df_no_retrys = groupby_tail_sorted(df, "procesoConvenioGuid", "date")
    print(f"DataFrame Transform with no retries len: {len(df_no_retrys)}")
    df_no_retrys.head()

    #df_no_retrys.to_csv("../data/processed/transform_data_no_retries.csv", index=False)
    #df.to_csv("../data/processed/transform_data.csv", index=False)

    save_to_db(df, db_config, "etl_db_final_workshop", "etl_transform_table_final_workshop")
    save_to_db(df_no_retrys, db_config, "etl_db_final_workshop", "etl_transform_no_retry_table_final_workshop")

# Configuración de argparse para recibir el archivo CSV de entrada
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL Script")
    parser.add_argument("-i","--input_csv", type=str, help="Ruta del archivo CSV posterior de ser procesado por 000_anonymized.py")
    args = parser.parse_args()
    
    run_etl(args.input_csv)