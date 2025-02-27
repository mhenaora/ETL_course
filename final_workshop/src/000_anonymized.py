import argparse
import pandas as pd
import os
import glob
import json
import re
import pydash


def anonymize_category(column,sufix):
    unique_values = column.unique()
    mapping = {value: f'{sufix}_{i+1}' for i, value in enumerate(unique_values)}
    return column.map(mapping)

def get_item(data, path, keys_to_extract=None, default=-1):
    if isinstance(path, str):
        paths = [path]
    else:
        paths = path

    for p in paths:
        keys = p.split('.')
        val = data
        for key in keys:
            if isinstance(val, dict):
                val = val.get(key, default)
            elif isinstance(val, list):
                try:
                    key = int(key)
                    val = val[key]
                except (ValueError, IndexError):
                    val = default
                    break
            else:
                val = default
                break

        if val != default:
            break

    if isinstance(val, list):
        result = []
        for item in val:
            if isinstance(item, dict):
                if keys_to_extract is None:
                    result.append(item)
                else:
                    filtered_item = {key: item[key] for key in keys_to_extract if key in item}
                    result.append(filtered_item)
            else:
                result.append(item)
        return result
    elif isinstance(val, dict):
        if keys_to_extract is None:
            return val
        else:
            return {key: val[key] for key in keys_to_extract if key in val}
    else:
        return val


def main(input_json, output_csv):

    with open(input_json, 'r') as f:
        data_list = json.load(f)

    # Lista de diccionarios con los keys que quieres extraer
    data_to_extract = []
    for data in data_list:
        extracted_data = {
            #OVERALL DATA
            'api_hash': pydash.get(data, 'api_hash'),
            'user_name': pydash.get(data, 'user_name'),
            'file_token': pydash.get(data, 'file_token'),
            'service': pydash.get(data, 'service'),
            'summary_code': pydash.get(data, 'summary.code'),
            'summary_status': pydash.get(data, 'summary.status'),
            'summary_desc': pydash.get(data, 'summary.desc'),
            'http_code': pydash.get(data, 'http_code'),
            'total_time_ms': pydash.get(data, 'total_time_ms'),
            'date': pydash.get(data, 'date'),
            
            #USER DATA
            'procesoConvenioGuid': pydash.get(data, 'user_data.procesoConvenioGuid'),
            'convenio': pydash.get(data, 'user_data.convenio'),
            'documento': pydash.get(data, 'user_data.documento'),

            #IMG METADATA INTEGRITY DIMS
            'img_size_near_img_width': pydash.get(data, 'metadata.near_img.integrity_dims.results.raw_img_size.0'),
            'img_size_near_img_height': pydash.get(data, 'metadata.near_img.integrity_dims.results.raw_img_size.1'),
            'img_size_far_img_width': pydash.get(data, 'metadata.far_img.integrity_dims.results.raw_img_size.0'),
            'img_size_far_img_height': pydash.get(data, 'metadata.far_img.integrity_dims.results.raw_img_size.1'),

            # SCORES IQA
            'score_liqe_near_img': pydash.get(data, 'metadata.near_img.liqe.results.predictions.score'),
            'score_liqe_far_img': pydash.get(data, 'metadata.far_img.liqe.results.predictions.score'),

            'score_topiq_near_img': pydash.get(data, 'metadata.near_img.topiq.results.predictions.score'),
            'score_topiq_far_img': pydash.get(data, 'metadata.far_img.topiq.results.predictions.score'),

            'score_sharpness_classic_metrics_near_img': pydash.get(data, 'metadata.near_img.classic_metrics.results.sharpness'),
            'score_sharpness_classic_metrics_far_img': pydash.get(data, 'metadata.far_img.classic_metrics.results.sharpness'),

            'score_colorfulness_classic_metrics_near_img': pydash.get(data, 'metadata.near_img.classic_metrics.results.colorfulness'),
            'score_colorfulness_classic_metrics_far_img': pydash.get(data, 'metadata.far_img.classic_metrics.results.colorfulness'),

            'score_contrast_classic_metrics_near_img': pydash.get(data, 'metadata.near_img.classic_metrics.results.contrast'),
            'score_contrast_classic_metrics_far_img': pydash.get(data, 'metadata.far_img.classic_metrics.results.contrast'),

            'score_brightness_classic_metrics_near_img': pydash.get(data, 'metadata.near_img.classic_metrics.results.brightness'),
            'score_brightness_classic_metrics_far_img': pydash.get(data, 'metadata.far_img.classic_metrics.results.brightness'),

            'score_blur_score_classic_metrics_near_img': pydash.get(data, 'metadata.near_img.classic_metrics.results.blur_score'),
            'score_blur_score_classic_metrics_far_img': pydash.get(data, 'metadata.far_img.classic_metrics.results.blur_score'),

            'score_svd_score_classic_metrics_near_img': pydash.get(data, 'metadata.near_img.classic_metrics.results.svd_score'),
            'score_svd_score_classic_metrics_far_img': pydash.get(data, 'metadata.far_img.classic_metrics.results.svd_score'),

            # SCORES AS
            'prob_as_35_selfies_near_img': pydash.get(data, 'metadata.near_img.as_35_selfies.results.predictions.prob'),
            'prob_as_35_selfies_far_img': pydash.get(data, 'metadata.far_img.as_35_selfies.results.predictions.prob'),

            'prob_ibeta2_crops_near_img': pydash.get(data, 'metadata.near_img.ibeta2_crops.results.predictions.prob'),
            'prob_ibeta2_crops_far_img': pydash.get(data, 'metadata.far_img.ibeta2_crops.results.predictions.prob'),

            'prob_ibeta2_full_near_img': pydash.get(data, 'metadata.near_img.ibeta2_full.results.predictions.prob'),
            'prob_ibeta2_full_far_img': pydash.get(data, 'metadata.far_img.ibeta2_full.results.predictions.prob'),

            'prob_ibeta2_clip_near_img': pydash.get(data, 'metadata.near_img.ibeta2_clip.results.predictions.prob'),
            'prob_ibeta2_clip_far_img': pydash.get(data, 'metadata.far_img.ibeta2_clip.results.predictions.prob'),

            'prob_as_heuristics_near_img': pydash.get(data, 'metadata.near_img.as_heuristics.results.predictions.prob'),
            'prob_as_heuristics_far_img': pydash.get(data, 'metadata.far_img.as_heuristics.results.predictions.prob'),

            # TIME 
            'time_ms_accessory_detector_near_img': pydash.get(data, 'metadata.near_img.accessory_detector.results.predictions.time_ms'),
            'time_ms_accessory_detector_far_img': pydash.get(data, 'metadata.far_img.accessory_detector.results.predictions.time_ms'),
            'queue_time_ms_accessory_detector_near_img': pydash.get(data, 'metadata.near_img.accessory_detector.results.predictions.queue_time_ms'),
            'queue_time_ms_accessory_detector_far_img': pydash.get(data, 'metadata.far_img.accessory_detector.results.predictions.queue_time_ms'),

            'time_ms_face_detector_near_img': pydash.get(data, 'metadata.near_img.face_detector.results.predictions.time_ms'),
            'time_ms_face_detector_far_img': pydash.get(data, 'metadata.far_img.face_detector.results.predictions.time_ms'),
            'queue_time_ms_face_detector_near_img': pydash.get(data, 'metadata.near_img.face_detector.results.predictions.queue_time_ms'),
            'queue_time_ms_face_detector_far_img': pydash.get(data, 'metadata.far_img.face_detector.results.predictions.queue_time_ms'),

            'time_ms_liqe_near_img': pydash.get(data, 'metadata.near_img.liqe.results.predictions.time_ms'),
            'time_ms_liqe_far_img': pydash.get(data, 'metadata.far_img.liqe.results.predictions.time_ms'),
            'queue_time_ms_liqe_near_img': pydash.get(data, 'metadata.near_img.liqe.results.predictions.queue_time_ms'),
            'queue_time_ms_liqe_far_img': pydash.get(data, 'metadata.far_img.liqe.results.predictions.queue_time_ms'),

            'time_ms_topiq_near_img': pydash.get(data, 'metadata.near_img.topiq.results.predictions.time_ms'),
            'time_ms_topiq_far_img': pydash.get(data, 'metadata.far_img.topiq.results.predictions.time_ms'),
            'queue_time_ms_topiq_near_img': pydash.get(data, 'metadata.near_img.topiq.results.predictions.queue_time_ms'),
            'queue_time_ms_topiq_far_img': pydash.get(data, 'metadata.far_img.topiq.results.predictions.queue_time_ms'),

            'time_ms_classic_metrics_near_img': pydash.get(data, 'metadata.near_img.classic_metrics.results.time_ms'),
            'time_ms_classic_metrics_far_img': pydash.get(data, 'metadata.far_img.classic_metrics.results.time_ms'),

            'time_ms_as_35_selfies_near_img': pydash.get(data, 'metadata.near_img.as_35_selfies.results.predictions.time_ms'),
            'time_ms_as_35_selfies_far_img': pydash.get(data, 'metadata.far_img.as_35_selfies.results.predictions.time_ms'),
            'queue_time_ms_as_35_selfies_near_img': pydash.get(data, 'metadata.near_img.as_35_selfies.results.predictions.queue_time_ms'),
            'queue_time_ms_as_35_selfies_far_img': pydash.get(data, 'metadata.far_img.as_35_selfies.results.predictions.queue_time_ms'),

            'time_ms_ibeta2_crops_near_img': pydash.get(data, 'metadata.near_img.ibeta2_crops.results.predictions.time_ms'),
            'time_ms_ibeta2_crops_far_img': pydash.get(data, 'metadata.far_img.ibeta2_crops.results.predictions.time_ms'),
            'queue_time_ms_ibeta2_crops_near_img': pydash.get(data, 'metadata.near_img.ibeta2_crops.results.predictions.queue_time_ms'),
            'queue_time_ms_ibeta2_crops_far_img': pydash.get(data, 'metadata.far_img.ibeta2_crops.results.predictions.queue_time_ms'),

            'time_ms_ibeta2_full_near_img': pydash.get(data, 'metadata.near_img.ibeta2_full.results.predictions.time_ms'),
            'time_ms_ibeta2_full_far_img': pydash.get(data, 'metadata.far_img.ibeta2_full.results.predictions.time_ms'),
            'queue_time_ms_ibeta2_full_near_img': pydash.get(data, 'metadata.near_img.ibeta2_full.results.predictions.queue_time_ms'),
            'queue_time_ms_ibeta2_full_far_img': pydash.get(data, 'metadata.far_img.ibeta2_full.results.predictions.queue_time_ms'),

            'time_ms_ibeta2_clip_near_img': pydash.get(data, 'metadata.near_img.ibeta2_clip.results.predictions.time_ms'),
            'time_ms_ibeta2_clip_far_img': pydash.get(data, 'metadata.far_img.ibeta2_clip.results.predictions.time_ms'),
            'queue_time_ms_ibeta2_clip_near_img': pydash.get(data, 'metadata.near_img.ibeta2_clip.results.predictions.queue_time_ms'),
            'queue_time_ms_ibeta2_clip_far_img': pydash.get(data, 'metadata.far_img.ibeta2_clip.results.predictions.queue_time_ms'),

            'time_ms_geometry_check': pydash.get(data, 'metadata.geometry_check.results.predictions.time_ms'),
            'queue_time_ms_geometry_check': pydash.get(data, 'metadata.geometry_check.results.predictions.queue_time_ms'),
        }
        data_to_extract.append(extracted_data)    
    df = pd.DataFrame(data_to_extract)   
    os.makedirs(os.path.dirname(output_csv), exist_ok=True)
    try:
        print(f"images before anonymize features len df : {len(df)}")

        df['user_name'] = anonymize_category(df['user_name'],"user_name")
        df['procesoConvenioGuid'] = anonymize_category(df['procesoConvenioGuid'],"procesoConvenioGuid")
        df['convenio'] = anonymize_category(df['convenio'],"convenio")
        df['documento'] = anonymize_category(df['documento'],"documento")

        print(f"images after anonymize features len df : {len(df)}")
        df.to_csv(output_csv, index=False)
        print(f'total images after filters: {len(df)} saved at {output_csv}')
    except KeyError as e:
        print(f"Error: {e}. El DataFrame no contiene la clave '{e.args[0]}'.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Procesar un archivo JSON y guardar la información en un archivo CSV.')
    parser.add_argument('-i', '--input', required=True, help='Ruta del archivo JSON de entrada.')
    parser.add_argument('-o', '--output', required=True, help='Ruta del archivo CSV de salida.')
    args = parser.parse_args()

    main(args.input, args.output)