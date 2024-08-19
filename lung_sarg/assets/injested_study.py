"""Injest staged study data into a collection's archive.

Staged study data can either come from the IDC or from the web browser portal.
"""

import os
from pathlib import Path
import shutil

import pandas as pd
import polars as pl
from dagster import asset, get_dagster_logger, Config
import numpy as np
from slugify import slugify

import ngff_zarr
import itk

from ..resources import STAGED_DIR, INJESTED_DIR, COLLECTIONS_DIR, CollectionTables

log = get_dagster_logger()

class StagedStudyConfig(Config):
    """Configuration for the staged study asset."""
    collection_name: str
    uploader: str
    study_id: str
    patient_id: str

def config_to_dataframe(config: StagedStudyConfig) -> pd.DataFrame:
    return pl.from_pandas(pd.DataFrame([config]))

def clean_column_name(name: str) -> str:
    return slugify(name.replace('%', 'percent'), separator='_')

def convert_date_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converts columns of a pandas dataframe with `date` in their name and contents that are of the form M/D/YYYY to YYYY-MM-DD.
    """
    date_columns = [col for col in df.columns if 'date' in col.lower()]
    for col in date_columns:
        df[col] = pd.to_datetime(df[col], format='mixed', errors='coerce').dt.strftime('%Y-%m-%d')
    return df

@asset()
def injested_study(config: StagedStudyConfig, collection_tables: CollectionTables) -> pl.DataFrame:
    """
    Injested study data.
    """
    staged_study_path = STAGED_DIR / config.collection_name / config.uploader / config.patient_id / config.study_id

    collection_path = COLLECTIONS_DIR / config.collection_name / config.patient_id


    patient = pd.read_json(staged_study_path / 'patient.json', orient='index')
    patient = patient.rename(columns=clean_column_name)
    patient = convert_date_columns(patient)
    log.info(f"Injesting study for patient: {config.patient_id}")
    collection_tables.insert_into_collection(config.collection_name, 'patients', patient)

    study = pd.read_json(staged_study_path / 'study.json', orient='rows')
    study = study.rename(columns=clean_column_name)
    study = convert_date_columns(study)
    collection_tables.insert_into_collection(config.collection_name, 'studies', study)

    series = pd.read_json(staged_study_path / 'series.json', orient='rows')
    series = series.rename(columns=clean_column_name)
    collection_tables.insert_into_collection(config.collection_name, 'series', series)

    for _, ds in series.iterrows():
        series_id = ds['series_instance_uid']
        nifti_file = staged_study_path / 'nifti' / series_id / 'image.nii.gz'

        if nifti_file.exists():
            collection_nifti_path = collection_path / 'nifti' / config.study_id / series_id
            os.makedirs(collection_nifti_path, exist_ok=True)
            shutil.copyfile(nifti_file, collection_nifti_path / 'image.nii.gz')

            image = itk.imread(nifti_file)

            itk_so_enums = itk.SpatialOrientationEnums  # shortens next line
            dicom_lps = itk_so_enums.ValidCoordinateOrientations_ITK_COORDINATE_ORIENTATION_RAI
            if image.dtype == np.int32 or image.dtype == np.uint32:
                image = image.astype(np.float32)
            oriented_image = itk.orient_image_filter(image, use_image_direction=False, desired_coordinate_orientation=dicom_lps)

            ngff_image = ngff_zarr.itk_image_to_ngff_image(oriented_image)

            multiscales = ngff_zarr.to_multiscales(ngff_image, chunks=64, method=ngff_zarr.Methods.DASK_IMAGE_GAUSSIAN)
            collection_ome_zarr_path = collection_path / 'ome-zarr' / config.study_id / series_id
            os.makedirs(collection_ome_zarr_path, exist_ok=True)
            ngff_zarr.to_ngff_zarr(collection_ome_zarr_path  / 'image.ome.zarr', multiscales)

    staged_study_path = STAGED_DIR / config.collection_name / config.uploader / config.patient_id / config.study_id
    injested_patient_path = INJESTED_DIR / config.collection_name / config.uploader / config.patient_id
    os.makedirs(injested_patient_path, exist_ok=True)
    shutil.move(staged_study_path, injested_patient_path)

    return config_to_dataframe(config)

@asset()
def collection_parquets(collection_tables: CollectionTables) -> None:
    """
    Export collection data to parquet files.
    """
    collection_tables.write_collection_parquets()