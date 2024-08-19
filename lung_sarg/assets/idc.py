"""NIH Imaging Data Commons (IDC) dataset assets."""

import os
from pathlib import Path

import pandas as pd
import polars as pl
from dagster import asset, get_dagster_logger

import itk

from ..resources import IDCNSCLCRadiogenomicSampler, PRE_STAGED_DIR, STAGED_DIR, NSCLC_RADIOGENOMICS_COLLECTION_NAME

log = get_dagster_logger()

@asset()
def idc_nsclc_radiogenomic_samples(idc_nsclc_radiogenomic_sampler: IDCNSCLCRadiogenomicSampler) -> pl.DataFrame:
    """
    IDC NSCLC Radiogenomic Samples. Samples are placed in data/pre-staged/nsclc-radiogenomics-samples.
    """
    return idc_nsclc_radiogenomic_sampler.get_samples()


@asset()
def staged_idc_nsclc_radiogenomic_samples(idc_nsclc_radiogenomic_samples) -> None:
    """
    Staged IDC NSCLC Radiogenomic Samples. Samples are placed in data/staged/nsclc-radiogenomics-samples.
    """

    for row in idc_nsclc_radiogenomic_samples.iter_rows(named=True):
        patient_id = row['Patient ID']
        patient_pre_staged_path = PRE_STAGED_DIR / NSCLC_RADIOGENOMICS_COLLECTION_NAME / patient_id

        image_series_path = patient_pre_staged_path / 'image-series.json'
        image_series = pd.read_json(image_series_path)

        studies = image_series['StudyInstanceUID'].drop_duplicates()
        for study in studies:
            series = image_series.loc[image_series['StudyInstanceUID'] == study]

            studies_table = pd.DataFrame(columns=['Patient ID', 'Study Instance UID', 'Study Date', 'Study Description'])
            studies_table.astype({'Patient ID': 'string', 'Study Instance UID': 'string', 'Study Date': 'string', 'Study Description': 'string'})

            series_table = pd.DataFrame(columns=['Patient ID', 'Study Instance UID', 'Series Instance UID', 'Series Number', 'Modality', 'Body Part Examined', 'Series Description'])
            series_table.astype({'Patient ID': 'string', 'Study Instance UID': 'string', 'Series Instance UID': 'string', 'Series Number': 'int32', 'Modality': 'string', 'Body Part Examined': 'string', 'Series Description': 'string'})

            for series_id, ds in series.iterrows():
                patient_staged_path = STAGED_DIR / NSCLC_RADIOGENOMICS_COLLECTION_NAME / "dagster" / patient_id / ds.StudyInstanceUID
                os.makedirs(patient_staged_path, exist_ok=True)

                dicom_path = patient_pre_staged_path / 'dicom' / ds.PatientID / ds.StudyInstanceUID / ds.SeriesInstanceUID
                files = os.listdir(dicom_path)
                try:
                    if len(files) == 0:
                        continue
                    elif len(files) == 1:
                        image = itk.imread(dicom_path / files[0])
                    else:
                    # series
                        image = itk.imread(dicom_path)
                except Exception:
                    continue
                meta = dict(image)
                study_date = meta.get('0008|0020', '00000000')
                study_date = f"{study_date[4:6]}-{study_date[6:8]}-{study_date[:4]}"
                study_description = meta.get('0008|1030', '')
                series_number = meta.get('0020|0011', '0')
                series_number = int(series_number)
                modality = meta.get('0008|0060', '').strip()
                body_part_examined = meta.get('0018|0015', '')
                series_description = meta.get('0008|103e', '')

                studies_table.loc[len(studies_table)] = {'Patient ID': ds.PatientID,
                                            'Study Instance UID': ds.StudyInstanceUID,
                                            'Study Date': study_date,
                                            'Study Description': study_description}
                series_table.loc[len(series_table)] = { 'Patient ID': ds.PatientID,
                                            'Study Instance UID': ds.StudyInstanceUID,
                                            'Series Instance UID': ds.SeriesInstanceUID,
                                            'Series Number': series_number,
                                            'Modality': modality,
                                            'Body Part Examined': body_part_examined,
                                            'Series Description': series_description }

                nifti_path = patient_staged_path / 'nifti' / ds.SeriesInstanceUID
                os.makedirs(nifti_path, exist_ok=True)
                itk.imwrite(image, nifti_path / 'image.nii.gz')

                with open(patient_staged_path / 'study.json', 'w') as fp:
                    fp.write(studies_table.to_json())

                with open(patient_staged_path / 'series.json', 'w') as fp:
                    fp.write(series_table.to_json())

                with open(patient_staged_path / 'patient.json', 'w') as fp:
                    fp.write(pd.DataFrame({0: row}).to_json())

                # itk_so_enums = itk.SpatialOrientationEnums  # shortens next line
                # dicom_lps = itk_so_enums.ValidCoordinateOrientations_ITK_COORDINATE_ORIENTATION_RAI
                # if image.dtype == np.int32 or image.dtype == np.uint32:
                #     image = image.astype(np.float32)
                # oriented_image = itk.orient_image_filter(image, use_image_direction=False, desired_coordinate_orientation=dicom_lps)

                # ngff_image = ngff_zarr.itk_image_to_ngff_image(oriented_image)

                # multiscales = ngff_zarr.to_multiscales(ngff_image, chunks=64, method=ngff_zarr.Methods.DASK_IMAGE_GAUSSIAN)
                # ome_zarr_path = Path('ome-zarr') / ds.PatientID / ds.StudyInstanceUID / ds.SeriesInstanceUID
                # os.makedirs(ome_zarr_path, exist_ok=True)
                # ngff_zarr.to_ngff_zarr(ome_zarr_path  / 'image.ome.zarr', multiscales)
