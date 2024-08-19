import os
import json
import datetime
import tempfile
from typing import Optional, List
from pathlib import Path
import subprocess
import shutil

import yaml
import httpx
import polars as pl
import pandas as pd
from dagster import InitResourceContext, ConfigurableResource, get_dagster_logger
from dagster_duckdb import DuckDBResource
from pydantic import PrivateAttr
from tenacity import retry, wait_exponential, stop_after_attempt
from huggingface_hub import HfApi
from pyarrow.csv import read_csv

log = get_dagster_logger()

DBT_PROJECT_DIR = str(Path(__file__).parent.resolve() / ".." / "dbt")
DATA_DIR = Path(__file__).parent.resolve() / ".." / "data"
PRE_STAGED_DIR = DATA_DIR / "pre-staged"
STAGED_DIR = DATA_DIR / "staged"
INJESTED_DIR = DATA_DIR / "injested"
COLLECTIONS_DIR = DATA_DIR / "collections"
DATABASE_PATH = os.getenv("DATABASE_PATH", str(DATA_DIR / "database.duckdb"))

NSCLC_RADIOGENOMICS_COLLECTION_NAME = "nsclc_radiogenomics"

collection_table_names = {"patients", "studies", "series"}
class CollectionTables(ConfigurableResource):
    duckdb: DuckDBResource
    collection_names: List[str] = [NSCLC_RADIOGENOMICS_COLLECTION_NAME]

    def setup_for_execution(self, context: InitResourceContext) -> None:
        os.makedirs(COLLECTIONS_DIR, exist_ok=True)
        self._db = self.duckdb

        with self._db.get_connection() as conn:
            for collection_name in self.collection_names:
                collection_path = COLLECTIONS_DIR / collection_name
                os.makedirs(collection_path, exist_ok=True)

                for table in collection_table_names:
                    table_parquet = collection_path / f"{table}.parquet"
                    table_name = f"{collection_name}_{table}"
                    if table_parquet.exists():
                        conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM parquet_scan('{table_parquet}')")
                    else:
                        if table == "patients":
                            conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (patient_id VARCHAR, patient_affiliation VARCHAR, age_at_histological_diagnosis BIGINT, weight_lbs VARCHAR, gender VARCHAR, ethnicity VARCHAR, smoking_status VARCHAR, pack_years VARCHAR, quit_smoking_year BIGINT, percentgg VARCHAR, tumor_location_choice_rul VARCHAR, tumor_location_choice_rml VARCHAR, tumor_location_choice_rll VARCHAR, tumor_location_choice_lul VARCHAR, tumor_location_choice_lll VARCHAR, tumor_location_choice_l_lingula VARCHAR, tumor_location_choice_unknown VARCHAR, histology VARCHAR, pathological_t_stage VARCHAR, pathological_n_stage VARCHAR, pathological_m_stage VARCHAR, histopathological_grade VARCHAR, lymphovascular_invasion VARCHAR, pleural_invasion_elastic_visceral_or_parietal VARCHAR, egfr_mutation_status VARCHAR, kras_mutation_status VARCHAR, alk_translocation_status VARCHAR, adjuvant_treatment VARCHAR, chemotherapy VARCHAR, radiation VARCHAR, recurrence VARCHAR, recurrence_location VARCHAR, date_of_recurrence DATE, date_of_last_known_alive DATE, survival_status VARCHAR, date_of_death DATE, time_to_death_days BIGINT, ct_date DATE, days_between_ct_and_surgery BIGINT, pet_date DATE);")
                        elif table == "studies":
                            conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (patient_id VARCHAR, study_instance_uid VARCHAR, study_date DATE, study_description VARCHAR);")
                        elif table == "series":
                            conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (patient_id VARCHAR, study_instance_uid VARCHAR, series_instance_uid VARCHAR, series_number BIGINT, modality VARCHAR, body_part_examined VARCHAR, series_description VARCHAR);")

    def teardown_after_execution(self, context: InitResourceContext) -> None:
        with self._db.get_connection() as conn:
            conn.execute("VACUUM")

    def write_collection_parquets(self):
        with self._db.get_connection() as conn:
            for collection_name in self.collection_names:
                collection_path = COLLECTIONS_DIR / collection_name
                for table in collection_table_names:
                    table_name = f"{collection_name}_{table}"
                    table_parquet = collection_path / f"{table}.parquet"
                    conn.execute(f"COPY {table_name} TO '{table_parquet}' (FORMAT 'parquet')")

    def insert_into_collection(self, collection_name: str, table_name: str, df: pd.DataFrame):
        if df.empty:
            return
        if collection_name not in self.collection_names:
            raise ValueError(f"Collection {collection_name} not found")
        if table_name not in collection_table_names:
            raise ValueError(f"Table {table_name} not found")

        with self._db.get_connection() as conn:
            conn.execute(f"INSERT INTO {collection_name}_{table_name} SELECT * FROM df")

class IDCNSCLCRadiogenomicSampler(ConfigurableResource):
    n_samples: int = 1

    def get_samples(self) -> pl.DataFrame:
        manifest_path = DATA_DIR / "idc-nsclc-radiogenomics-sampler"
        patients_path = manifest_path / 'NSCLCR01Radiogenomic_DATA_LABELS_2018-05-22_1500-shifted.csv'
        patients_table = pl.from_arrow(read_csv(patients_path))

        samples = patients_table.sample(self.n_samples)

        images_manifest_path = manifest_path / 'idc_manifest_full_table.csv'
        images_table = read_csv(images_manifest_path).to_pandas()

        for row in samples.iter_rows(named=True):
            log.info(f"Fetching images for patient {row['Patient ID']}")
            output_path = PRE_STAGED_DIR / NSCLC_RADIOGENOMICS_COLLECTION_NAME / row['Patient ID']

            if output_path.exists():
                log.info(f"Patient {row['Patient ID']} already exists")
                continue

            os.makedirs(output_path, exist_ok=True)
            with open(output_path / 'patient.json', 'w') as fp:
                fp.write(pd.DataFrame({0: row}).to_json())

            images_path = output_path / 'dicom'
            images_path.mkdir(exist_ok=True)

            series = images_table.loc[images_table['PatientID'] == 'AMC-001', ['PatientID', 'StudyInstanceUID', 'SeriesInstanceUID', 'crdc_study_uuid', 'crdc_series_uuid']]
            with open(output_path / 'image-series.json', 'w') as fp:
                fp.write(pd.DataFrame(series).to_json())

            for _, ds in series.iterrows():
                dicom_path = images_path / ds.PatientID / ds.StudyInstanceUID / ds.SeriesInstanceUID
                os.makedirs(dicom_path, exist_ok=True)

                command = ["s5cmd",
                    "--no-sign-request",
                    "--endpoint-url",
                    "https://s3.amazonaws.com",
                    "cp",
                    f"s3://idc-open-data/{ds.crdc_series_uuid}/*",
                    "."]
                subprocess.check_call(command, cwd=dicom_path, stdout=subprocess.DEVNULL)

        return samples

class CollectionPublisher(ConfigurableResource):
    hf_token: str
    tmp_dir: str = tempfile.gettempdir()

    _api: HfApi = PrivateAttr()

    def setup_for_execution(self, context: InitResourceContext) -> None:
        self._api = HfApi(token=self.hf_token)

    def publish(
        self,
        collection_name: str,
        readme: Optional[str] = None,
        generate_datapackage: bool = False,
    ):
        with tempfile.TemporaryDirectory(dir=self.tmp_dir) as temp_dir:
            collection_path = COLLECTIONS_DIR / collection_name
            log.info(f"Copying collection {collection_name} to {temp_dir}")
            shutil.copytree(collection_path, temp_dir, dirs_exist_ok=True)

            if readme:
                readme_path = os.path.join(temp_dir, "README.md")
                with open(readme_path, "w") as readme_file:
                    readme_file.write(readme)

            if generate_datapackage:
                datapackage = {
                    "name": collection_name,
                    "resources": [
                        {"path": "patients.parquet", "format": "parquet"},
                        {"path": "studies.parquet", "format": "parquet"},
                        {"path": "series.parquet", "format": "parquet"},
                    ],
                }
                datapackage_path = os.path.join(temp_dir, "datapackage.yaml")
                with open(datapackage_path, "w") as dp_file:
                    yaml.dump(datapackage, dp_file)

            log.info(f"Uploading collection {collection_name} to Hugging Face")
            # Note: the repository has to be already created
            self._api.upload_folder(
                folder_path=temp_dir,
                repo_id=f"radiogenomics/lung_sarg_{collection_name}",
                repo_type="dataset",
                commit_message=f"Update {collection_name} collection",
            )
