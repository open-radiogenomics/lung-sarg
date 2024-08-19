import os
from pathlib import Path

from dagster import EnvVar, Definitions, load_assets_from_modules, define_asset_job
from dagster_dbt import DbtCliResource, load_assets_from_dbt_project
from dagster_duckdb_polars import DuckDBPolarsIOManager
from dagster_duckdb import DuckDBResource

from .assets import huggingface, idc, injested_study
from .resources import (
    DBT_PROJECT_DIR,
    DATABASE_PATH,
    CollectionPublisher,
    CollectionTables,
    IDCNSCLCRadiogenomicSampler
)
from .sensors import staged_study_sensor, injest_and_analyze_study_job

# dbt = DbtCliResource(project_dir=DBT_PROJECT_DIR, profiles_dir=DBT_PROJECT_DIR)
duckdb_resource = DuckDBResource(database=DATABASE_PATH)

# dbt_assets = load_assets_from_dbt_project(DBT_PROJECT_DIR, DBT_PROJECT_DIR)
dbt_assets = []
all_assets = load_assets_from_modules([idc, huggingface, injested_study])

stage_idc_nsclc_radiogenomic_samples_job = define_asset_job(
    "stage_idc_nsclc_radiogenomic_samples",
    [idc.idc_nsclc_radiogenomic_samples, idc.staged_idc_nsclc_radiogenomic_samples],
    description="Stages IDC NSCLC Radiogenomic samples",
)
jobs = [stage_idc_nsclc_radiogenomic_samples_job, injest_and_analyze_study_job]

resources = {
    # "dbt": dbt,
    "io_manager": DuckDBPolarsIOManager(database=DATABASE_PATH, schema="main"),
    "idc_nsclc_radiogenomic_sampler": IDCNSCLCRadiogenomicSampler(n_samples=2),
    "collection_publisher": CollectionPublisher(hf_token=EnvVar("HUGGINGFACE_TOKEN"), tmp_dir=str("/home/matt/data/hf")),
    "duckdb": duckdb_resource,
    "collection_tables": CollectionTables(duckdb=duckdb_resource),
}

sensors = [staged_study_sensor]

defs = Definitions(assets=[*dbt_assets, *all_assets], resources=resources, jobs=jobs, sensors=sensors)
