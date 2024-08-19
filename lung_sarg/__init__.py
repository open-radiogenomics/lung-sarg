import os
from pathlib import Path

from dagster import EnvVar, Definitions, load_assets_from_modules, define_asset_job
from dagster_dbt import DbtCliResource, load_assets_from_dbt_project
from dagster_duckdb_polars import DuckDBPolarsIOManager
from dagster_duckdb import DuckDBResource

from .assets import spain, others, indicators, huggingface, idc, injested_study
from .resources import (
    DBT_PROJECT_DIR,
    DATABASE_PATH,
    DatasetPublisher,
    CollectionTables,
    IDCNSCLCRadiogenomicSampler
)

dbt = DbtCliResource(project_dir=DBT_PROJECT_DIR, profiles_dir=DBT_PROJECT_DIR)
duckdb_resource = DuckDBResource(database=DATABASE_PATH)

dbt_assets = load_assets_from_dbt_project(DBT_PROJECT_DIR, DBT_PROJECT_DIR)
all_assets = load_assets_from_modules([idc, indicators, huggingface, others, spain, injested_study])

stage_idc_nsclc_radiogenomic_samples_job = define_asset_job(
    "stage_idc_nsclc_radiogenomic_samples",
    [idc.idc_nsclc_radiogenomic_samples, idc.staged_idc_nsclc_radiogenomic_samples],
    description="Stages IDC NSCLC Radiogenomic Samples",
)
jobs = [stage_idc_nsclc_radiogenomic_samples_job,]

resources = {
    "dbt": dbt,
    "io_manager": DuckDBPolarsIOManager(database=DATABASE_PATH, schema="main"),
    "idc_nsclc_radiogenomic_sampler": IDCNSCLCRadiogenomicSampler(n_samples=2),
    "dp": DatasetPublisher(hf_token=EnvVar("HUGGINGFACE_TOKEN")),
    "duckdb": duckdb_resource,
    "collection_tables": CollectionTables(duckdb=duckdb_resource),
}

defs = Definitions(assets=[*dbt_assets, *all_assets], resources=resources, jobs=jobs)
