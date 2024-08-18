"""NIH Imaging Data Commons (IDC) dataset assets."""

import polars as pl
from dagster import asset

from ..resources import IDCNSCLCRadiogenomicSampler

@asset()
def idc_nsclc_radiogenomic_samples(idc_nsclc_radiogenomic_sampler: IDCNSCLCRadiogenomicSampler) -> pl.DataFrame:
    """
    IDC NSCLC Radiogenomic Samples.
    """
    return idc_nsclc_radiogenomic_sampler.get_samples()