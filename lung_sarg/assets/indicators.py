import io
import zipfile

import httpx
import polars as pl
from dagster import asset
from slugify import slugify


@asset()
def world_bank_wdi() -> pl.DataFrame:
    """
    World Development Indicators (WDI) is the World Bank's premier compilation of cross-country comparable data on development.

    Bulk data download is available at https://datatopics.worldbank.org/world-development-indicators/
    """

    url = "https://databankfiles.worldbank.org/public/ddpext_download/WDI_CSV.zip"

    response = httpx.get(url)

    zipfile.ZipFile(io.BytesIO(response.content)).extractall(path="/tmp/")

    # Load the WDICSV.csv file as a DataFrame
    df = pl.read_csv("/tmp/WDICSV.csv")

    # Reshape the dataframe
    df = df.melt(
        id_vars=["Country Name", "Country Code",
                 "Indicator Name", "Indicator Code"],
        value_name="Indicator Value",
        variable_name="Year",
    )

    # Make one column per Indicator Name
    df = df.pivot(
        index=["Country Name", "Country Code", "Year"],
        columns="Indicator Name",
        values="Indicator Value",
    )

    # Cast to floats
    df = df.select(
        [
            pl.col("Country Name"),
            pl.col("Country Code"),
            pl.col("Year").cast(pl.Int32),
            *[pl.col(col).cast(pl.Float32) for col in df.columns[3:]],
        ]
    )

    # Clean column names
    df = df.rename(
        lambda column_name: slugify(
            column_name.replace("%", "percent"), separator="_")
    )

    return df
