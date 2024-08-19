import io

import httpx
import polars as pl
from dagster import AssetExecutionContext, Backoff, RetryPolicy, asset

@asset(
    retry_policy=RetryPolicy(max_retries=5, delay=2, backoff=Backoff.EXPONENTIAL),
)
def wikidata_asteroids() -> pl.DataFrame:
    """
    Wikidata asteroids data.
    """
    url = "https://query.wikidata.org/sparql"
    query = """
        SELECT
            ?asteroidLabel
            ?discovered
            ?discovererLabel
        WHERE {
            ?asteroid wdt:P31 wd:Q3863;  # Retrieve instances of "asteroid"
                        wdt:P61 ?discoverer; # Retrieve discoverer of the asteroid
                        wdt:P575 ?discovered; # Retrieve discovered date of the asteroid
            SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
        }
        ORDER BY DESC(?discovered)
    """

    response = httpx.get(
        url, headers={"Accept": "text/csv"}, params={"query": query}, timeout=30
    )

    df = pl.read_csv(io.StringIO(response.content.decode("utf-8")))

    return df
