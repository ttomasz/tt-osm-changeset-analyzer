from datetime import datetime
from typing import NamedTuple
from functools import lru_cache

import duckdb
import pandas as pd
import streamlit as st


class DatasetStats(NamedTuple):
    query: str
    min_opened_date: datetime
    max_opened_date: datetime


class StatsForYear(NamedTuple):
    query: str
    number_of_changesets: int
    number_of_unique_users: int
    number_of_object_changes: int
    number_of_comments: int


class DataFrameResult(NamedTuple):
    query: str
    df: pd.DataFrame


# init and prepare duckdb 🦆
db = duckdb.connect(database=":memory:")
db.execute("install 'httpfs'; load 'httpfs'; set s3_region='eu-central-1';")

url_template = "s3://tt-osm-changesets/full_by_year/{year}.parquet"


def get_delta(current_value: int, previous_value: int | None) -> str | None:
    if previous_value is None:
        return None
    return f"{(current_value - previous_value):+,d}"


@st.cache(hash_funcs={duckdb.DuckDBPyConnection: None})
def fetch_one(query: str) -> tuple:
    result = db.execute(query)
    return *result.fetchone(), query  # type: ignore


@st.cache(hash_funcs={duckdb.DuckDBPyConnection: None})
def fetch_df(query: str) -> DataFrameResult:
    result = db.execute(query)
    return DataFrameResult(query, result.df())


@lru_cache
def min_max_timestamps() -> DatasetStats:
    query = f"""
SELECT
    min(created_at) start_range,
    max(created_at) end_range
FROM '{url_template.format(year='*')}'
""".strip()
    result: tuple = db.execute(query).fetchone()  # type: ignore
    return DatasetStats(query, *result)


@lru_cache
def year_stats(year: int) -> StatsForYear:
    query = f"""
SELECT
    count(*) number_of_changesets,
    count(distinct uid) number_of_unique_users,
    sum(num_changes) number_of_object_changes,
    sum(comments_count) number_of_comments
FROM '{url_template.format(year=year)}'
""".strip()
    result: tuple = db.execute(query).fetchone()  # type: ignore
    return StatsForYear(query, *result)


@lru_cache
def most_popular_editors(year: int) -> DataFrameResult:
    query = f"""
SELECT
    CASE -- using [1] since in duckdb map lookups return 1-indexed array
        WHEN tags['created_by'][1] like 'iD%' then 'iD'
        WHEN tags['created_by'][1] like 'JOSM%' then 'JOSM'
        WHEN tags['created_by'][1] like 'Level0%' then 'Level0'
        WHEN tags['created_by'][1] like 'StreetComplete%' then 'StreetComplete'
        WHEN tags['created_by'][1] like 'RapiD%' then 'RapiD'
        WHEN tags['created_by'][1] like 'Potlach%' then 'Potlach'
        WHEN tags['created_by'][1] like 'Potlatch%' then 'Potlatch'
        WHEN tags['created_by'][1] like 'Go Map!!%' then 'Go Map!!'
        WHEN tags['created_by'][1] like 'Merkaartor%' then 'Merkaartor'
        WHEN tags['created_by'][1] like 'OsmAnd%' then 'OsmAnd'
        WHEN tags['created_by'][1] like 'MAPS.ME%' then 'MAPS.ME'
        WHEN tags['created_by'][1] like 'Vespucci%' then 'Vespucci'
        WHEN tags['created_by'][1] like 'Organic Maps%' then 'Organic Maps'
        WHEN tags['created_by'][1] like 'ArcGIS Editor%' then 'ArcGIS Editor'
        WHEN tags['created_by'][1] like 'bulk_upload.py%' then 'bulk_upload.py'
        WHEN tags['created_by'][1] like 'reverter%' then 'reverter'
        WHEN tags['created_by'][1] like 'Every_Door%' then 'EveryDoor'
        WHEN tags['created_by'][1] like 'osmtools%' then 'osmtools'
        WHEN tags['created_by'][1] like 'osmapi%' then 'osmapi'
        WHEN tags['created_by'][1] like 'rosemary%' then 'rosemary'
        WHEN tags['created_by'][1] like 'Globe%' then 'Globe'
        WHEN tags['created_by'][1] like 'PythonOsmApi%' then 'PythonOsmApi'
        WHEN tags['created_by'][1] like 'bot-source-cadastre.py%' then 'bot-source-cadastre.py'
        WHEN tags['created_by'][1] like 'upload.py%' then 'upload.py'
        ELSE coalesce(tags['created_by'][1], '<unknown>')
    END editor,
    sum(num_changes)::bigint number_of_object_changes, -- cast to bigint makes formatting later easier
    count(*) number_of_changesets
FROM '{url_template.format(year=year)}'
GROUP BY 1
ORDER BY 2 DESC
LIMIT 25
""".strip()
    result = db.execute(query)
    return DataFrameResult(query, result.df())


st.markdown("""
# OpenStreetMap changesets
This Streamlit app queries remote Parquet files with info from changeset dump downloaded from planet.osm.org.
Thanks  to DuckDB we can query files hosted on S3 storage without having to download everything (7 GB).
Although for running larger analyses this is a better course of action since it's order of magnitude faster.
With local files DuckDB can query all the data in seconds.

**Since changesets can be opened in one year and closed in another everywhere we assume that `year`
is the year the changeset was opened in.**

First let's see range of timestamps in our files:
""")
dataset_stats = min_max_timestamps()
with st.expander("SQL query", expanded=False):
    st.code(dataset_stats.query, language="sql")
st.metric("Minimum changeset opening datetime", dataset_stats.min_opened_date.isoformat())
st.metric("Maximum changeset opening datetime", dataset_stats.max_opened_date.isoformat())

# year = st.slider("Select year for analysis:", min_value=2005, max_value=2023, value=2023, step=1)
year_options = tuple(range(2005, 2024))
selected_year = st.selectbox("Select year for analysis:", options=year_options, index=len(year_options)-1)
previous_year = selected_year - 1 if selected_year > 2005 else None

st.markdown(f"## In {selected_year} there were:")

stats_for_year = year_stats(selected_year)
if previous_year is not None:
    stats_for_previous_year = year_stats(previous_year)
else:
    stats_for_previous_year = StatsForYear(None, None, None, None, None)
with st.expander("SQL query", expanded=False):
    st.code(stats_for_year.query, language="sql")
col1, col2 = st.columns(2)
col1.metric(
    "Changesets open",
    f"{stats_for_year.number_of_changesets:,d}",
    get_delta(stats_for_year.number_of_changesets, stats_for_previous_year.number_of_changesets)
)
col2.metric(
    "Unique users who opened a changeset",
    f"{stats_for_year.number_of_unique_users:,d}",
    get_delta(stats_for_year.number_of_unique_users, stats_for_previous_year.number_of_unique_users)
)
col1.metric(
    "Objects edited",
    f"{stats_for_year.number_of_object_changes:,d}",
    get_delta(stats_for_year.number_of_object_changes, stats_for_previous_year.number_of_object_changes)
)
col2.metric(
    "Comments in discussions under changesets",
    f"{stats_for_year.number_of_comments:,d}",
    get_delta(stats_for_year.number_of_comments, stats_for_previous_year.number_of_comments)
)

st.markdown("### Most popular editors")
dataframe_result = most_popular_editors(selected_year)
dataframe_result.df.columns = dataframe_result.df.columns.map(lambda x: str(x).replace("_", " ").title())
with st.expander("SQL query", expanded=False):
    st.code(dataframe_result.query, language="sql")
st.dataframe(data=dataframe_result.df, use_container_width=True)
