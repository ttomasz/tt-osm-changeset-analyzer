import os
from datetime import datetime
from typing import NamedTuple

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

url_template = os.environ.get("url_template", "s3://tt-osm-changesets/full_by_year/{year}.zstd.parquet")


def get_delta(current_value: int, previous_value: int | None) -> str | None:
    if previous_value is None:
        return None
    return f"{(current_value - previous_value):+,d}"


@st.experimental_memo
def fetch_one(query: str) -> tuple:
    return db.execute(query).fetchone()  # type: ignore


@st.experimental_memo
def fetch_df(query: str) -> DataFrameResult:
    result = db.execute(query)
    return DataFrameResult(query, result.df())


def min_max_timestamps() -> DatasetStats:
    query = f"""
SELECT
    min(created_at) start_range,
    max(created_at) end_range
FROM '{url_template.format(year='*')}'
""".strip()
    result = fetch_one(query)
    return DatasetStats(query, *result)


def year_stats(year: int) -> StatsForYear:
    query = f"""
SELECT
    count(*) number_of_changesets,
    count(distinct uid) number_of_unique_users,
    sum(num_changes) number_of_object_changes,
    sum(comments_count) number_of_comments
FROM '{url_template.format(year=year)}'
""".strip()
    result = fetch_one(query)
    return StatsForYear(query, *result)


def most_popular_editors(year: int) -> DataFrameResult:
    query = f"""
SELECT
    CASE
        WHEN created_by like 'iD%' then 'iD'
        WHEN created_by like 'JOSM%' then 'JOSM'
        WHEN created_by like 'Level0%' then 'Level0'
        WHEN created_by like 'StreetComplete%' then 'StreetComplete'
        WHEN created_by like 'RapiD%' then 'RapiD'
        WHEN created_by like 'Potlach%' then 'Potlach'
        WHEN created_by like 'Potlatch%' then 'Potlatch'
        WHEN created_by like 'Go Map!!%' then 'Go Map!!'
        WHEN created_by like 'Merkaartor%' then 'Merkaartor'
        WHEN created_by like 'OsmAnd%' then 'OsmAnd'
        WHEN created_by like 'MAPS.ME%' then 'MAPS.ME'
        WHEN created_by like 'Vespucci%' then 'Vespucci'
        WHEN created_by like 'Organic Maps%' then 'Organic Maps'
        WHEN created_by like 'ArcGIS Editor%' then 'ArcGIS Editor'
        WHEN created_by like 'bulk_upload.py%' then 'bulk_upload.py'
        WHEN created_by like 'reverter%' then 'reverter'
        WHEN created_by like 'Every_Door%' then 'EveryDoor'
        WHEN created_by like 'osmtools%' then 'osmtools'
        WHEN created_by like 'osmapi%' then 'osmapi'
        WHEN created_by like 'rosemary%' then 'rosemary'
        WHEN created_by like 'Globe%' then 'Globe'
        WHEN created_by like 'PythonOsmApi%' then 'PythonOsmApi'
        WHEN created_by like 'bot-source-cadastre.py%' then 'bot-source-cadastre.py'
        WHEN created_by like 'upload.py%' then 'upload.py'
        ELSE coalesce(created_by, '<unknown>')
    END editor,
    sum(num_changes)::bigint number_of_object_changes, -- cast to bigint makes formatting later easier
    count(*) number_of_changesets
FROM '{url_template.format(year=year)}'
GROUP BY 1
ORDER BY 2 DESC
LIMIT 25
""".strip()
    return fetch_df(query)


def get_sample_data(year: int) -> DataFrameResult:
    query = f"""
SELECT *
FROM '{url_template.format(year=year)}'
LIMIT 10
""".strip()
    return fetch_df(query)


def most_reported_locale(year: int) -> DataFrameResult:
    query = f"""
SELECT
    coalesce(locale, '<unknown>') reported_locale,
    count(*) number_of_changesets,
    round(
        100.0 * count(*) / (SELECT count(*) FROM '{url_template.format(year=year)}'),
        2
    ) || '%' as percentage_of_all_changesets
FROM '{url_template.format(year=year)}'
GROUP BY 1
ORDER BY 2 DESC
LIMIT 15
""".strip()
    return fetch_df(query)


st.markdown("""
# OpenStreetMap changesets
This Streamlit app queries remote Parquet files with info from changeset dump downloaded from planet.osm.org.
Thanks  to DuckDB we can query files hosted on S3 storage without having to download everything (~7 GB).
Although for running larger analyses this is a better course of action since it's order of magnitude faster.
With local files DuckDB can query all the data in seconds.

**Since changesets can be opened in one year and closed in another everywhere we assume that `year`
is the year the changeset was opened in.**

Each part shows corresponding SQL query that is executed by DuckDB.

First let's see range of timestamps in our files. Give it some time to load the data.
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

st.write("A sample of data from Parquet files:")
sample_data = get_sample_data(selected_year)
with st.expander("SQL query", expanded=False):
    st.code(sample_data.query, language="sql")
st.dataframe(data=sample_data.df, use_container_width=True)

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
editor_result = most_popular_editors(selected_year)
editor_result.df.columns = editor_result.df.columns.map(lambda x: str(x).replace("_", " ").title())
with st.expander("SQL query", expanded=False):
    st.code(editor_result.query, language="sql")
st.dataframe(data=editor_result.df, use_container_width=True)

st.markdown("### Most reported locale")
locale_result = most_reported_locale(selected_year)
locale_result.df.columns = locale_result.df.columns.map(lambda x: str(x).replace("_", " ").title())
with st.expander("SQL query", expanded=False):
    st.code(locale_result.query, language="sql")
st.dataframe(data=locale_result.df, use_container_width=True)
