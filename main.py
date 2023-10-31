import os

import duckdb
import pandas as pd
import streamlit as st

from datastructures import StatsForYear, DataFrameResult, DatasetStats
from utils import get_delta, paths_for_years


# ------------------------------------------------------------------------------------------------------
# functions
# ------------------------------------------------------------------------------------------------------
@st.experimental_memo
def fetch_one(query: str) -> tuple:
    return db.execute(query).fetchone()  # type: ignore


@st.experimental_memo
def fetch_df(query: str) -> DataFrameResult:
    result = db.execute(query)
    return DataFrameResult(query, result.arrow().to_pandas(types_mapper=pd.ArrowDtype))


def min_max_timestamps(url_template: str) -> DatasetStats:
    query = f"""
SELECT
    min(created_at) start_range,
    max(created_at) end_range
FROM '{url_template.format(year='*')}'
""".strip()
    result = fetch_one(query)
    return DatasetStats(query, *result)


def year_stats(year: int, url_template: str) -> StatsForYear:
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


def most_popular_editors(year: int, url_template: str) -> DataFrameResult:
    query = f"""
SELECT
    CASE
        WHEN created_by LIKE 'iD%' THEN 'iD'
        WHEN created_by LIKE 'JOSM%' THEN 'JOSM'
        WHEN created_by LIKE 'Level0%' THEN 'Level0'
        WHEN created_by LIKE 'StreetComplete%' THEN 'StreetComplete'
        WHEN created_by LIKE 'RapiD%' THEN 'RapiD'
        WHEN created_by LIKE 'Rapid%' THEN 'RapiD'
        WHEN created_by LIKE 'Potlach%' THEN 'Potlach'
        WHEN created_by LIKE 'Potlatch%' THEN 'Potlatch'
        WHEN created_by LIKE 'Go Map!!%' THEN 'Go Map!!'
        WHEN created_by LIKE 'Merkaartor%' THEN 'Merkaartor'
        WHEN created_by LIKE 'OsmAnd%' THEN 'OsmAnd'
        WHEN created_by LIKE 'MAPS.ME%' THEN 'MAPS.ME'
        WHEN created_by LIKE 'Vespucci%' THEN 'Vespucci'
        WHEN created_by LIKE 'Organic Maps%' THEN 'Organic Maps'
        WHEN created_by LIKE 'ArcGIS Editor%' THEN 'ArcGIS Editor'
        WHEN created_by LIKE 'bulk_upload.py%' THEN 'bulk_upload.py'
        WHEN created_by LIKE 'reverter%' THEN 'reverter'
        WHEN created_by LIKE 'osm-revert%' THEN 'osm-revert'
        WHEN created_by LIKE 'Every_Door%' THEN 'EveryDoor'
        WHEN created_by LIKE 'osmtools%' THEN 'osmtools'
        WHEN created_by LIKE 'osmapi%' THEN 'osmapi'
        WHEN created_by LIKE 'rosemary%' THEN 'rosemary'
        WHEN created_by LIKE 'Globe%' THEN 'Globe'
        WHEN created_by LIKE 'PythonOsmApi%' THEN 'PythonOsmApi'
        WHEN created_by LIKE 'bot-source-cadastre.py%' THEN 'bot-source-cadastre.py'
        WHEN created_by LIKE 'upload.py%' THEN 'upload.py'
        WHEN created_by LIKE 'osm-budynki-orto-import%' THEN 'osm-budynki-orto-import'
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


def get_sample_data(year: int, url_template: str) -> DataFrameResult:
    query = f"""
SELECT *
FROM '{url_template.format(year=year)}'
LIMIT 10
""".strip()
    return fetch_df(query)


def most_reported_locale(year: int, url_template: str) -> DataFrameResult:
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


def new_users(year: int, url_template: str) -> DataFrameResult:
    paths_for_previous_years = paths_for_years(2005, year - 1, url_template)
    sep = ",\n" + " " * 8
    query = f"""
WITH
users_who_previously_edited as (
    SELECT DISTINCT uid
    FROM parquet_scan([
        {sep.join(paths_for_previous_years)}
    ])
),
users_who_edited_current_year as (
    SELECT DISTINCT uid
    FROM parquet_scan('{url_template.format(year=year)}')
)
SELECT
    CASE
        WHEN pe.uid IS NULL THEN true
        ELSE false
    END users_who_did_not_edit_before,
    count(*) number_of_users
FROM users_who_edited_current_year cu
LEFT JOIN users_who_previously_edited pe USING(uid)
GROUP BY 1
""".strip()
    return fetch_df(query)


def stats_over_years(url_template: str, max_year: int) -> DataFrameResult:
    sep = ",\n" + " " * 4
    query = f"""
SELECT
    regexp_extract(filename, '([0-9]{{4}})', 1) as "year",
    sum(num_changes)::bigint as number_of_changes,
    count(*) as number_of_changesets,
    count(distinct uid) as number_of_unique_users
FROM parquet_scan([
    {sep.join(paths_for_years(2005, max_year, url_template))}
], FILENAME = 1)
GROUP BY 1
ORDER BY 1
""".strip()
    return fetch_df(query)


# ------------------------------------------------------------------------------------------------------
# init
# ------------------------------------------------------------------------------------------------------
# create in-memory duckdb database 🦆
db = duckdb.connect(database=":memory:")
db.execute("""
    install 'httpfs';
    load 'httpfs';
    set s3_region='eu-central-1';
    set enable_http_metadata_cache=true;
    set enable_object_cache=true;
    set preserve_insertion_order=false;
""")

data_url_template = os.environ.get("url_template", "s3://tt-osm-changesets/full_by_year/{year}.zstd.parquet")

# ------------------------------------------------------------------------------------------------------
# beginning of the app
# ------------------------------------------------------------------------------------------------------
st.markdown(
    """
# OpenStreetMap changesets

This Streamlit app queries remote Parquet files with info from
[OpenStreetMap changeset](https://wiki.openstreetmap.org/wiki/Changeset)
dump downloaded from [planet.osm.org](https://planet.osm.org).
I described conversion process here: https://ttomasz.github.io/2023-01-30/spark-read-xml

Thanks to DuckDB we can query files hosted on S3 storage without having to download everything (~13,4 GB).
Although for running larger analyses this is a better course of action since it's order of magnitude faster.
With local files DuckDB can query all the data in seconds.

GitHub repo: https://github.com/ttomasz/tt-osm-changeset-analyzer

**Since changesets can be opened in one year and closed in another everywhere we assume that `year`
is the year the changeset was opened in.**

Each part shows corresponding SQL query that is executed by DuckDB.

First let's see range of timestamps in our files. Give it some time to load the data.
""")
dataset_stats = min_max_timestamps(data_url_template)
with st.expander("SQL query", expanded=False):
    st.code(dataset_stats.query, language="sql")
st.metric("Minimum changeset opening datetime", dataset_stats.min_opened_date.isoformat())
st.metric("Maximum changeset opening datetime", dataset_stats.max_opened_date.isoformat())

st.write("Let's see some charts")
max_year = dataset_stats.max_opened_date.year
stats = stats_over_years(url_template=data_url_template, max_year=max_year)
stats.df.columns = stats.df.columns.map(lambda x: str(x).replace("_", " ").title())
with st.expander("SQL query", expanded=False):
    st.code(stats.query, language="sql")
st.line_chart(data=stats.df, x="year".title(), y="number_of_changes".replace("_", " ").title())
st.line_chart(data=stats.df, x="year".title(), y="number_of_changesets".replace("_", " ").title())
st.line_chart(data=stats.df, x="year".title(), y="number_of_unique_users".replace("_", " ").title())

# year = st.slider("Select year for analysis:", min_value=2005, max_value=2023, value=2023, step=1)
year_options = tuple(range(2005, max_year + 1))
selected_year = st.selectbox("Select year for analysis:", options=year_options, index=len(year_options) - 1)
previous_year = selected_year - 1 if selected_year > 2005 else None

st.write("A sample of data from Parquet files:")
sample_data = get_sample_data(selected_year, data_url_template)
with st.expander("SQL query", expanded=False):
    st.code(sample_data.query, language="sql")
st.dataframe(data=sample_data.df, use_container_width=True)

st.markdown(f"## In {selected_year} there were:")

stats_for_year = year_stats(selected_year, data_url_template)
if previous_year is not None:
    stats_for_previous_year = year_stats(previous_year, data_url_template)
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
editor_result = most_popular_editors(selected_year, data_url_template)
editor_result.df.columns = editor_result.df.columns.map(lambda x: str(x).replace("_", " ").title())
with st.expander("SQL query", expanded=False):
    st.code(editor_result.query, language="sql")
st.dataframe(data=editor_result.df, use_container_width=True)

st.markdown("### Most reported locale")
locale_result = most_reported_locale(selected_year, data_url_template)
locale_result.df.columns = locale_result.df.columns.map(lambda x: str(x).replace("_", " ").title())
with st.expander("SQL query", expanded=False):
    st.code(locale_result.query, language="sql")
st.dataframe(data=locale_result.df, use_container_width=True)

if selected_year > 2005:
    st.markdown("### New vs old users")
    new_users_result = new_users(selected_year, data_url_template)
    new_users_result.df.columns = new_users_result.df.columns.map(lambda x: str(x).replace("_", " ").title())
    with st.expander("SQL query", expanded=False):
        st.code(new_users_result.query, language="sql")
    st.dataframe(data=new_users_result.df, use_container_width=True)
else:
    st.empty()
