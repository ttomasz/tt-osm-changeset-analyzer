from datetime import datetime
from typing import NamedTuple

import pandas as pd


class DatasetStats(NamedTuple):
    query: str
    min_opened_date: datetime
    max_opened_date: datetime


class StatsForYear(NamedTuple):
    query: str | None
    number_of_changesets: int | None
    number_of_unique_users: int | None
    number_of_object_changes: int | None
    number_of_comments: int | None


class DataFrameResult(NamedTuple):
    query: str
    df: pd.DataFrame
