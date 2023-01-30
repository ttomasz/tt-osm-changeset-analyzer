This is a simple [Streamlit App](https://streamlit.io) showing some simple queries run by [DuckDB](https://duckdb.org) over Parquet files that I prepared by converting [OpenStreetMap](https://osm.org) changeset dump from XML to Parquet and uploaded to AWS S3.

I described data preparation process here: https://ttomasz.github.io/2023-01-30/spark-read-xml

Running locally:
```bash
# close repository then run
python -m venv venv
# for linux
source ./venv/bin/activate
# for windows
cmd ./venv/bin/activate.bat

pip install -r requirements.txt
streamlit run main.py
```
If you downloaded the files from S3 you can override URL template by setting environment variable `url_template`.
