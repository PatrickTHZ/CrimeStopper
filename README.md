# CrimeStoppers Crimes Analysus Dashboard

Dark Streamlit dashboard for analysing NSW BOCSAR crime data with suburb
trends, LGA rankings, advanced charts, and hotspot maps.

## Run the Dashboard

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
.venv/bin/python scripts/build_dashboard_data.py
.venv/bin/streamlit run dashboard/app.py
```

The raw BOCSAR downloads live in `data/raw/bocsar`. Processed dashboard tables
are generated into `data/processed/bocsar`.

On Streamlit Community Cloud, the app can bootstrap the required BOCSAR source
files and processed tables on first launch if they are not already present.

## Streamlit Community Cloud

Use this entrypoint when deploying:

```text
dashboard/app.py
```

The first deployment can take a little longer while the app downloads BOCSAR
data and builds its Parquet/GeoJSON cache.

## Refresh BOCSAR Data

```bash
bash scripts/download_bocsar_data.sh
.venv/bin/python scripts/build_dashboard_data.py --download-missing
```

The download script writes `data/raw/bocsar/download_manifest.tsv` with source
URLs, file sizes, SHA-256 checksums, and download timestamps.
