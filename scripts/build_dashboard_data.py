#!/usr/bin/env python3
"""Build compact dashboard-ready BOCSAR datasets from raw downloads."""

from __future__ import annotations

import argparse
from io import BytesIO
import json
import re
import urllib.request
import zipfile
from pathlib import Path

import pandas as pd
import shapefile


ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw" / "bocsar"
OUT = ROOT / "data" / "processed" / "bocsar"
HOTSPOTS_OUT = OUT / "hotspots"


MONTH_RE = re.compile(r"^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) \d{4}$")

REQUIRED_RAW_DOWNLOADS = {
    RAW / "core" / "Incident_by_NSW.xlsx": "https://bocsar.nsw.gov.au/content/dam/dcj/bocsar/documents/open-datasets/Incident_by_NSW.xlsx",
    RAW / "core" / "RCI_offencebymonth.xlsm": "https://bocsarblob.blob.core.windows.net/bocsar-open-data/RCI_offencebymonth.xlsm",
    RAW / "core" / "SuburbData.zip": "https://bocsarblob.blob.core.windows.net/bocsar-open-data/SuburbData.zip",
    RAW / "core" / "LGA_trends.xlsx": "https://bocsar.nsw.gov.au/content/dam/dcj/bocsar/documents/open-datasets/LGA_trends.xlsx",
    RAW / "spatial" / "CrimeToolHotspots.zip": "https://bocsarblob.blob.core.windows.net/bocsar-open-data/CrimeToolHotspots.zip",
}


def normalise_month_columns(columns: list[object]) -> tuple[list[str], dict[object, str]]:
    mapping: dict[object, str] = {}
    month_cols: list[str] = []
    for col in columns:
        parsed = pd.to_datetime(col, errors="coerce")
        if pd.notna(parsed):
            name = parsed.strftime("%Y-%m")
            mapping[col] = name
            month_cols.append(name)
        elif isinstance(col, str) and MONTH_RE.match(col):
            parsed = pd.to_datetime(col, format="%b %Y")
            name = parsed.strftime("%Y-%m")
            mapping[col] = name
            month_cols.append(name)
    return month_cols, mapping


def fiscal_year_columns(month_cols: list[str]) -> dict[int, list[str]]:
    years = sorted({int(col[:4]) for col in month_cols})
    return {year: [col for col in month_cols if col.startswith(f"{year}-")] for year in years}


def write_parquet(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    print(f"Wrote {path.relative_to(ROOT)} ({len(df):,} rows)")


def ensure_required_raw_data() -> None:
    manifest_rows = []
    for path, url in REQUIRED_RAW_DOWNLOADS.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            print(f"Downloading {path.relative_to(ROOT)}")
            urllib.request.urlretrieve(url, path)
        manifest_rows.append(
            {
                "category": path.parent.name,
                "file": path.name,
                "url": url,
                "bytes": path.stat().st_size,
            }
        )

    manifest_path = RAW / "download_manifest.tsv"
    if not manifest_path.exists():
        pd.DataFrame(manifest_rows).to_csv(manifest_path, sep="\t", index=False)


def read_zipped_csv(path: Path) -> pd.DataFrame:
    with zipfile.ZipFile(path) as archive:
        names = [name for name in archive.namelist() if name.lower().endswith(".csv")]
        if not names:
            raise ValueError(f"No CSV found in {path}")
        with archive.open(names[0]) as handle:
            return pd.read_csv(handle)


def build_area_wide(
    df: pd.DataFrame,
    area_col: str,
    out_prefix: str,
) -> dict[str, object]:
    month_cols, mapping = normalise_month_columns(df.columns.tolist())
    df = df.rename(columns=mapping)
    id_cols = [area_col, "Offence category"]
    grouped = (
        df[id_cols + month_cols]
        .groupby(id_cols, as_index=False, observed=True)
        .sum(numeric_only=True)
        .sort_values(id_cols)
    )

    write_parquet(grouped, OUT / f"{out_prefix}_category_wide.parquet")

    long_category = (
        grouped.groupby("Offence category", observed=True)[month_cols]
        .sum()
        .reset_index()
        .melt(id_vars="Offence category", var_name="month", value_name="incidents")
    )
    long_category["month"] = pd.to_datetime(long_category["month"])
    long_category["incidents"] = long_category["incidents"].astype("int64")
    write_parquet(long_category, OUT / f"{out_prefix}_monthly_by_category.parquet")

    year_map = fiscal_year_columns(month_cols)
    yearly_frames = []
    for year, cols in year_map.items():
        frame = grouped[id_cols].copy()
        frame["year"] = year
        frame["incidents"] = grouped[cols].sum(axis=1).astype("int64")
        yearly_frames.append(frame)
    yearly = pd.concat(yearly_frames, ignore_index=True)
    write_parquet(yearly, OUT / f"{out_prefix}_yearly_by_category.parquet")

    latest_year = max(year_map)
    prior_year = latest_year - 1
    latest = yearly[yearly["year"] == latest_year].copy()
    latest_area = (
        latest.groupby(area_col, observed=True)["incidents"]
        .sum()
        .reset_index(name=f"incidents_{latest_year}")
    )
    prior_area = (
        yearly[yearly["year"] == prior_year]
        .groupby(area_col, observed=True)["incidents"]
        .sum()
        .reset_index(name=f"incidents_{prior_year}")
    )
    top_cat = (
        latest.sort_values([area_col, "incidents"], ascending=[True, False])
        .drop_duplicates(area_col)[[area_col, "Offence category", "incidents"]]
        .rename(
            columns={
                "Offence category": f"top_category_{latest_year}",
                "incidents": f"top_category_incidents_{latest_year}",
            }
        )
    )
    index = latest_area.merge(prior_area, on=area_col, how="left").merge(
        top_cat, on=area_col, how="left"
    )
    index["change_vs_prior"] = (
        index[f"incidents_{latest_year}"] - index[f"incidents_{prior_year}"].fillna(0)
    ).astype("int64")
    index["change_pct_vs_prior"] = (
        index["change_vs_prior"] / index[f"incidents_{prior_year}"].replace({0: pd.NA})
    ) * 100
    index = index.sort_values(f"incidents_{latest_year}", ascending=False)
    write_parquet(index, OUT / f"{out_prefix}_index.parquet")

    return {
        "latest_year": latest_year,
        "month_min": min(month_cols),
        "month_max": max(month_cols),
        "area_count": grouped[area_col].nunique(),
        "category_count": grouped["Offence category"].nunique(),
    }


def build_lga_trends() -> dict[str, object]:
    path = RAW / "core" / "LGA_trends.xlsx"
    trends = pd.read_excel(path, sheet_name="Local Government Area", header=3)
    trends = trends[trends["Local Government Area"].notna()].copy()
    trends = trends[trends["Local Government Area"] != "Local Government Area"]

    rename = {
        "Local Government Area": "lga",
        "Offence type": "offence_type",
        "Rate per 100,000 population Jan - Dec 2025": "rate_2025",
        "LGA Rank \n Jan - Dec 2025": "rank_2025",
        "2 year trend and annual percent change (Jan 2024-Dec 2025)": "trend_2y",
    }
    for col in trends.columns:
        if str(col).startswith("10 year trend"):
            rename[col] = "trend_10y"
    trends = trends.rename(columns=rename)

    annual_cols = [col for col in trends.columns if str(col).startswith("Jan - Dec ")]
    for col in annual_cols:
        year = int(str(col).split()[-1])
        trends[f"incidents_{year}"] = pd.to_numeric(trends[col], errors="coerce")
    trends["rate_2025"] = pd.to_numeric(trends["rate_2025"], errors="coerce")
    trends["rank_2025"] = pd.to_numeric(trends["rank_2025"], errors="coerce")

    keep_cols = [
        "lga",
        "offence_type",
        *[f"incidents_{year}" for year in range(2016, 2026)],
        "rate_2025",
        "rank_2025",
        "trend_2y",
        "trend_10y",
    ]
    trends = trends[[col for col in keep_cols if col in trends.columns]]
    write_parquet(trends, OUT / "lga_trends.parquet")

    return {
        "lga_count": trends["lga"].nunique(),
        "offence_type_count": trends["offence_type"].nunique(),
    }


def build_nsw_monthly() -> dict[str, object]:
    path = RAW / "core" / "Incident_by_NSW.xlsx"
    df = pd.read_excel(path, sheet_name="Data")
    month_cols, mapping = normalise_month_columns(df.columns.tolist())
    df = df.rename(columns=mapping)

    grouped = (
        df[["Offence category"] + month_cols]
        .groupby("Offence category", as_index=False, observed=True)
        .sum(numeric_only=True)
    )
    monthly = grouped.melt(
        id_vars="Offence category", var_name="month", value_name="incidents"
    )
    monthly["month"] = pd.to_datetime(monthly["month"])
    monthly["incidents"] = monthly["incidents"].astype("int64")
    write_parquet(monthly, OUT / "nsw_monthly_by_category.parquet")

    return {"category_count": grouped["Offence category"].nunique()}


def split_shapefile_parts(shape: shapefile.Shape) -> list[list[list[float]]]:
    parts = list(shape.parts) + [len(shape.points)]
    polygons: list[list[list[float]]] = []
    for start, end in zip(parts, parts[1:]):
        ring = [[float(x), float(y)] for x, y in shape.points[start:end]]
        if len(ring) >= 4:
            polygons.append(ring)
    return polygons


def build_hotspot_geojson() -> dict[str, object]:
    zip_path = RAW / "spatial" / "CrimeToolHotspots.zip"
    HOTSPOTS_OUT.mkdir(parents=True, exist_ok=True)
    summaries = []

    with zipfile.ZipFile(zip_path) as archive:
        shp_names = sorted(name for name in archive.namelist() if name.lower().endswith(".shp"))
        for shp_name in shp_names:
            stem = Path(shp_name).stem
            parts = stem.split("_")
            crime_type = "_".join(parts[:-2])
            period = "_".join(parts[-2:])

            reader = shapefile.Reader(
                shp=BytesIO(archive.read(f"{stem}.shp")),
                shx=BytesIO(archive.read(f"{stem}.shx")),
                dbf=BytesIO(archive.read(f"{stem}.dbf")),
            )
            field_names = [field[0] for field in reader.fields[1:]]
            features = []
            density_counts: dict[str, int] = {}

            for idx, shape_record in enumerate(reader.iterShapeRecords()):
                props = dict(zip(field_names, shape_record.record))
                density = str(props.get("Density", "Hotspot")).strip()
                density_counts[density] = density_counts.get(density, 0) + 1
                polygons = split_shapefile_parts(shape_record.shape)
                if not polygons:
                    continue
                geometry = (
                    {"type": "Polygon", "coordinates": polygons}
                    if len(polygons) == 1
                    else {"type": "MultiPolygon", "coordinates": [[ring] for ring in polygons]}
                )
                features.append(
                    {
                        "type": "Feature",
                        "id": f"{stem}-{idx}",
                        "properties": {
                            "crime_type": crime_type,
                            "crime_label": re.sub(r"(?<!^)(?=[A-Z])", " ", crime_type),
                            "period": period,
                            "density": density,
                        },
                        "geometry": geometry,
                    }
                )

            geojson = {
                "type": "FeatureCollection",
                "name": stem,
                "features": features,
            }
            output = HOTSPOTS_OUT / f"{stem}.geojson"
            output.write_text(json.dumps(geojson), encoding="utf-8")
            summaries.append(
                {
                    "crime_type": crime_type,
                    "crime_label": re.sub(r"(?<!^)(?=[A-Z])", " ", crime_type),
                    "period": period,
                    "file": str(output.relative_to(ROOT)),
                    "features": len(features),
                    "density_counts": density_counts,
                }
            )
            print(f"Wrote {output.relative_to(ROOT)} ({len(features):,} features)")

    summary_df = pd.DataFrame(
        [
            {
                "crime_type": item["crime_type"],
                "crime_label": item["crime_label"],
                "period": item["period"],
                "file": item["file"],
                "features": item["features"],
            }
            for item in summaries
        ]
    )
    write_parquet(summary_df, OUT / "hotspot_layers.parquet")
    return {"layer_count": len(summaries), "feature_count": int(summary_df["features"].sum())}


def build_metadata(summary: dict[str, object]) -> None:
    manifest = RAW / "download_manifest.tsv"
    metadata = {
        "source": "NSW Bureau of Crime Statistics and Research open datasets",
        "raw_manifest": str(manifest.relative_to(ROOT)),
        "built_at": pd.Timestamp.utcnow().isoformat(),
        **summary,
    }
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "metadata.json").write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    print(f"Wrote {(OUT / 'metadata.json').relative_to(ROOT)}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--skip-lga-monthly", action="store_true")
    parser.add_argument("--download-missing", action="store_true")
    args = parser.parse_args()

    OUT.mkdir(parents=True, exist_ok=True)
    if args.download_missing:
        ensure_required_raw_data()

    summary: dict[str, object] = {}
    print("Processing suburb data")
    suburb = read_zipped_csv(RAW / "core" / "SuburbData.zip")
    summary["suburb"] = build_area_wide(suburb, "Suburb", "suburb")
    del suburb

    if not args.skip_lga_monthly:
        print("Processing LGA monthly data")
        lga = pd.read_excel(
            RAW / "core" / "RCI_offencebymonth.xlsm",
            sheet_name="Data",
            engine="openpyxl",
        )
        summary["lga_monthly"] = build_area_wide(lga, "LGA", "lga")
        del lga

    print("Processing LGA trends")
    summary["lga_trends"] = build_lga_trends()
    print("Processing NSW monthly data")
    summary["nsw"] = build_nsw_monthly()
    print("Processing hotspot map layers")
    summary["hotspots"] = build_hotspot_geojson()
    build_metadata(summary)


if __name__ == "__main__":
    main()
