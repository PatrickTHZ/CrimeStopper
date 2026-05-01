#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATA_DIR="$ROOT_DIR/data/raw/bocsar"
DOWNLOADED_AT="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
MANIFEST="$DATA_DIR/download_manifest.tsv"

mkdir -p \
  "$DATA_DIR/metadata" \
  "$DATA_DIR/core" \
  "$DATA_DIR/rankings" \
  "$DATA_DIR/spatial" \
  "$DATA_DIR/advanced" \
  "$DATA_DIR/local_area_tables"

printf "category\tfile\turl\tbytes\tsha256\tdownloaded_at\n" > "$MANIFEST"

download() {
  local category="$1"
  local url="$2"
  local output="$3"
  local path="$DATA_DIR/$category/$output"

  mkdir -p "$(dirname "$path")"
  echo "Downloading $category/$output"
  curl -fL --retry 3 --retry-delay 2 -o "$path" "$url"

  local bytes sha
  bytes="$(wc -c < "$path" | tr -d ' ')"
  sha="$(shasum -a 256 "$path" | awk '{print $1}')"
  printf "%s\t%s\t%s\t%s\t%s\t%s\n" "$category" "$output" "$url" "$bytes" "$sha" "$DOWNLOADED_AT" >> "$MANIFEST"
}

download metadata \
  "https://data.nsw.gov.au/data/api/3/action/package_show?id=nsw-crime-and-policing-statistics" \
  "nsw-crime-and-policing-statistics.package.json"

download core \
  "https://bocsar.nsw.gov.au/content/dam/dcj/bocsar/documents/open-datasets/Incident_by_NSW.xlsx" \
  "Incident_by_NSW.xlsx"
download core \
  "https://bocsarblob.blob.core.windows.net/bocsar-open-data/RCI_offencebymonth.xlsm" \
  "RCI_offencebymonth.xlsm"
download core \
  "https://bocsarblob.blob.core.windows.net/bocsar-open-data/SuburbData.zip" \
  "SuburbData.zip"
download core \
  "https://bocsarblob.blob.core.windows.net/bocsar-open-data/PostcodeData.zip" \
  "PostcodeData.zip"
download core \
  "https://bocsar.nsw.gov.au/content/dam/dcj/bocsar/documents/open-datasets/NSW_trends.xlsx" \
  "NSW_trends.xlsx"
download core \
  "https://bocsar.nsw.gov.au/content/dam/dcj/bocsar/documents/open-datasets/LGA_trends.xlsx" \
  "LGA_trends.xlsx"
download core \
  "https://bocsar.nsw.gov.au/content/dam/dcj/bocsar/documents/open-datasets/SA_trends.xlsx" \
  "SA_trends.xlsx"
download core \
  "https://bocsar.nsw.gov.au/content/dam/dcj/bocsar/documents/open-datasets/Long_term_trends.xlsx" \
  "Long_term_trends.xlsx"

download rankings \
  "https://bocsar.nsw.gov.au/content/dam/dcj/bocsar/documents/open-datasets/LGA_ranking_for_violent_and_property_offences.xlsx" \
  "LGA_ranking_for_violent_and_property_offences.xlsx"
download rankings \
  "https://bocsar.nsw.gov.au/content/dam/dcj/bocsar/documents/open-datasets/LgaRankings_27_Offences.xlsx" \
  "LgaRankings_27_Offences.xlsx"
download rankings \
  "https://bocsar.nsw.gov.au/content/dam/dcj/bocsar/documents/open-datasets/mediansentenceranking.xlsx" \
  "mediansentenceranking.xlsx"

download spatial \
  "https://bocsarblob.blob.core.windows.net/bocsar-open-data/CrimeToolHotspots.zip" \
  "CrimeToolHotspots.zip"

download advanced \
  "https://bocsar.nsw.gov.au/content/dam/dcj/bocsar/documents/open-datasets/NSW_criminal_incidents_daily.xlsx" \
  "NSW_criminal_incidents_daily.xlsx"
download advanced \
  "https://bocsar.nsw.gov.au/content/dam/dcj/bocsar/documents/open-datasets/NSW_Alleged_offender_data.xlsx" \
  "NSW_Alleged_offender_data.xlsx"
download advanced \
  "https://bocsar.nsw.gov.au/content/dam/dcj/bocsar/documents/open-datasets/Drink_food_spiking_incidents_in_NSW.xlsx" \
  "Drink_food_spiking_incidents_in_NSW.xlsx"
download advanced \
  "https://bocsar.nsw.gov.au/content/dam/dcj/bocsar/documents/topic-areas/weapons/Non_Fatal_Shootings.xlsx" \
  "Non_Fatal_Shootings.xlsx"
download advanced \
  "https://bocsar.nsw.gov.au/content/dam/dcj/bocsar/documents/open-datasets/Offences_On_Public_Transport.xlsx" \
  "Offences_On_Public_Transport.xlsx"
download advanced \
  "https://bocsar.nsw.gov.au/content/dam/dcj/bocsar/documents/topic-areas/weapons/Weapons_Statistics.xlsx" \
  "Weapons_Statistics.xlsx"
download advanced \
  "https://bocsar.nsw.gov.au/content/dam/dcj/bocsar/documents/open-datasets/Graffiti_incidents_by_LGA.xlsx" \
  "Graffiti_incidents_by_LGA.xlsx"

echo "Discovering local area Excel tables"
curl -fsL "https://bocsar.nsw.gov.au/statistics-dashboards/crime-and-policing/lga-excel-crime-tables.html" \
  | rg -o '/content/dam/[^" ]+\.xlsx' \
  | sed 's/&amp;/\&/g' \
  | sort -u \
  | while IFS= read -r path; do
      url="https://bocsar.nsw.gov.au$path"
      file="$(basename "$path")"
      download local_area_tables "$url" "$file"
    done

echo "Done. Manifest written to $MANIFEST"
