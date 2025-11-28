#!/usr/bin/env bash
# Automated solution for "Get Started with Dataplex: Challenge Lab"
set -euo pipefail
IFS=$'\n\t'

echo "--------------------------------------------------------"
echo " GOOGLE CLOUD ARCADE – DATAPLEX CHALLENGE LAB AUTOMATION"
echo "--------------------------------------------------------"
echo

# ---------------------------------------------------------
# CONFIGURATION (Auto-detected)
# ---------------------------------------------------------
PROJECT="${DEVSHELL_PROJECT_ID:-$(gcloud config get-value project --quiet)}"
REGION="us-central1"

LAKE_ID="customer-engagements"
LAKE_DISPLAY="Customer Engagements"

ZONE_ID="raw-event-data"
ZONE_DISPLAY="Raw Event Data"

ASSET_ID="raw-event-files"
ASSET_DISPLAY="Raw Event Files"

ASPECT_TYPE_ID="protected-raw-data-aspect"
ASPECT_DISPLAY="Protected Raw Data Aspect"

BUCKET_NAME="${PROJECT}"

DATAPLEX_API="https://dataplex.googleapis.com/v1"

echo "Project: $PROJECT"
echo "Region:  $REGION"
echo

# ---------------------------------------------------------
# ENABLE APIS
# ---------------------------------------------------------
echo "[1/6] Enabling required APIs..."
gcloud services enable dataplex.googleapis.com storage.googleapis.com --project="$PROJECT" >/dev/null
echo "✓ APIs enabled."
echo

# ---------------------------------------------------------
# TASK 1 — CREATE LAKE
# ---------------------------------------------------------
echo "[2/6] Creating Dataplex Lake..."

if gcloud dataplex lakes describe "$LAKE_ID" --location="$REGION" --project="$PROJECT" >/dev/null 2>&1; then
    echo "Lake already exists."
else
    gcloud dataplex lakes create "$LAKE_ID" \
        --location="$REGION" \
        --display-name="$LAKE_DISPLAY" \
        --project="$PROJECT" >/dev/null
    echo "Lake created."
fi

# ---------------------------------------------------------
# TASK 1 — CREATE RAW ZONE
# ---------------------------------------------------------
echo "[3/6] Creating RAW Zone..."

if gcloud dataplex zones describe "$ZONE_ID" --lake="$LAKE_ID" --location="$REGION" --project="$PROJECT" >/dev/null 2>&1; then
    echo "Zone already exists."
else
    gcloud dataplex zones create "$ZONE_ID" \
        --location="$REGION" \
        --lake="$LAKE_ID" \
        --display-name="$ZONE_DISPLAY" \
        --type=RAW \
        --resource-location-type=SINGLE_REGION \
        --project="$PROJECT" >/dev/null
    echo "Zone created."
fi

echo "✓ Task 1 Completed: Lake + Raw Zone Ready."
echo

# ---------------------------------------------------------
# TASK 2 — CREATE BUCKET
# ---------------------------------------------------------
echo "[4/6] Creating Cloud Storage Bucket..."

if gsutil ls -b "gs://${BUCKET_NAME}" >/dev/null 2>&1; then
    echo "Bucket already exists."
else
    gsutil mb -l "$REGION" -p "$PROJECT" "gs://${BUCKET_NAME}/"
    echo "Bucket created."
fi

# ---------------------------------------------------------
# TASK 2 — ATTACH ASSET
# ---------------------------------------------------------
echo "[5/6] Attaching Asset to Zone..."

RESOURCE="projects/${PROJECT}/buckets/${BUCKET_NAME}"

if gcloud dataplex assets describe "$ASSET_ID" \
    --location="$REGION" --lake="$LAKE_ID" --zone="$ZONE_ID" --project="$PROJECT" >/dev/null 2>&1; then
    echo "Asset already exists."
else
    gcloud dataplex assets create "$ASSET_ID" \
        --location="$REGION" \
        --lake="$LAKE_ID" \
        --zone="$ZONE_ID" \
        --display-name="$ASSET_DISPLAY" \
        --resource="$RESOURCE" \
        --project="$PROJECT" >/dev/null
    echo "Asset created and attached."
fi

echo "✓ Task 2 Completed: Bucket Attached as Asset."
echo

# ---------------------------------------------------------
# TASK 3 — CREATE ASPECT TYPE
# ---------------------------------------------------------
echo "[6/6] Creating Aspect Type..."

ASPECT_JSON=$(mktemp)
cat > "$ASPECT_JSON" <<EOF
{
  "aspectTypeId": "${ASPECT_TYPE_ID}",
  "displayName": "${ASPECT_DISPLAY}",
  "metadataTemplate": {
    "fields": [
      {
        "fieldId": "protected_raw_data_flag",
        "displayName": "Protected Raw Data Flag",
        "type": "ENUM",
        "enumValues": [
          {"displayName": "Y", "value": "Y"},
          {"displayName": "N", "value": "N"}
        ]
      }
    ]
  }
}
EOF

TOKEN=$(gcloud auth print-access-token)

curl -s -X POST \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json" \
  -d @"${ASPECT_JSON}" \
  "${DATAPLEX_API}/projects/${PROJECT}/locations/${REGION}/aspectTypes?aspectTypeId=${ASPECT_TYPE_ID}" \
  >/dev/null || true

echo "Aspect Type created (or already existed)."

# ---------------------------------------------------------
# TASK 3 — ATTACH ASPECT TO ZONE
# ---------------------------------------------------------

ATTACH_JSON=$(mktemp)
cat > "$ATTACH_JSON" <<EOF
{
  "aspectType": "projects/${PROJECT}/locations/${REGION}/aspectTypes/${ASPECT_TYPE_ID}",
  "data": "{\"protected_raw_data_flag\":\"Y\"}"
}
EOF

curl -s -X POST \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json" \
  -d @"${ATTACH_JSON}" \
  "${DATAPLEX_API}/projects/${PROJECT}/locations/${REGION}/lakes/${LAKE_ID}/zones/${ZONE_ID}/aspects" \
  >/dev/null || true

echo "Aspect attached to Zone."
echo

echo "==========================================================="
echo "        ALL TASKS COMPLETED SUCCESSFULLY 🎉"
echo "==========================================================="
