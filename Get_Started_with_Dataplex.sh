#!/usr/bin/env bash
# datapl ex-lab.sh — Automate "Get Started with Dataplex: Challenge Lab"
# Best-effort, idempotent, non-interactive. Prints a message after each task.
set -euo pipefail
IFS=$'\n\t'

# ----------------- Configuration -----------------
PROJECT="${DEVSHELL_PROJECT_ID:-$(gcloud config get-value project --quiet || true)}"
REGION="${REGION:-$(gcloud config get-value compute/region --quiet || true)}"
REGION="${REGION:-us-central1}"   # default if none found
LAKE_ID="customer-engagements"
LAKE_DISPLAY="Customer Engagements"
ZONE_ID="raw-event-data"
ZONE_DISPLAY="Raw Event Data"
ZONE_TYPE="RAW"                    # RAW zone as per task
ASSET_ID="raw-event-files"
ASSET_DISPLAY="Raw Event Files"
ASPECT_TYPE_ID="protected-raw-data-aspect"
ASPECT_DISPLAY="Protected Raw Data Aspect"
# Bucket name per lab statement: "Create a Cloud Storage bucket named Project ID"
# That means the bucket ID should be the GCP project id (project IDs are globally unique)
BUCKET_NAME="${PROJECT}"
# Service / API endpoints
DATAPLEX_BASE="https://dataplex.googleapis.com/v1"

# ----------------- Helpers -----------------
_err() { echo >&2 "ERROR: $*"; }
_info() { echo "INFO: $*"; }
_try_cmd() { echo "+ $*"; "$@"; }

# ----------------- Prechecks -----------------
if [[ -z "$PROJECT" ]]; then
  _err "Project ID not detected. Set DEVSHELL_PROJECT_ID or run 'gcloud config set project PROJECT_ID'."
  exit 1
fi

if ! command -v gcloud >/dev/null 2>&1; then
  _err "gcloud CLI not installed or not in PATH."
  exit 1
fi

if ! command -v gsutil >/dev/null 2>&1; then
  _err "gsutil not installed or not in PATH."
  exit 1
fi

_info "Project: $PROJECT"
_info "Region: $REGION"
_info "Lake ID: $LAKE_ID (display: $LAKE_DISPLAY)"
_info "Zone ID: $ZONE_ID (display: $ZONE_DISPLAY)"
_info "Bucket: gs://$BUCKET_NAME"
echo

# ----------------- Enable required APIs -----------------
_info "Enabling required APIs (Dataplex, Storage) ..."
gcloud services enable dataplex.googleapis.com storage.googleapis.com --project="$PROJECT" >/dev/null
_info "APIs enabled (or already enabled)."

# ----------------- Task 1: Create Lake with Raw Zone -----------------
_info "Task 1 — Create lake '$LAKE_DISPLAY' with zone '$ZONE_DISPLAY' in region '$REGION' ..."

# Create lake (try stable command, fall back to alpha/beta if needed)
create_lake() {
  if gcloud dataplex lakes describe "$LAKE_ID" --location="$REGION" --project="$PROJECT" >/dev/null 2>&1; then
    _info "Lake '$LAKE_ID' already exists in $REGION."
    return 0
  fi

  # Try main command first
  if gcloud dataplex lakes create "$LAKE_ID" \
       --location="$REGION" \
       --display-name="$LAKE_DISPLAY" \
       --project="$PROJECT" \
       >/dev/null 2>&1; then
    _info "Lake created using 'gcloud dataplex lakes create'."
    return 0
  fi

  # Try alpha variant
  if gcloud alpha dataplex lakes create "$LAKE_ID" \
       --location="$REGION" \
       --display-name="$LAKE_DISPLAY" \
       --project="$PROJECT" \
       >/dev/null 2>&1; then
    _info "Lake created using 'gcloud alpha dataplex lakes create'."
    return 0
  fi

  return 1
}

if create_lake; then
  _info "Task 1.1: Lake '$LAKE_DISPLAY' ($LAKE_ID) exists or was created successfully."
else
  _err "Failed to create lake '$LAKE_ID'. See gcloud output for details."
  exit 1
fi

# Create zone (zones are regional under lake)
create_zone() {
  if gcloud dataplex zones describe "$ZONE_ID" --lake="$LAKE_ID" --location="$REGION" --project="$PROJECT" >/dev/null 2>&1; then
    _info "Zone '$ZONE_ID' already exists in lake '$LAKE_ID'."
    return 0
  fi

  # Recommended flags for a single-region RAW zone
  if gcloud dataplex zones create "$ZONE_ID" \
       --location="$REGION" \
       --lake="$LAKE_ID" \
       --display-name="$ZONE_DISPLAY" \
       --resource-location-type=SINGLE_REGION \
       --type="$ZONE_TYPE" \
       --project="$PROJECT" \
       >/dev/null 2>&1; then
    _info "Zone created using 'gcloud dataplex zones create'."
    return 0
  fi

  # fallback to alpha
  if gcloud alpha dataplex zones create "$ZONE_ID" \
       --location="$REGION" \
       --lake="$LAKE_ID" \
       --display-name="$ZONE_DISPLAY" \
       --resource-location-type=SINGLE_REGION \
       --type="$ZONE_TYPE" \
       --project="$PROJECT" \
       >/dev/null 2>&1; then
    _info "Zone created using 'gcloud alpha dataplex zones create'."
    return 0
  fi

  return 1
}

if create_zone; then
  echo "✅ Task 1 complete: Lake '$LAKE_DISPLAY' and zone '$ZONE_DISPLAY' are ready."
else
  _err "Task 1 failed: could not create zone '$ZONE_ID'."
  exit 1
fi

# ----------------- Task 2: Create GS bucket & attach as asset -----------------
_info "Task 2 — Create Cloud Storage bucket and attach as Dataplex asset ..."

# Create bucket if not exists
if gsutil ls -b "gs://$BUCKET_NAME" >/dev/null 2>&1; then
  _info "Bucket gs://$BUCKET_NAME already exists."
else
  _info "Creating bucket gs://$BUCKET_NAME in region $REGION ..."
  # Use regional location flag; many projects use multi-region, but lab requests region
  # gsutil mb -l REGION accepts region or multi-region; for regional use: -l REGION
  _try_cmd gsutil mb -l "$REGION" -p "$PROJECT" "gs://$BUCKET_NAME/"
  _info "Bucket created."
fi

# Create Dataplex asset for the bucket (attach to zone)
create_asset() {
  # check if asset exists
  if gcloud dataplex assets describe "$ASSET_ID" --location="$REGION" --lake="$LAKE_ID" --zone="$ZONE_ID" --project="$PROJECT" >/dev/null 2>&1; then
    _info "Asset '$ASSET_ID' already exists in zone '$ZONE_ID'."
    return 0
  fi

  # Resource name for bucket: projects/{project}/buckets/{bucket}
  RESOURCE="projects/${PROJECT}/buckets/${BUCKET_NAME}"

  # Try main gcloud command
  if gcloud dataplex assets create "$ASSET_ID" \
       --location="$REGION" \
       --lake="$LAKE_ID" \
       --zone="$ZONE_ID" \
       --display-name="$ASSET_DISPLAY" \
       --resource="$RESOURCE" \
       --project="$PROJECT" \
       >/dev/null 2>&1; then
    _info "Asset created using 'gcloud dataplex assets create'."
    return 0
  fi

  # fallback to alpha
  if gcloud alpha dataplex assets create "$ASSET_ID" \
       --location="$REGION" \
       --lake="$LAKE_ID" \
       --zone="$ZONE_ID" \
       --display-name="$ASSET_DISPLAY" \
       --resource="$RESOURCE" \
       --project="$PROJECT" \
       >/dev/null 2>&1; then
    _info "Asset created using 'gcloud alpha dataplex assets create'."
    return 0
  fi

  return 1
}

if create_asset; then
  echo "✅ Task 2 complete: Bucket gs://$BUCKET_NAME attached as asset '$ASSET_DISPLAY'."
else
  _err "Task 2 failed: could not create asset for gs://$BUCKET_NAME."
  exit 1
fi

# ----------------- Task 3: Create an Aspect Type and add the Aspect to the zone -----------------
_info "Task 3 — Create aspect type '$ASPECT_DISPLAY' and attach aspect to the zone ..."

# Prepare metadataTemplate JSON for aspect type (enum field)
tmp_aspect_json="$(mktemp --suffix=.json)"
cat > "$tmp_aspect_json" <<EOF
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

_info "Prepared aspect type JSON at $tmp_aspect_json"

# Try creating aspect type via gcloud if supported
create_aspect_type() {
  # Check if aspect type exists (list & grep)
  if gcloud dataplex aspect-types list --location="$REGION" --project="$PROJECT" --format="value(name)" 2>/dev/null | grep -q "$ASPECT_TYPE_ID"; then
    _info "Aspect type '$ASPECT_TYPE_ID' already exists."
    return 0
  fi

  # First attempt: gcloud dataplex aspect-types create --metadata-template-file (if supported)
  if gcloud dataplex aspect-types create "$ASPECT_TYPE_ID" \
       --location="$REGION" \
       --display-name="$ASPECT_DISPLAY" \
       --project="$PROJECT" \
       --metadata-template-from-file="$tmp_aspect_json" \
       >/dev/null 2>&1; then
    _info "Aspect type created using 'gcloud dataplex aspect-types create' (metadata-template-from-file)."
    return 0
  fi

  # Fallback: call Dataplex REST API directly using gcloud access token
  TOKEN="$(gcloud auth print-access-token --project="$PROJECT")"
  if [[ -z "$TOKEN" ]]; then
    _err "Could not obtain access token for REST API call."
    return 1
  fi

  # REST endpoint: POST https://dataplex.googleapis.com/v1/projects/{project}/locations/{location}/aspectTypes
  REST_URL="${DATAPLEX_BASE}/projects/${PROJECT}/locations/${REGION}/aspectTypes?aspectTypeId=${ASPECT_TYPE_ID}"
  http_status=$(curl -s -w "%{http_code}" -o /tmp/dataplex_aspect_response.json \
    -X POST "$REST_URL" \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d @"$tmp_aspect_json")

  if [[ "$http_status" =~ ^2 ]]; then
    _info "Aspect type created via Dataplex REST API."
    return 0
  else
    _err "REST API returned HTTP $http_status creating aspect type. Response:"
    cat /tmp/dataplex_aspect_response.json >&2
    return 1
  fi
}

if create_aspect_type; then
  echo "✅ Task 3.1 complete: Aspect type '$ASPECT_DISPLAY' created."
else
  _err "Task 3 failed: could not create aspect type."
  rm -f "$tmp_aspect_json" /tmp/dataplex_aspect_response.json || true
  exit 1
fi

# Attach the aspect to the zone (create an Aspect resource attached to the zone entry)
# Dataplex aspects are attached to Entries (assets, zones, tables). Zones are parent entries.
# To attach to a zone we create an Aspect with parent equal to:
# projects/{project}/locations/{location}/lakes/{lake}/zones/{zone}
PARENT="projects/${PROJECT}/locations/${REGION}/lakes/${LAKE_ID}/zones/${ZONE_ID}"
ASPECT_JSON="$(mktemp --suffix=.json)"
# The aspect data should conform to the aspect type schema. We'll set the enum flag to "Y" here as an example.
cat > "$ASPECT_JSON" <<EOF
{
  "aspectType": "projects/${PROJECT}/locations/${REGION}/aspectTypes/${ASPECT_TYPE_ID}",
  "data": "{\"protected_raw_data_flag\":\"Y\"}"
}
EOF

_info "Attaching aspect to the zone (this creates an Aspect resource under the zone parent) ..."
TOKEN="$(gcloud auth print-access-token --project="$PROJECT")"
ASPECTS_URL="${DATAPLEX_BASE}/${PARENT}/aspects"

http_status=$(curl -s -w "%{http_code}" -o /tmp/dataplex_aspect_attach_resp.json \
  -X POST "$ASPECTS_URL" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d @"$ASPECT_JSON")

if [[ "$http_status" =~ ^2 ]]; then
  echo "✅ Task 3.2 complete: Aspect added to zone '$ZONE_DISPLAY'."
else
  _err "Failed to attach aspect to zone. HTTP $http_status. Response:"
  cat /tmp/dataplex_aspect_attach_resp.json >&2
  rm -f "$tmp_aspect_json" "$ASPECT_JSON" /tmp/dataplex_aspect_response.json /tmp/dataplex_aspect_attach_resp.json || true
  exit 1
fi

# ----------------- Cleanup and final message -----------------
rm -f "$tmp_aspect_json" "$ASPECT_JSON" /tmp/dataplex_aspect_response.json /tmp/dataplex_aspect_attach_resp.json || true

echo
echo "🎉 All tasks completed successfully."
echo " - Lake: $LAKE_DISPLAY (ID: $LAKE_ID)"
echo " - Zone: $ZONE_DISPLAY (ID: $ZONE_ID)"
echo " - Asset (bucket): gs://$BUCKET_NAME (asset id: $ASSET_ID)"
echo " - Aspect Type: $ASPECT_DISPLAY (id: $ASPECT_TYPE_ID)"
echo
echo "If the lab checker still fails: verify the project & region are the same as the lab-provided values and inspect the resources:"
echo "  gcloud dataplex lakes describe $LAKE_ID --location=$REGION --project=$PROJECT"
echo "  gcloud dataplex zones describe $ZONE_ID --lake=$LAKE_ID --location=$REGION --project=$PROJECT"
echo "  gcloud dataplex assets describe $ASSET_ID --zone=$ZONE_ID --lake=$LAKE_ID --location=$REGION --project=$PROJECT"
echo
echo "Logs / REST troubleshooting:"
echo " - View Dataplex operations in Cloud Logging or re-run failing gcloud commands without redirects to see detailed errors."
