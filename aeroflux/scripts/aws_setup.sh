#!/usr/bin/env bash
# scripts/aws_setup.sh — CLI-scripted creation of AeroFlux's cloud storage +
# IAM layer: the S3 lake bucket, the DynamoDB current-state table (correct
# key schema + TTL), and the aeroflux-local (writer) / aeroflux-app (reader)
# IAM users + least-privilege policies matching what's actually deployed.
#
# This is the CLI-scripted approach, kept deliberately simple — NOT
# Terraform/CDK. For a real team/production setup, the cloud layer belongs
# in Terraform/CDK (versioned, planned, reviewed, drift-detected); that's
# the production path. This script exists so one person (or a rebuild after
# deleting resources) can stand the storage/IAM layer back up in one command
# without hand-clicking the AWS console, matching today's real deployment.
#
# Goal: a new person runs this to recreate the cloud storage/IAM layer, then
# follows DEPLOYMENT.md for the Lightsail box itself (NOT scripted here —
# see the checklist at the bottom of this file).
#
#   AWS_PROFILE=<admin profile> ./scripts/aws_setup.sh plan     # default —
#                                                        dry-run, prints what
#                                                        would be created,
#                                                        makes no mutating calls
#   AWS_PROFILE=<admin profile> ./scripts/aws_setup.sh apply    # actually creates
#
# Run this with an ADMIN-privileged AWS profile/credentials — NOT
# aeroflux-local or aeroflux-app. Those are the least-privilege identities
# THIS SCRIPT creates; neither has IAM or bucket-admin permission by design
# (verified live, 2026-08-09: aeroflux-local gets AccessDenied on
# iam:ListAttachedUserPolicies, iam:GetPolicy, and even S3 bucket-metadata
# calls like s3:GetBucketLocation). The script checks the calling identity
# and refuses to proceed if it's one of those two users.
#
# Idempotent: every create step checks for the resource by name/ARN first
# (head-bucket, describe-table, get-policy, get-user, list-attached-*) and
# skips — noting so — if it already exists. Existing IAM policies are never
# silently overwritten; re-run is always safe.
#
# IMPORTANT — the IAM policy documents below are RECONSTRUCTED, not pulled
# byte-for-byte from the live account. No available AWS profile — including
# aeroflux-local itself — has IAM read permission (by design, least
# privilege), so this script could not introspect and copy the exact live
# policy JSON. These are derived from two things instead: (1) every AWS API
# call the codebase actually makes against S3/DynamoDB, found by grepping
# aeroflux_ml/io.py and scripts/smoke_cloud_backends.py — not guessed; and
# (2) the permission boundary confirmed live against the real account
# (aeroflux-local succeeds on dynamodb:{GetItem,UpdateItem,DeleteItem,Scan,
# DescribeTable} and s3:{GetObject,PutObject,DeleteObject,ListBucket}, and
# is denied every IAM action and every S3 bucket-metadata action). If the
# live account's actual attached policies differ from what's below, treat
# the live ones as authoritative — this script recreates an equivalent
# layer, it isn't a diff/audit tool against what's already there.
#
# Access keys are deliberately NOT created by this script. AWS only reveals
# an access key's secret once, at creation — capturing that in a script's
# output or a file this script writes would violate the "never print/store
# credential-bearing output" rule in CLAUDE.md. Instead, once a user exists,
# this prints the one-line `aws iam create-access-key` command for you to
# run yourself, interactively, so the secret only ever appears in your own
# terminal.

set -uo pipefail

MODE="${1:-plan}"
case "$MODE" in
  plan|apply) ;;
  *) echo "usage: $0 [plan|apply]   (default: plan)"; exit 1 ;;
esac
DRY_RUN=1
[ "$MODE" = "apply" ] && DRY_RUN=0

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# ---- config (override via env) ---------------------------------------------
: "${AWS_REGION:=us-east-1}"
: "${AWS_PROFILE:=}"                       # empty = default credential chain
: "${IAM_WRITER_USER:=aeroflux-local}"     # writer: local dev machine (sync_cloud, score_live)
: "${IAM_READER_USER:=aeroflux-app}"       # reader: the always-on Lightsail app
: "${DYNAMODB_TABLE:=aeroflux-current-state}"
: "${S3_WRITER_POLICY:=aeroflux-s3-policy}"
: "${S3_READER_POLICY:=aeroflux-s3-read-only-policy}"
: "${DYNAMODB_WRITER_POLICY:=aeroflux-dynamodb-policy}"
: "${DYNAMODB_READER_POLICY:=aeroflux-dynamodb-policy-read-only}"

awscli(){ aws ${AWS_PROFILE:+--profile "$AWS_PROFILE"} --region "$AWS_REGION" "$@"; }

log(){ echo "$(date +%H:%M:%S) | aws_setup | $*"; }
die(){ echo "ERROR: $*" >&2; exit 1; }
run(){ # mutating call — echoed always, executed only in apply mode
  log "+ $*"
  if [ "$DRY_RUN" -eq 0 ]; then
    "$@"
  fi
}

# ---- identity + safety check ------------------------------------------------
IDENTITY_JSON="$(awscli sts get-caller-identity 2>&1)" || die "AWS credentials not usable (profile: \"${AWS_PROFILE:-default chain}\", region: $AWS_REGION). Output: $IDENTITY_JSON"
ACCOUNT_ID="$(echo "$IDENTITY_JSON" | python3 -c 'import json,sys; print(json.load(sys.stdin)["Account"])')"
CALLER_ARN="$(echo "$IDENTITY_JSON" | python3 -c 'import json,sys; print(json.load(sys.stdin)["Arn"])')"
log "authenticated as: $CALLER_ARN (account $ACCOUNT_ID, region $AWS_REGION, mode=$MODE)"
case "$CALLER_ARN" in
  */"$IAM_WRITER_USER"|*/"$IAM_READER_USER")
    die "Refusing to run as $CALLER_ARN — this script must be run with an admin-privileged identity, not one of the least-privilege users it manages (\$IAM_WRITER_USER=$IAM_WRITER_USER / \$IAM_READER_USER=$IAM_READER_USER). Use a different AWS_PROFILE." ;;
esac

: "${S3_BUCKET:=aeroflux-lake-${ACCOUNT_ID}-${AWS_REGION}}"
# NOTE: the bucket actually deployed today is
# aeroflux-lake-411750981882-us-east-1-an (a manually-appended "-an" suffix
# from initial setup). S3 bucket names are globally unique across ALL AWS
# accounts, so a fresh deployment in a NEW account gets the clean
# account-scoped default above; to operate on the exact existing bucket in
# THIS account, export S3_BUCKET=aeroflux-lake-411750981882-us-east-1-an
# explicitly.

log "S3_BUCKET=$S3_BUCKET  DYNAMODB_TABLE=$DYNAMODB_TABLE"
[ "$DRY_RUN" -eq 1 ] && log "PLAN mode — no mutating AWS calls will be made. Re-run with 'apply' to create."

# ============================================================================
# 1) S3 lake bucket
# ============================================================================
setup_s3_bucket(){
  log "--- S3 bucket ---"
  if awscli s3api head-bucket --bucket "$S3_BUCKET" >/dev/null 2>&1; then
    log "bucket $S3_BUCKET already exists — skipping create."
  else
    log "bucket $S3_BUCKET does not exist (or isn't visible to this identity) — creating."
    if [ "$AWS_REGION" = "us-east-1" ]; then
      # us-east-1 is the one region where --create-bucket-configuration
      # must be omitted entirely — passing LocationConstraint=us-east-1
      # is a documented API error (IllegalLocationConstraintException).
      run aws ${AWS_PROFILE:+--profile "$AWS_PROFILE"} s3api create-bucket \
        --bucket "$S3_BUCKET" --region "$AWS_REGION"
    else
      run aws ${AWS_PROFILE:+--profile "$AWS_PROFILE"} s3api create-bucket \
        --bucket "$S3_BUCKET" --region "$AWS_REGION" \
        --create-bucket-configuration "LocationConstraint=$AWS_REGION"
    fi
  fi

  # Hardening defaults applied either way (idempotent — these are PUT/set
  # calls, safe to re-run). NOTE: could not verify these against the live
  # bucket's actual settings (this script's admin identity is the only one
  # that even *could* check — aeroflux-local is denied
  # s3:GetBucketPublicAccessBlock etc.) — these are standard-practice
  # defaults for a new bucket, not confirmed live parity.
  run awscli s3api put-public-access-block --bucket "$S3_BUCKET" \
    --public-access-block-configuration \
    BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
  run awscli s3api put-bucket-encryption --bucket "$S3_BUCKET" \
    --server-side-encryption-configuration \
    '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}'
}

# ============================================================================
# 2) DynamoDB current-state table
# ============================================================================
setup_dynamodb_table(){
  log "--- DynamoDB table ---"
  if awscli dynamodb describe-table --table-name "$DYNAMODB_TABLE" >/dev/null 2>&1; then
    log "table $DYNAMODB_TABLE already exists — skipping create."
  else
    log "table $DYNAMODB_TABLE does not exist — creating (HASH key flight_key (S), PAY_PER_REQUEST)."
    # Matches aeroflux_ml/io.py's DynamoDBStateRepository: single HASH key
    # flight_key, no sort key, no GSI (Scan+FilterExpression is the
    # deliberate demo-scale choice — see io.py's own comment on that).
    run awscli dynamodb create-table \
      --table-name "$DYNAMODB_TABLE" \
      --attribute-definitions AttributeName=flight_key,AttributeType=S \
      --key-schema AttributeName=flight_key,KeyType=HASH \
      --billing-mode PAY_PER_REQUEST
    if [ "$DRY_RUN" -eq 0 ]; then
      log "waiting for table to become ACTIVE ..."
      awscli dynamodb wait table-exists --table-name "$DYNAMODB_TABLE"
    fi
  fi

  # TTL on expires_at — every upsert_flight_state/upsert_prediction call
  # refreshes this to now + DYNAMODB_TTL_HOURS (default 48h). Check current
  # status before calling update-time-to-live: re-enabling an
  # already-ENABLED TTL on the same attribute is itself harmless, but
  # calling it needlessly on every run isn't "idempotent where possible."
  ttl_status="unknown"
  if [ "$DRY_RUN" -eq 0 ] || awscli dynamodb describe-table --table-name "$DYNAMODB_TABLE" >/dev/null 2>&1; then
    ttl_status="$(awscli dynamodb describe-time-to-live --table-name "$DYNAMODB_TABLE" \
      --query 'TimeToLiveDescription.TimeToLiveStatus' --output text 2>/dev/null || echo "unknown")"
  fi
  if [ "$ttl_status" = "ENABLED" ]; then
    log "TTL on expires_at already ENABLED — skipping."
  else
    log "enabling TTL on expires_at (current status: $ttl_status)."
    run awscli dynamodb update-time-to-live --table-name "$DYNAMODB_TABLE" \
      --time-to-live-specification "Enabled=true,AttributeName=expires_at"
  fi
}

# ============================================================================
# 3) IAM policies (least-privilege, derived from actual API usage — see
#    header comment) + users + attachments
# ============================================================================
POLICY_TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$POLICY_TMP_DIR"' EXIT

# Writer (aeroflux-local): object read/write/delete + ListBucket. Matches
# every S3 call in S3LakeStore: put_object, get_object, list_objects_v2
# (delete_object is only exercised by scripts/smoke_cloud_backends.py's
# cleanup step, but a writer needing to clean up its own writes is
# reasonable, not scope creep).
cat > "$POLICY_TMP_DIR/s3-writer.json" <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {"Sid": "AeroFluxLakeObjectReadWrite", "Effect": "Allow",
     "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
     "Resource": "arn:aws:s3:::${S3_BUCKET}/*"},
    {"Sid": "AeroFluxLakeListBucket", "Effect": "Allow",
     "Action": ["s3:ListBucket"], "Resource": "arn:aws:s3:::${S3_BUCKET}"}
  ]
}
EOF

# Reader (aeroflux-app): read-only. Matches data_access.py's lake reads —
# read_parquet (GetObject) — S3LakeStore.list also used for any future
# listing need.
cat > "$POLICY_TMP_DIR/s3-reader.json" <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {"Sid": "AeroFluxLakeObjectRead", "Effect": "Allow",
     "Action": ["s3:GetObject"], "Resource": "arn:aws:s3:::${S3_BUCKET}/*"},
    {"Sid": "AeroFluxLakeListBucket", "Effect": "Allow",
     "Action": ["s3:ListBucket"], "Resource": "arn:aws:s3:::${S3_BUCKET}"}
  ]
}
EOF

# Writer (aeroflux-local): full item CRUD + Scan + DescribeTable. Matches
# DynamoDBStateRepository (UpdateItem, Scan) plus
# scripts/smoke_cloud_backends.py (GetItem, DeleteItem for its own cleanup,
# DescribeTable for the pre-flight check).
cat > "$POLICY_TMP_DIR/dynamodb-writer.json" <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {"Sid": "AeroFluxStateReadWrite", "Effect": "Allow",
     "Action": ["dynamodb:GetItem", "dynamodb:UpdateItem", "dynamodb:DeleteItem",
                "dynamodb:Scan", "dynamodb:DescribeTable"],
     "Resource": "arn:aws:dynamodb:${AWS_REGION}:${ACCOUNT_ID}:table/${DYNAMODB_TABLE}"}
  ]
}
EOF

# Reader (aeroflux-app): read-only. Matches data_access.py's only DynamoDB
# call — recent_flight_states (Scan). DescribeTable included too (cheap,
# metadata-only, useful for the app's own health checks / baseline_metrics.sh).
cat > "$POLICY_TMP_DIR/dynamodb-reader.json" <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {"Sid": "AeroFluxStateRead", "Effect": "Allow",
     "Action": ["dynamodb:GetItem", "dynamodb:Scan", "dynamodb:DescribeTable"],
     "Resource": "arn:aws:dynamodb:${AWS_REGION}:${ACCOUNT_ID}:table/${DYNAMODB_TABLE}"}
  ]
}
EOF

ensure_policy(){ # name, json-file -> prints the policy ARN on stdout
  local name="$1" file="$2"
  local arn="arn:aws:iam::${ACCOUNT_ID}:policy/${name}"
  if awscli iam get-policy --policy-arn "$arn" >/dev/null 2>&1; then
    log "policy $name already exists — skipping create (not overwriting; see FORCE_POLICY_UPDATE below)." >&2
    if [ "${FORCE_POLICY_UPDATE:-0}" = "1" ]; then
      log "FORCE_POLICY_UPDATE=1 — creating a new default policy version from $file." >&2
      run awscli iam create-policy-version --policy-arn "$arn" --policy-document "file://$file" --set-as-default >&2
    fi
  else
    log "policy $name does not exist — creating from $file." >&2
    run awscli iam create-policy --policy-name "$name" --policy-document "file://$file" >&2
  fi
  echo "$arn"
}

ensure_user(){ # name
  local name="$1"
  if awscli iam get-user --user-name "$name" >/dev/null 2>&1; then
    log "IAM user $name already exists — skipping create."
  else
    log "IAM user $name does not exist — creating."
    run awscli iam create-user --user-name "$name"
  fi
}

ensure_attached(){ # user, policy-arn
  local user="$1" arn="$2"
  local attached
  attached="$(awscli iam list-attached-user-policies --user-name "$user" \
    --query "AttachedPolicies[?PolicyArn=='${arn}'] | length(@)" --output text 2>/dev/null || echo 0)"
  if [ "$attached" = "1" ]; then
    log "policy $arn already attached to $user — skipping."
  else
    log "attaching $arn to $user."
    run awscli iam attach-user-policy --user-name "$user" --policy-arn "$arn"
  fi
}

setup_iam(){
  log "--- IAM: writer ($IAM_WRITER_USER) ---"
  ensure_user "$IAM_WRITER_USER"
  s3w_arn="$(ensure_policy "$S3_WRITER_POLICY" "$POLICY_TMP_DIR/s3-writer.json")"
  ddbw_arn="$(ensure_policy "$DYNAMODB_WRITER_POLICY" "$POLICY_TMP_DIR/dynamodb-writer.json")"
  if [ "$DRY_RUN" -eq 0 ]; then
    ensure_attached "$IAM_WRITER_USER" "$s3w_arn"
    ensure_attached "$IAM_WRITER_USER" "$ddbw_arn"
  else
    log "(plan mode: skipping attach checks for a user/policy that may not exist yet)"
  fi

  log "--- IAM: reader ($IAM_READER_USER) ---"
  ensure_user "$IAM_READER_USER"
  s3r_arn="$(ensure_policy "$S3_READER_POLICY" "$POLICY_TMP_DIR/s3-reader.json")"
  ddbr_arn="$(ensure_policy "$DYNAMODB_READER_POLICY" "$POLICY_TMP_DIR/dynamodb-reader.json")"
  if [ "$DRY_RUN" -eq 0 ]; then
    ensure_attached "$IAM_READER_USER" "$s3r_arn"
    ensure_attached "$IAM_READER_USER" "$ddbr_arn"
  else
    log "(plan mode: skipping attach checks for a user/policy that may not exist yet)"
  fi

  echo
  log "Access keys are NOT created by this script (see header comment)."
  log "Once you're ready, pull a key yourself — the secret is shown exactly"
  log "once and never captured here:"
  log "  aws iam create-access-key --user-name $IAM_WRITER_USER   # for local dev's .env"
  log "  aws iam create-access-key --user-name $IAM_READER_USER   # for the Lightsail box's .env"
}

# ============================================================================
main(){
  setup_s3_bucket
  setup_dynamodb_table
  setup_iam
  echo
  log "done ($MODE)."
  if [ "$DRY_RUN" -eq 1 ]; then
    log "This was a PLAN — nothing was created. Re-run with 'apply' to make it real:"
    log "  AWS_PROFILE=${AWS_PROFILE:-<admin profile>} $0 apply"
  else
    log "Cloud storage/IAM layer is up. Next: DEPLOYMENT.md for the Lightsail box"
    log "(instance, Docker, firewall, DNS — see the checklist below, not scripted here)."
  fi
}

main

# ==============================================================================
# Lightsail setup checklist — NOT scripted here (manual / console steps).
# Full detail, gotchas, and exact commands live in DEPLOYMENT.md; this is
# just the shape of what's left after this script's cloud storage/IAM layer
# is up:
#
#   [ ] Create the Lightsail instance (Ubuntu, smallest plan is plenty —
#       the app container idles around 244MB/3.7GB). See DEPLOYMENT.md
#       "First-time box setup."
#   [ ] Install Docker + docker compose plugin on the instance; add the
#       default user to the `docker` group (log out/in to pick it up).
#   [ ] Check nothing is already bound to :80/:443 before starting Caddy —
#       a leftover nginx from an earlier deploy silently blocks Caddy from
#       binding at all (DEPLOYMENT.md §5.1/§5.2 — a real incident, not
#       hypothetical).
#   [ ] Open the Lightsail **console firewall** (Networking tab) for
#       80/443 (Caddy/HTTPS) and optionally 8501 (direct app access,
#       bypasses Caddy) — this is a separate gate from any OS-level
#       firewall/security group and is easy to forget (DEPLOYMENT.md §5.3).
#   [ ] DNS: point your domain (e.g. a DuckDNS hostname) at the instance's
#       static IP; Caddy auto-provisions Let's Encrypt TLS on first request
#       once DNS resolves.
#   [ ] Write the box's .env (STATE_BACKEND=dynamodb, LAKE_BACKEND=s3,
#       AWS_REGION, S3_BUCKET, DYNAMODB_TABLE, and the aeroflux-app access
#       key pulled above) — see DEPLOYMENT.md's non-printing heredoc
#       pattern for writing this over SSH without echoing secrets.
#   [ ] GHCR: the image this account pushes is private by default even in
#       a public repo — flip it to public (or give the box a read:packages
#       token) or the box's anonymous `docker pull` gets 401
#       (DEPLOYMENT.md §5.4).
#   [ ] First deploy: ./deploy.sh push && ./deploy.sh deploy — see
#       DEPLOYMENT.md for the full flow, including the sample-data-first
#       verification step before flipping STATE_BACKEND/LAKE_BACKEND live.
# ==============================================================================
