#!/usr/bin/env bash
# deploy.sh — manual deploy/rollback for the AeroFlux Streamlit app, mirroring
# e2e.sh's style. The GitHub Action (.github/workflows/deploy-ui.yml) does the
# same build -> push -> deploy automatically on every push to
# aeroflux/aeroflux_ui/**; this is for a manual run, a local build/smoke-test
# with no SSH needed, or a rollback the Action doesn't express.
#
#   ./deploy.sh build              # local image build only — no push/SSH needed
#   ./deploy.sh push                # build + push to GHCR (needs `docker login ghcr.io` first)
#   ./deploy.sh deploy [tag]        # SSH to Lightsail, pull [tag|latest], up -d
#   ./deploy.sh rollback <tag>      # same as deploy, but requires an explicit tag
#   ./deploy.sh status              # SSH + curl: show running containers and reachability
#
# SSH config (only needed for deploy/rollback/status — build/push work
# without any of this):
#   LIGHTSAIL_SSH_HOST       the Lightsail instance's IP or hostname
#   LIGHTSAIL_SSH_USER       default: ubuntu
#   LIGHTSAIL_SSH_KEY_PATH   path to the private key
#   REMOTE_DIR               where docker-compose.lightsail.yml lives on the
#                            instance, default: /opt/aeroflux
set -uo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
UI_DIR="$ROOT/aeroflux/aeroflux_ui/streamlit_app"
IMAGE="ghcr.io/jonathanwilsonami/aeroflux-ui"

: "${LIGHTSAIL_SSH_HOST:=}"
: "${LIGHTSAIL_SSH_USER:=ubuntu}"
: "${LIGHTSAIL_SSH_KEY_PATH:=}"
: "${REMOTE_DIR:=/opt/aeroflux}"

log(){ echo "$(date +%H:%M:%S) | deploy | $*"; }
die(){ echo "ERROR: $*" >&2; exit 1; }

need_ssh(){
  [ -n "$LIGHTSAIL_SSH_HOST" ] || die "LIGHTSAIL_SSH_HOST not set — export it (see this script's header)"
  [ -n "$LIGHTSAIL_SSH_KEY_PATH" ] || die "LIGHTSAIL_SSH_KEY_PATH not set — export the path to the SSH private key"
  [ -f "$LIGHTSAIL_SSH_KEY_PATH" ] || die "LIGHTSAIL_SSH_KEY_PATH ($LIGHTSAIL_SSH_KEY_PATH) not found"
}
ssh_cmd(){ ssh -i "$LIGHTSAIL_SSH_KEY_PATH" -o StrictHostKeyChecking=accept-new "$LIGHTSAIL_SSH_USER@$LIGHTSAIL_SSH_HOST" "$@"; }

cmd_build(){
  log "building $IMAGE:local from $UI_DIR"
  docker build -t "$IMAGE:local" "$UI_DIR"
  log "built. run it locally with: docker run --rm -p 8501:8501 $IMAGE:local"
}

cmd_push(){
  cmd_build
  log "tagging + pushing $IMAGE:latest (docker login ghcr.io first if you haven't)"
  docker tag "$IMAGE:local" "$IMAGE:latest"
  docker push "$IMAGE:latest"
}

cmd_deploy(){
  local tag="${1:-latest}"
  need_ssh
  log "deploying $IMAGE:$tag to $LIGHTSAIL_SSH_HOST:$REMOTE_DIR"
  ssh_cmd "cd '$REMOTE_DIR' && IMAGE_TAG='$tag' docker compose -f docker-compose.lightsail.yml pull && IMAGE_TAG='$tag' docker compose -f docker-compose.lightsail.yml up -d"
  log "deployed. check: ./deploy.sh status"
}

cmd_rollback(){
  local tag="${1:?usage: ./deploy.sh rollback <image-tag>}"
  log "rolling back to $IMAGE:$tag"
  cmd_deploy "$tag"
}

cmd_status(){
  need_ssh
  ssh_cmd "cd '$REMOTE_DIR' && docker compose -f docker-compose.lightsail.yml ps"
  echo
  curl -sf "https://aeroflux.duckdns.org/_stcore/health" >/dev/null 2>&1 && echo "https (Caddy):   UP" || echo "https (Caddy):   down"
  curl -sf "http://$LIGHTSAIL_SSH_HOST:8501/_stcore/health" >/dev/null 2>&1 && echo "direct :8501:    UP" || echo "direct :8501:    down"
}

case "${1:-}" in
  build) cmd_build ;;
  push) cmd_push ;;
  deploy) cmd_deploy "${2:-latest}" ;;
  rollback) cmd_rollback "${2:-}" ;;
  status) cmd_status ;;
  *) echo "usage: ./deploy.sh {build|push|deploy [tag]|rollback <tag>|status}"; exit 1 ;;
esac
