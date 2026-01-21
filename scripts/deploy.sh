#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DEPLOY_DIR="${DEPLOY_DIR:-/opt/ww-orderbook}"
SERVICE_NAME="${SERVICE_NAME:-ww-orderbook}"
DATA_DIR="${DATA_DIR:-/var/lib/ww-orderbook}"

if [ "${SKIP_GIT_PULL:-0}" = "1" ]; then
  echo "Skipping git pull."
else
  echo "Updating repo..."
  git -C "$REPO_DIR" pull --ff-only
fi

echo "Syncing to $DEPLOY_DIR..."
sudo mkdir -p "$DEPLOY_DIR"
sudo rsync -a --delete \
  --exclude .git \
  --exclude node_modules \
  --exclude .env \
  --exclude data \
  --exclude dist \
  "$REPO_DIR/" "$DEPLOY_DIR/"

echo "Installing dependencies..."
sudo chown -R ubuntu:ubuntu "$DEPLOY_DIR"
npm install --prefix "$DEPLOY_DIR"

echo "Building backend..."
npm run build --prefix "$DEPLOY_DIR"

echo "Ensuring data dir..."
sudo mkdir -p "$DATA_DIR"
sudo chown -R www-data:www-data "$DATA_DIR"

echo "Fixing ownership..."
sudo chown -R www-data:www-data "$DEPLOY_DIR"

echo "Restarting service..."
sudo systemctl restart "$SERVICE_NAME"
sudo systemctl status "$SERVICE_NAME" --no-pager
