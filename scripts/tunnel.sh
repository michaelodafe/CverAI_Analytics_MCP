#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# scripts/tunnel.sh
# Opens an SSH tunnel so the MCP server can reach a remote Postgres instance
# from your local machine (e.g. a GCP / AWS / Azure database behind a jump host).
#
# Usage:
#   chmod +x scripts/tunnel.sh
#   SSH_HOST=<your-jump-host-ip> \
#   SSH_USER=root \
#   DB_HOST=<your-db-private-ip> \
#   DB_PORT=5432 \
#   LOCAL_PORT=5433 \
#   ./scripts/tunnel.sh
#
# Then set DATABASE_URL in .env to:
#   postgresql://user:password@localhost:5433/dbname
#
# Docker containers should use host.docker.internal:5433 (Mac/Windows)
# or 172.17.0.1:5433 (Linux) as the host.
# ─────────────────────────────────────────────────────────────────────────────

SSH_HOST=${SSH_HOST:?SSH_HOST is required (e.g. SSH_HOST=1.2.3.4)}
SSH_USER=${SSH_USER:-root}
DB_HOST=${DB_HOST:?DB_HOST is required (private IP of your database server)}
DB_PORT=${DB_PORT:-5432}
LOCAL_PORT=${LOCAL_PORT:-5433}

echo "Opening SSH tunnel: localhost:${LOCAL_PORT} → ${DB_HOST}:${DB_PORT} via ${SSH_USER}@${SSH_HOST}"

ssh -o StrictHostKeyChecking=no \
    -o ServerAliveInterval=30 \
    -o ServerAliveCountMax=20 \
    -L "${LOCAL_PORT}:${DB_HOST}:${DB_PORT}" \
    "${SSH_USER}@${SSH_HOST}" \
    -N

echo "Tunnel closed."
