#!/bin/bash
set -e

# Create multiple databases specified in POSTGRES_MULTIPLE_DATABASES
if [ -n "$POSTGRES_MULTIPLE_DATABASES" ]; then
  for db in $(echo $POSTGRES_MULTIPLE_DATABASES | tr ',' ' '); do
    psql -U "$POSTGRES_USER" -d postgres -c "SELECT 1 FROM pg_database WHERE datname = '$db';" | grep -q 1 || psql -U "$POSTGRES_USER" -d postgres -c "CREATE DATABASE $db;"
  done
fi