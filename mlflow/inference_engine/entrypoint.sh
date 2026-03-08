#!/bin/bash
set -e

# Auto-create symlink from host artifact paths to container mount point.
# MLflow stores absolute host paths in the SQLite DB (e.g. /home/user/.../mlflow/experiments/mlruns/...).
# Inside the container, those files live at /mlflow-data/mlruns/...
# This script reads the actual artifact base path from the DB and symlinks it to /mlflow-data.

ARTIFACT_BASE=$(python3 -c "
import sqlite3, os, sys
uri = os.environ.get('MLFLOW_TRACKING_URI', '')
if not uri.startswith('sqlite:'):
    sys.exit(0)
db_path = uri.replace('sqlite:////', '/').replace('sqlite:///', '')
try:
    conn = sqlite3.connect(db_path)
    row = conn.execute('SELECT artifact_location FROM experiments WHERE experiment_id > 0 LIMIT 1').fetchone()
    if row:
        path = row[0]
        idx = path.find('/mlruns')
        if idx > 0:
            print(path[:idx])
except Exception:
    pass
" 2>/dev/null || true)

if [ -n "$ARTIFACT_BASE" ] && [ "$ARTIFACT_BASE" != "/mlflow-data" ]; then
    mkdir -p "$(dirname "$ARTIFACT_BASE")"
    ln -sfn /mlflow-data "$ARTIFACT_BASE"
    echo "[inference] Symlinked artifact path: $ARTIFACT_BASE -> /mlflow-data"
fi

exec python inference_app.py "$@"
