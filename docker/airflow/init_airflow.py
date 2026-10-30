"""Initialize Airflow metadata and its local development admin password."""

import json
import os
import subprocess
from pathlib import Path

password = os.environ["AIRFLOW_ADMIN_PASSWORD"]
if not password:
    raise ValueError("AIRFLOW_ADMIN_PASSWORD must not be empty")

subprocess.run(["airflow", "db", "migrate"], check=True)

password_file = Path(os.environ["AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE"])
password_file.parent.mkdir(parents=True, exist_ok=True)
# Replace the complete JSON file so readers cannot see a partial write.
temporary_file = password_file.with_suffix(".tmp")
temporary_file.write_text(json.dumps({"admin": password}) + "\n", encoding="utf-8")
temporary_file.chmod(0o600)
temporary_file.replace(password_file)
print("Airflow admin password synchronized from AIRFLOW_ADMIN_PASSWORD.")
