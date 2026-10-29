"""Checks that keep the repo runnable for someone who just cloned it."""

import ast
import re
import subprocess
from pathlib import Path

import pandas as pd
import yaml

ROOT = Path(__file__).resolve().parent.parent
DAGS_DIR = ROOT / "airflow" / "dags"
SEED_FILE = ROOT / "data" / "tehran_housing_seeds.csv"


def _compose() -> dict:
    return yaml.safe_load((ROOT / "docker-compose.yaml").read_text(encoding="utf-8"))


def test_housing_seed_file_is_committed_and_readable():
    assert SEED_FILE.exists(), "the housing DAG needs data/tehran_housing_seeds.csv"
    df = pd.read_csv(SEED_FILE)
    assert list(df.columns) == ["jalali_year", "avg_price_toman_sqm", "usd_rate_at_time"]
    assert len(df) > 0
    assert df["jalali_year"].is_monotonic_increasing


def test_gitignore_keeps_seed_file_and_hides_env():
    def ignored(path: str) -> bool:
        result = subprocess.run(
            ["git", "check-ignore", "--no-index", "-q", path], cwd=ROOT, check=False
        )
        return result.returncode == 0

    assert not ignored("data/tehran_housing_seeds.csv")
    assert not ignored(".env.example")
    assert ignored(".env")
    assert ignored("data/some_other_download.csv")


def test_every_file_in_dags_folder_defines_a_dag():
    for path in sorted(DAGS_DIR.glob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        calls = {
            node.func.id
            for node in ast.walk(tree)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
        }
        assert "DAG" in calls, f"{path.name} is in dags/ but defines no DAG"


def test_compose_has_no_hardcoded_credentials():
    text = (ROOT / "docker-compose.yaml").read_text(encoding="utf-8")
    # The only place a password may appear is as a ${VAR:-default} fallback.
    for match in re.finditer(r"quant_password", text):
        assert text[match.start() - 2 : match.start()] == ":-", "hardcoded password"
    assert "super-secret-key" not in text


def test_airflow_waits_for_init_which_creates_the_login():
    services = _compose()["services"]
    init_cmd = services["airflow-init"]["command"]
    assert "db migrate" in init_cmd and "users create" in init_cmd
    for name in ("airflow-webserver", "airflow-scheduler"):
        dep = services[name]["depends_on"]["airflow-init"]
        assert dep["condition"] == "service_completed_successfully", name
    assert services["airflow-init"]["depends_on"]["postgres"]["condition"] == "service_healthy"
