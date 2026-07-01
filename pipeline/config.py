from dotenv import load_dotenv
import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")


def _resolve_path(value: str | None) -> str | None:
	if not value:
		return None
	path = Path(value)
	if path.is_absolute():
		if path.exists():
			return str(path)

		parts = path.parts
		if PROJECT_ROOT.name in parts:
			index = parts.index(PROJECT_ROOT.name)
			return str((PROJECT_ROOT / Path(*parts[index + 1 :])).resolve())

		return str(path)
	return str((PROJECT_ROOT / path).resolve())


PIPELINE_LOGS_PATH = _resolve_path(os.getenv("PIPELINE_LOGS_PATH"))
CACHE_PATH = _resolve_path(os.getenv("CACHE_PATH"))
RAW_DATA_PATH = _resolve_path(os.getenv("RAW_DATA_PATH"))
PROCESSED_DATA_PATH = _resolve_path(os.getenv("PROCESSED_DATA_PATH"))
YEAR = 2024
ROUND = 1
SESSION = 'R'
WHEREHOUSE_PATH = _resolve_path(os.getenv("WHEREHOUSE_PATH"))
ANALYSIS_PATH = _resolve_path(os.getenv("ANALYSIS_PATH"))
VALIDATION_PATH = _resolve_path(os.getenv("VALIDATION_PATH"))
GOOGLE_APPLICATION_CREDENTIALS = _resolve_path(os.getenv("GOOGLE_APPLICATION_CREDENTIALS"))
BIGQUERY_PROJECT = os.getenv("BIGQUERY_PROJECT")
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET")
FAILED_RACES_PATH = _resolve_path(os.getenv("FAILED_RACES_PATH"))
SUCCESSFUL_RACES_PATH = _resolve_path(os.getenv("SUCCESSFUL_RACES_PATH"))
