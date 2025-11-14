import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]   # <- parent of "test" = project root
SRC = ROOT / "src"

if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from incremental_ingestion import download_url_template


def test_download_url():
    url = download_url_template(2024, 1)
    assert url.endswith("yellow_tripdata_2024-01.parquet")
