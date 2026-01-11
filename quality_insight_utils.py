import json
from pathlib import Path


def read_quality_insight_records(log_path: Path):
    if not log_path.exists():
        return []

    records = []
    try:
        with log_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    except OSError:
        return []

    return records
