from pathlib import Path
from src.audit.audit_manager import AuditManager
ROOT = Path(__file__).resolve().parents[2]
if __name__ == "__main__":
    outputs = AuditManager(ROOT / "audit/runtime").export_parquet_views()
    for name, path in outputs.items():
        print(name, path)
