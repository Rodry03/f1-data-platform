import sys
import subprocess
from pathlib import Path

year = int(sys.argv[1]) if len(sys.argv) > 1 else None

def run(cmd, cwd=None):
    print(f"\n>>> {cmd}")
    result = subprocess.run(cmd, shell=True, cwd=cwd)
    if result.returncode != 0:
        print(f"\nError en: {cmd}")
        sys.exit(result.returncode)

root = Path(__file__).parent

if year:
    run(f"python ingest_f1.py {year}", cwd=root)
else:
    print("Sin año especificado, saltando ingesta.")

run("python export_seeds.py", cwd=root)
run("dbt seed", cwd=root / "f1_dbt")
run("dbt run", cwd=root / "f1_dbt")
run("dbt test", cwd=root / "f1_dbt")

print(f"\nPipeline completado.")
