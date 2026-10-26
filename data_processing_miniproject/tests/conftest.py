import sys
from pathlib import Path

# scripts/ bukan package; tambahkan ke path agar modul dapat diimpor.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
