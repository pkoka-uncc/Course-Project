import argparse
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

#Ingestion

def load_data(data_dir: str = ".") -> dict[str, pd.DataFrame]:
    p = Path(data_dir)
    files = {
        "games":         p / "games.csv",
        "games_details": p / "games_details.csv",
        "players":       p / "players.csv",
        "ranking":       p / "ranking.csv",
        "teams":         p / "teams.csv",
    }

    dfs = {}
    for name, path in files.items():
        dfs[name] = pd.read_csv(path)
        print(f"Loaded {name:15s}  {dfs[name].shape[0]:>9,} rows × {dfs[name].shape[1]} cols")
    return dfs

