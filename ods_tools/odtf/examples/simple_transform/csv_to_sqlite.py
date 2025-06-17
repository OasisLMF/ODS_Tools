"""
small helper script to transform input.csv into a sqlite db file
"""

import sqlite3
import pandas as pd
from pathlib import Path


root_dir = Path(__file__).parent

df = pd.read_csv(Path(root_dir, 'csv', 'input.csv'))


con = sqlite3.connect(Path(root_dir, 'sqlite', 'input.db'))
df.to_sql("location", con, if_exists="replace")

print('sqlite db created successfully')
