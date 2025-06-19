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

df = pd.read_csv(Path(root_dir, 'expected_output.csv'))

con = sqlite3.connect(Path(root_dir, 'expected_output.db'))
sql_types = {
    "PortNumber": "TEXT",
    "AccNumber": "TEXT",
    "LocNumber": "TEXT",
    "CountryCode": "TEXT",
    "Latitude": "REAL",
    "Longitude": "REAL",
    "StreetAddress": "TEXT",
    "PostalCode": "TEXT",
    "OccupancyCode": "TEXT",
    "ConstructionCode": "TEXT",
    "LocPerilsCovered": "TEXT",
    "BuildingTIV": "REAL",
    "OtherTIV": "REAL",
    "ContentsTIV": "REAL",
    "BITIV": "REAL",
    "LocCurrency": "TEXT"
}
df.to_sql("output", con, if_exists="replace", index=False, dtype=sql_types)
print('sqlite db created successfully')
