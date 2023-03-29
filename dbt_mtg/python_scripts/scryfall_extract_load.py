import pandas as pd
from sqlalchemy import create_engine
import os
import sys
import subprocess

scryfall_oracle = 'https://data.scryfall.io/oracle-cards/oracle-cards-20230329090230.json'
df = pd.read_json(scryfall_oracle)

name = 'mtg_oracle'

user = sys.argv[1]
pw = sys.argv[2]
host = sys.argv[3]
db = sys.argv[4]


engine = create_engine("mariadb+pymysql://{}:{}@{}:3306/{}".format(user,pw,host,db))
con = engine

for range in df:
    df[range] = df[range].astype('str')

df.to_sql(
    name, con,
    schema=None,
    if_exists='replace',
    index=False,
    index_label=None,
    chunksize=None,
    dtype=None,
    method=None)


os.chdir('../python_scripts')
c = 'dbt snapshot --select mtg_oracle_history'
subprocess.run(c, shell=True, capture_output=True )
