# %%
import json

import pymongo
import psycopg2
import pandas as pd

from envyaml import EnvYAML
from sqlalchemy import create_engine

def main():
    """This script ingest data from MongoDB database to Postgres database"""
    
    config = EnvYAML('configuration.yml')
    
    # source
    source_config = config['mongodb']
    source_host = source_config['host']
    source_port = source_config['port']
    source_database = source_config['database']
    source_collection = source_config['collection']
    
    mongodb = pymongo.MongoClient(f"mongodb://{source_host}:{source_port}/")
    nofulljobs_collection = mongodb[source_database][source_collection]

    # sink
    sink_config = config['postgres']
    sink_host = sink_config['host']
    sink_port = sink_config['port']
    sink_user = sink_config['user']
    sink_password = sink_config['password']
    sink_database = sink_config['database']
    sink_table = sink_config['database']
    sink_schema = sink_config['database']
    
    dbt = create_engine(f'postgresql://{sink_user}:{sink_password}@{sink_host}:{sink_port}/{sink_database}')

    # load from source
    df = pd.DataFrame().from_dict(nofulljobs_collection.find())

    # serialize lists and nested dictionaries
    for col in df.columns:
        if type(df[col].iloc[0]) in (dict, list):
            df[col] = df[col].apply(lambda x: json.dumps(x))
        else:
            df[col] = df[col].apply(lambda x: str(x))

    # write to sink
    df.to_sql(
        name=sink_table,
        con=dbt,
        schema=sink_schema,
        if_exists='replace',
        method='multi'
    )
    
if __name__ == '__main__':
    main()
# %%
