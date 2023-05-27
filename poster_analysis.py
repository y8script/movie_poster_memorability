import boto3
import pandas as pd
import numpy as np

rds = boto3.client('rds', region_name='us-east-1')

response = rds.describe_db_instances()
db = [ tmp_db for tmp_db in response['DBInstances'] if tmp_db['DBName'] == 'moviesdb'][0]
ENDPOINT = db['Endpoint']['Address']
PORT = db['Endpoint']['Port']
DBID = db['DBInstanceIdentifier']
username = 'yuetong'
password = 'password'

import mysql.connector
conn =  mysql.connector.connect(host=ENDPOINT,
                                user=username,
                                passwd=password,
                                port=PORT,
                                database='moviesdb')
cur = conn.cursor()

cur.execute('''SELECT * from movie_info''')
query_results = cur.fetchall()

df_all = pd.DataFrame(query_results)

cur.close()
conn.close()

column_names = ['id','tconst','title','MPAA','genre','poster_url','imdb_rating','num_imdb_votes','memorability']
df_all.columns = column_names
print('Correlation between imdb rating and memorability score:')
print(df_all['imdb_rating'].corr(df_all['memorability']))
print('Correlation between total number of votes and memorability score:')
print(df_all['num_imdb_votes'].corr(df_all['memorability']))
print('Mean value of rating, votes, memorability grouped by MPAA:')
print(df_all.groupby('MPAA').mean())
