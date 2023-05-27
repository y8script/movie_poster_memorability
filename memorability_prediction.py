import boto3
from PIL import Image
from resmem import ResMem, transformer
import pandas as pd
import numpy as np
import mysql.connector


def read_image_from_s3(bucket, key, region_name='us-east-1'):
    """Load image file from s3.

    Parameters
    ----------
    bucket: string
        Bucket name
    key : string
        Path in s3

    Returns
    -------
    np array
        Image array
    """
    s3 = boto3.resource('s3', region_name=region_name)
    bucket = s3.Bucket(bucket)
    object = bucket.Object(key)
    response = object.get()
    file_stream = response['Body']
    im = Image.open(file_stream)
    return im


def main():
    '''
    iterate through all movie data in RDS instance, get movie poster images from s3 bucket,
    compute memorability prediction, and update memorability score to the RDS instance.
    '''
    rds = boto3.client('rds')
    response = rds.describe_db_instances()
    db = [tmp_db for tmp_db in response['DBInstances'] if tmp_db['DBName'] == 'moviesdb'][0]
    ENDPOINT = db['Endpoint']['Address']
    PORT = db['Endpoint']['Port']
    DBID = db['DBInstanceIdentifier']
    username = 'yuetong'
    password = 'password'
    conn = mysql.connector.connect(host=ENDPOINT,
                                   user=username,
                                   passwd=password,
                                   port=PORT,
                                   database='moviesdb')
    cur = conn.cursor()
    cur.execute('''SELECT tconst from movie_info''')
    query_results = cur.fetchall()
    tconst_df = pd.DataFrame(query_results)
    model = ResMem(pretrained=True)
    model.eval()

    for tconst in tconst_df[0]:
        cur.execute('''SELECT poster_url FROM movie_info WHERE tconst = \'{}\' '''.format(tconst))
        query_results = cur.fetchall()
        np.array(query_results)
        img = read_image_from_s3('images-final-resmem', 'posters/' + str(tconst) + '.jpg')
        img = img.convert('RGB')
        image_x = transformer(img)
        prediction = model(image_x.view(-1, 3, 227, 227))
        print(prediction.item())
        cur.execute('''UPDATE movie_info SET memorability = {} WHERE tconst = \'{}\''''.format(str(prediction.item()), tconst))
        conn.commit()
        # cur.execute('''SELECT memorability FROM movie_info WHERE tconst = \'{}\' '''.format(tconst))
        # query_results = cur.fetchall()
        # print(query_results)

    cur.close()
    conn.close()
    return True

if __name__ == "__main__":
    main()


