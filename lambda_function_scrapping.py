import dataset
from datetime import datetime
import boto3

import requests
from bs4 import BeautifulSoup

from PIL import Image
from io import BytesIO

import re
import json

import time
import random

rds = boto3.client('rds')

def write_image_to_s3(img_array, bucket, key, region_name='us-east-1'):
    """Write an image array into S3 bucket

    Parameters
    ----------
    bucket: string
        Bucket name
    key : string
        Path in s3

    Returns
    -------
    None
    """
    s3 = boto3.resource('s3', region_name)
    bucket = s3.Bucket(bucket)
    object = bucket.Object(key)
    file_stream = BytesIO()
    im = Image.fromarray(img_array)
    im.save(file_stream, format='jpeg')
    object.put(Body=file_stream.getvalue())

def to_numeric(string, num_type='int'):
    '''
    Function to strip all non-numeric characters from string and return int or float
    INPUT - String to convert
          - num_type: either 'int' or 'float'
    OUTPUT - int or float type (returns original string if neither specified)
    '''
    if num_type == 'float':
        x = float(re.sub("[^0-9]", "", string))
    elif num_type == 'int':
        x = int(re.sub("[^0-9]", "", string))
    else:
        x = string
    return x


def savePoster(imdb_id, img_url):
    '''
    Function that fetches and save the poster image from provided url
    and saves it with the provided id (corresponding with IMDb).
    Won't replace (or even fetch) if file already exists.

    INPUT:  id from imdb, url where to find image
    OUTPUT: boolean flag if saved or not.
    '''
    import os.path

    # Get file extension
    ext = img_url.split('.')[-1]

    # Check to see if I already have it
    if os.path.isfile(f'posters/{imdb_id}.{ext}'):
        return False

    # Get image data, and save it as imdb_id
    response = requests.get(img_url)
    img = Image.open(BytesIO(response.content))
    write_image_to_s3(np.array(img), 'images-final-resmem', f'posters/{imdb_id}.{ext}', region_name='us-east-1')
    #img.save(f'posters/{imdb_id}.{ext}')
    return True


def concatenate_list_data(my_list):
    result = ''
    for element in my_list:
        result += str(element)
    return result


def time_since(start_time):
    '''
    Simple timer calculating time difference between
    start_time input parameter, and now

    OUTPUT: string ' 2m45s'
    INPUT: timestamp of starting time
    '''
    end_time = time.time()
    mins = (end_time - start_time) // 60
    secs = (end_time - start_time) - (60 * mins)
    return f'{mins:2.0f}m{secs:2.0f}s'

def concatenate_list_data(my_list):
    result = ''
    for element in my_list:
        result += str(element)
    return result


def time_since(start_time):
    '''
    Simple timer calculating time difference between
    start_time input parameter, and now

    OUTPUT: string ' 2m45s'
    INPUT: timestamp of starting time
    '''
    end_time = time.time()
    mins = (end_time - start_time) // 60
    secs = (end_time - start_time) - (60 * mins)
    return f'{mins:2.0f}m{secs:2.0f}s'

def imdb_scrape(imdb_id, db, save_image=False, debug=False):
    try:
        # Target datapoints to scrape (with provided imdb_id)
        imdb_info_dict = {}
        imdb_info_dict = {'tconst': imdb_id, 'title': '',
                          'MPAA': '', 'genre': [], 'poster_url': '',
                          'imdb_rating': '', 'num_imdb_votes': '',
                          }
        imdb_info_dict['tconst'] = imdb_id

        imdb_base_url = 'https://www.imdb.com/title/'
        #print(f'{imdb_id.ljust(10)} ', end='')

        # Main content - build URL, and soup content
        imdb_full_url = imdb_base_url + imdb_id

        r = None
        while True:
            try:
                r = requests.get(imdb_full_url,headers={'User-Agent': userAgentsList[np.random.randint(0,3)]}).content
                break
            except:
                continue

        soup = BeautifulSoup(r, 'html.parser')
        #print(f'[x]   ', end='')

        # Code from js section has json variables
        json_dict = json.loads(str(soup.findAll('script', {'type': 'application/ld+json'})[0].text))

        # Info - Movie title, year, parental content rating, poster url
        imdb_info_dict['title'] = json_dict['name']
        if 'contentRating' in json_dict:
            imdb_info_dict['MPAA'] = json_dict['contentRating']
        imdb_info_dict['poster_url'] = json_dict['image']
        #imdb_info_dict['release_year'] = int(soup.find('span', {'id': 'titleYear'}).a.text)
        #imdb_info_dict['runtime'] = to_numeric(soup.find('time')['datetime'])

        # Release date (from top header)
        #date_string = soup.find('div', {'class': 'title_wrapper'}).findAll('a')[-1].text.split(' (')[0]
        #imdb_info_dict['release_date'] = date_string

        # Genres (up to 7)
        imdb_info_dict['genre'] = ','.join(json_dict['genre'])

        # Ratings - IMDb rating (and vote count), Metacritic
        imdb_info_dict['imdb_rating'] = float(json_dict['aggregateRating']['ratingValue'])
        imdb_info_dict['num_imdb_votes'] = json_dict['aggregateRating']['ratingCount']

        # Metacritic score, if there is one
        if soup.find('div', {'class': 'metacriticScore'}) != None:
            imdb_info_dict['metacritic'] = int(soup.find('div', {'class': 'metacriticScore'}).span.text)

        # Reviews - Number of critic and public reviews (different than ratings/votes)
        num_review_list = soup.findAll('div', {'class': 'titleReviewBarItem titleReviewbarItemBorder'})
        if num_review_list != []:
            reviews = num_review_list[0].findAll('a')
            if len(reviews) > 1:
                imdb_info_dict['num_critic_reviews'] = to_numeric(reviews[1].text)
            if len(reviews) > 0:
                imdb_info_dict['num_user_reviews'] = to_numeric(reviews[0].text)
        time.sleep(random.randint(1, 10) / 100)

        savePoster(imdb_id, imdb_info_dict['poster_url'])
        time.sleep(random.randint(1,10) / 100)
        db['movie_info'].upsert(imdb_info_dict, ['imdb_id'])
        return True
    except:
        print(soup)


def lambda_handler(event, context):

    response = rds.describe_db_instances()
    db = [tmp_db for tmp_db in response['DBInstances'] if tmp_db['DBName'] == 'moviesdb'][0]
    ENDPOINT = db['Endpoint']['Address']
    PORT = db['Endpoint']['Port']
    DBID = db['DBInstanceIdentifier']

    username = 'yuetong'
    password = 'password'

    db_url = \
        "mysql+mysqlconnector://{}:{}@{}:{}/moviesdb".format(username, password, ENDPOINT, PORT)
    db = None
    while True:
        try:
            db = dataset.connect(db_url)
            break
        except:
            continue

    # Now scrape book by book, oldest first
    movies = event['movies']
    for imdb_id in movies:
        #movie_url = base_url + imdb_id
        imdb_scrape(imdb_id,db,save_image=True)
        # Update the last seen timestamp
        db['movies'].upsert({'imdb_id': imdb_id,
                            'last_seen': datetime.now()
                            }, ['imdb_id'])

    db.close()

    return {'StatusCode': 200}