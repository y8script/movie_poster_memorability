import boto3
from PIL import Image
from io import BytesIO
from resmem import ResMem, transformer
import time
import numpy as np
from matplotlib import pyplot as plt


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


s3 = boto3.client('s3')


def main():
    bucket_name = 'images-final-resmem'
    response = s3.list_buckets()
    buckets = [bucket['Name'] for bucket in response['Buckets'] if bucket['Name'] == bucket_name]
    if len(buckets) == 0:
        s3.create_bucket(Bucket=bucket_name,CreateBucketConfiguration={'LocationConstraint': 'us-east-1'})
    with open("./test.png", "rb") as f:
        s3.upload_fileobj(f, bucket_name, "test_image.png")

    tmp_img = read_image_from_s3(bucket_name, 'test_image.png')
    tmp_img = tmp_img.convert('RGB')

    model = ResMem(pretrained=True)
    model.eval()

    t1 = time.time()
    image_x = transformer(tmp_img)
    # Run the preprocessing function

    prediction = model(image_x.view(-1, 3, 227, 227))

    print(time.time() - t1)
    print(prediction[0][0])




if __name__ == "__main__":
    main()
