'''
Run application with `python app.py` or `flask run` command
in terminal window
'''

from flask import request
from flask import Flask, render_template
import numpy as np
from waitress import serve

from PIL import Image
import requests
from io import BytesIO

from resmem import ResMem, transformer
model = ResMem(pretrained=True)
model.eval()


# Create an instance of Flask class (represents our application)
# Pass in name of application's module (__name__ evaluates to current module name)
app = Flask(__name__)

# Define Python functions that will be triggered if we go to defined URL paths
# Anything we `return` is rendered in our browser as HTML by default
@app.route("/")
def hello_world():
    resp =  "<p>API for prediction of image memorability (ResMem) with two usages.<br> 1./from_url, need to pass in image_url, the url of an online image and will immediately return the prediction of image memorability.<br> 2./from_s3, need to pass in  bucket and key, which is a s3 object that this instance could access to and the key of the image. Then it will return memorability prediction. </p>"
    return resp


@app.route('/from_url')
def pred_from_url():
    image_url = request.args.get('image_url')
    response = requests.get(image_url)
    img = Image.open(BytesIO(response.content))
    img = img.convert('RGB')
    image_x = transformer(img)
    prediction = model(image_x.view(-1, 3, 227, 227))
    return 'Memorability prediction is: {}'.format(prediction.item())

@app.route('/from_s3')
def pred_from_s3():
    bucket = request.args.get('bucket')
    key = request.args.get('key')
    obj = s3.Object(bucket, key)
    img = Image.open(obj.get()['Body'].read())
    img = img.convert('RGB')
    image_x = transformer(img)
    prediction = model(image_x.view(-1, 3, 227, 227))
    return "Memorability prediction is: {}".format(prediction.item())

if __name__ == "__main__":
    serve(app, host='0.0.0.0', port=80) # allows us to run app via `python app.py`
