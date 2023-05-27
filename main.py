from resmem import ResMem, transformer
import time
import torch

model = ResMem(pretrained=True)
model.eval()

from PIL import Image

img = Image.open('C:/Users/y8scr/OneDrive - The University of Chicago/Desktop/fmri_Results.jpg') # This loads your image into memory
img = img.convert('RGB')
# This will convert your image into RGB, for instance if it's a PNG (RGBA) or if it's black and white.

model.eval()
# Set the model to inference mode.

t1 = time.time()
image_x = transformer(img)
# Run the preprocessing function


prediction = model(image_x.view(-1, 3, 227, 227))

print(time.time() - t1)

print(torch.cuda.is_available())

print(prediction)
print(type(model))
