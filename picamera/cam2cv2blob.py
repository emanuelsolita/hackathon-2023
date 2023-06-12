from os import environ
from io import BytesIO
from azure.storage.blob import ContainerClient
from picamera.array import PiRGBArray
from picamera import PiCamera
from PIL import Image
from time import time, sleep
from logging import getLogger, StreamHandler, INFO
from cv2 import CascadeClassifier, COLOR_BGR2GRAY, cvtColor

log = getLogger()
log.setLevel(INFO)
log.addHandler(StreamHandler())

# exec(open('./secrets').read())  # initialize environment with SAS URLs
container = ContainerClient.from_container_url(environ['landing'])

W = 2560
H = 1920

stream = BytesIO()
output = BytesIO()

while True:

    camera = PiCamera()
    camera.resolution = (W, H)
    camera.iso = 400
    rawCapture = PiRGBArray(camera)
    sleep(5)  # 5s cam warmup

    file_path = 'picam_grey_jpeg/{}'.format(str(round(time())))
    log.info('\n\n\nCapture picture\n{}\n'.format(file_path))
    camera.capture(rawCapture, format='bgr')
    camera.close()

    image = cvtColor(rawCapture.array, COLOR_BGR2GRAY)

    # wget https://raw.githubusercontent.com/kipr/opencv/master/data/haarcascades/haarcascade_frontalface_default.xml
    face_cascade = CascadeClassifier('/home/worker/haarcascade_frontalface_default.xml')

    faces = face_cascade.detectMultiScale(
        image,
        scaleFactor = 1.2,
        minNeighbors = 5,
        minSize = (30,30)
    )
    log.info('{} faces found'.format(str(len(faces))))

    if len(faces)>=1:
        log.info('Converting image to jpeg')
        jpeg_io = BytesIO()
        Image.fromarray(image).save(jpeg_io, format='JPEG')

        log.info('Uploading image {} to blob'.format(file_path))
        blob = container.get_blob_client(file_path)
        blob.upload_blob(jpeg_io.getvalue(), overwrite=True)
