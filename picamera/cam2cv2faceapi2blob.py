from os import environ
from io import BytesIO
from picamera.array import PiRGBArray
from picamera import PiCamera
from PIL import Image
from time import time, sleep
from json import dumps
from cv2 import VideoCapture, CascadeClassifier, COLOR_BGR2GRAY, cvtColor, imwrite, imencode
from azure.storage.blob import ContainerClient
from azure.cognitiveservices.vision.face import FaceClient
from msrest.authentication import CognitiveServicesCredentials
from logging import getLogger, StreamHandler, INFO


log = getLogger()
log.setLevel(INFO)
log.addHandler(StreamHandler())

haarcascade_path = '/home/worker/haarcascade_frontalface_default.xml'

# Set blob storage credentials
# exec(open('./environment').read())  # initialize environment with SAS URLs
container = ContainerClient.from_container_url(environ['landing'])

# Set FaceAPI credentials
KEY = environ['cognitive_services_key']
ENDPOINT = environ['cognitive_services_endpoint']
face_client = FaceClient(ENDPOINT, CognitiveServicesCredentials(KEY))
attributes = ['age', 'gender', 'headPose', 'smile', 'facialHair', 'glasses', 'emotion', 'hair', 'makeup', 'occlusion', 'blur', 'exposure', 'noise']
# omitted from attributes: 'accessories' - breaking the delta schema

# set highest resolution
W = 2560
H = 1920
# the face api rejects when sending the pictures uncompressed
# -> JPG is required

while True:

    camera = PiCamera()
    camera.resolution = (W, H)
    camera.iso = 400
    rawCapture = PiRGBArray(camera)
    sleep(1)  # 5s cam warmup
    epoch_now = int(time())
    camera.capture(rawCapture, format='bgr')
    bgr_array = rawCapture.array
    camera.close()
    log.info('\n\n\nCapture \n{}\n'.format(epoch_now))

    faces = (
        CascadeClassifier(haarcascade_path)
        .detectMultiScale(
            cvtColor(bgr_array, COLOR_BGR2GRAY),
            scaleFactor = 1.2,
            minNeighbors = 5,
            minSize = (30,30)
        ))
    log.info('{} faces found'.format(str(len(faces))))

    if len(faces)>=1:
        log.info('Converting frame capture to jpg')
        jpg_buffer = imencode('.jpg', bgr_array)[1]

        log.info('Call FaceAPI')
        json_response = (
            face_client
            .face
            .detect_with_stream(
                BytesIO(jpg_buffer),
                return_face_id=True,
                return_face_landmarks=False,
                return_face_attributes=attributes,
                recognition_model='recognition_01',
                return_recognition_model=False,
                detection_model='detection_01',
                face_id_time_to_live=86400,
                custom_headers=None,
                raw=True)
            .response
            .json()
        )

        if len(json_response)==0:
            log.info('FaceAPI could not detect any face')
            continue  # take next capture

        log.info('Adding capture time epoch to Face API JSON response'
        for F in json_response:
            F.update({'capturetime': epoch_now})

        json_binary = (
            dumps(
                json_response,
                ensure_ascii=False)
            .encode(
                'utf-8',
                'ignore'))

        json_path = 'face_api_json/{}.json'.format(epoch_now)
        log.info('Uploading FaceAPI json {} to blob'.format(json_path))
        blob = container.get_blob_client(json_path)
        blob.upload_blob(json_binary, overwrite=True)


        jpg_path = 'picam_jpg/{}.jpg'.format(epoch_now)
        log.info('Uploading image {} to blob'.format(jpg_path))
        blob = container.get_blob_client(jpg_path)
        blob.upload_blob(BytesIO(jpg_buffer).getvalue(), overwrite=True)


cap.release()
