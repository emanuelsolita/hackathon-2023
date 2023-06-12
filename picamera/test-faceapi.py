from os import environ, system
from io import BytesIO
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

# Set blob storage credentials
exec(open('./picamera/environment').read())  # initialize environment with SAS URLs
container = ContainerClient.from_container_url(landing)

# Set FaceAPI credentials
KEY = cognitive_services_key
ENDPOINT = cognitive_services_endpoint
face_client = FaceClient(ENDPOINT, CognitiveServicesCredentials(KEY))
attributes = ['age', 'gender', 'headPose', 'smile', 'facialHair', 'glasses', 'emotion', 'hair', 'makeup', 'occlusion', 'blur', 'exposure', 'noise']
# not  'accessories'
cap = VideoCapture(0)

while(cap.isOpened()):
    epoch_now = int(time())
    ret, frame = cap.read()
    if not ret:
        cap.release()  # force restart when no frame is returned

    log.info('\n\n\nCapture \n{}\n'.format(epoch_now))

    bgr_array = cvtColor(frame, COLOR_BGR2GRAY)

    face_cascade = CascadeClassifier('./picamera/haarcascade_frontalface_default.xml')

    faces = face_cascade.detectMultiScale(
        bgr_array,
        scaleFactor = 1.2,
        minNeighbors = 5,
        minSize = (30,30)
    )
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
            print(f)

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


