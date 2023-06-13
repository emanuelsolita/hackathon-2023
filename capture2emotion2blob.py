from cv2 import VideoCapture, CascadeClassifier, COLOR_BGR2GRAY, cvtColor, imwrite, imencode
import cv2
from msrest.authentication import CognitiveServicesCredentials
from azure.cognitiveservices.vision.face import FaceClient
from time import time, sleep
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from io import BytesIO
from json import dumps


connection_string='DefaultEndpointsProtocol=https;AccountName=landing123emhol;AccountKey=1RsQ5kv+PfpyKB4KeoOaLdkuZTNTRTbqF7EuZz1PZ02PYAD+mJIOJNxr0W0wX7+uSGrVv8Pn0HdV+AStKy3OBQ==;EndpointSuffix=core.windows.net'
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
container = blob_service_client.get_container_client('landing')

ENDPOINT = "https://hackation-2023.cognitiveservices.azure.com/"
KEY = "7d3b9dfa499d410fb59225479acf1ee7"
face_client = FaceClient(ENDPOINT, CognitiveServicesCredentials(KEY))
attributes = ['age', 'gender', 'headPose', 'smile', 'facialHair', 'glasses', 'emotion', 'hair', 'makeup', 'occlusion', 'blur', 'exposure', 'noise']

cap = cv2.VideoCapture(0)

while True:
    ret, frame = cap.read()
    rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2BGRA)

    cv2.imshow('frame', rgb)
    if cv2.waitKey(1) & 0xFF == ord('a'):
        epoch = str(int(1000*time()))
        filename = 'capture_' + epoch + '.jpg'
        out = cv2.imwrite(filename, frame)
        jpg_buffer = imencode('.jpg', frame)[1]

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

        for F in json_response:
            F.update({'capturetime': epoch})

        json_binary = (
            dumps(
                json_response,
                ensure_ascii=False)
            .encode(
                'utf-8',
                'ignore'))
        
        json_path = 'face_api_json/{}.json'.format(epoch)
        blob = container.get_blob_client(json_path)
        blob.upload_blob(json_binary, overwrite=True)
        
        
    if cv2.waitKey(1) & 0xFF == ord('q'):
        break

cap.release()
cv2.destroyAllWindows()