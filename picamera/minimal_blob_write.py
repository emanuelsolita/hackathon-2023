from os import environ
from azure.storage.blob import ContainerClient
from logging import getLogger, StreamHandler, INFO


log = getLogger()
log.setLevel(INFO)
log.addHandler(StreamHandler())

container = ContainerClient.from_container_url(environ['landing'])
# environ['landing'] holds the SAS URL created in the Azure portal
# The SAS URL should be created on container level (not storage account)
# and have minimal permissions (write at least)

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

