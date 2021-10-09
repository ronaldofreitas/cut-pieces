from flask import Flask, request, jsonify
from pydub import AudioSegment
from google.cloud import storage
import os
storage_client = storage.Client()
#bucket_origem = storage_client.get_bucket("catalobyte-output")
bucket_destino = storage_client.get_bucket("catalobyte-play")

'''
trigger: function 'queue-to-speech'
task: 'output-to-cut'

vai receber mensagem de uma fila

vai lê o bucket 'catalobyte-output'
vai escrever no 'catalobyte-play/full/' e 'catalobyte-play/pieces/'
vai apagar do 'catalobyte-output'

um arquivo grande (FLAC com 2 horas e meia) deve durar até uns 2 minutos pra fazer tudo


https://cut-play-iafcvqe73a-ue.a.run.app
'''

app = Flask(__name__)

@app.route("/", methods=["POST"])
def receive():

    data = request.get_json()
    gs_uri = data['gs_uri'] # uri original do arquivo em 'catalobyte-input'
    #gs_uri = 'gs://catalobyte-output/VJaYYVBO4mOb2Yf9y8Nx3VR2Rfz2/a5p7k0r7a8/1158732356/1628633099505.flac'
    path_div = gs_uri.split('/')
    fire_user_uid = path_div[3]
    manticore_id = path_div[4]
    uid_file = path_div[5]
    full_file_name = path_div[6]
    remov_ext = full_file_name.split('.')
    name_file = remov_ext[0]

    tempfile = full_file_name
    #destname = name_file+".aac"
    destname = name_file+".mp3"

    if not os.path.exists(tempfile):
        os.mknod(tempfile)
    with open(tempfile, 'wb+') as file_obj:
        storage_client.download_blob_to_file(gs_uri, file_obj)

    a = AudioSegment.from_file(tempfile)
    a.export(destname, bitrate="30k", format="mp3")
    #a.export(destname, bitrate="30k", format="adts")

    blob = bucket_destino.blob(fire_user_uid+'/'+manticore_id+'/'+uid_file+'/full/'+destname)
    blob.upload_from_filename(destname)

    '''
    gera os 'pices' de audio utilizados na pesquisa Manticore
    dura = int(round(a.duration_seconds))

    def chunks(lst, n):
        """Yield successive n-sized chunks from lst."""
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    rr = list(chunks(range(0, dura), 10))

    for r in rr:
        rangeList = r
        frs = rangeList[0]
        lst = (rangeList[len(rangeList)-1:][0]+1)
        f_frs = (frs*1000)
        f_lst = (lst*1000)
        slice = a[f_frs:f_lst]
        namef = f'{frs}.aac'
        slice.export(namef, bitrate="30k", format="adts")
        blob_f = bucket_destino.blob(fire_user_uid+'/'+manticore_id+'/'+uid_file+'/pieces/'+namef)
        blob_f.upload_from_filename(namef)

        # catalobyte-output/VJaYYVBO4mOb2Yf9y8Nx3VR2Rfz2/a5p7k0r7a8/1158732356
        #bucket_origem.delete_blob(fire_user_uid+'/'+manticore_id+'/'+uid_file+'/')

    '''
    return "ok"

if __name__ == "__main__":
    # Used when running locally only. When deploying to Cloud Run,
    # a webserver process such as Gunicorn will serve the app.
    app.run(host="localhost", port=8080, debug=True)
