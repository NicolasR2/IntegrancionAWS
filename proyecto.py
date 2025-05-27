import boto3
import requests
from datetime import datetime

s3 = boto3.client('s3')
BUCKET = 'pacialcorte3-2025'

def app(event, context):
    now = datetime.utcnow()
    timestamp = now.strftime('%Y-%m-%d-%H-%M')

    diarios = {
        'eltiempo': 'https://www.eltiempo.com',
        'publimetro': 'https://www.publimetro.co/',
    }

    for nombre, url in diarios.items():
        resp = requests.get(url)
        if resp.status_code == 200:
            key = f'raw/contenido-{nombre}-{timestamp}.html'
            s3.put_object(Bucket=BUCKET, Key=key, Body=resp.content)
            print(f'Subido: s3://{BUCKET}/{key}')
        else:
            print(f'Error al descargar {url}')
