import io
import json
import pytest
from datetime import datetime

import boto3
from botocore.exceptions import ClientError

# --------- Lambda 1: descarga diarios y sube a S3 ---------

# Supondremos que tu primer archivo se llama lambda1.py
from proyecto.py import app as lambda1_app

class DummyResponse:
    def __init__(self, status_code, content=b""):
        self.status_code = status_code
        self.content = content

@pytest.fixture
def fake_s3_client(monkeypatch):
    calls = []
    class FakeS3:
        def put_object(self, Bucket, Key, Body):
            calls.append((Bucket, Key, Body))
    monkeypatch.setattr(boto3, 'client', lambda service: FakeS3())
    return calls

@pytest.fixture
def fake_requests(monkeypatch):
    calls = []
    def fake_get(url):
        calls.append(url)
        return DummyResponse(200, b"<html>OK</html>")
    monkeypatch.setattr("lambda1.requests.get", fake_get)
    return calls

def test_lambda1_success(fake_s3_client, fake_requests, monkeypatch):
    # Fijar datetime para predecir el timestamp
    fixed_dt = datetime(2025,5,27,12,0)
    monkeypatch.setattr("lambda1.datetime", type("dt", (), {"utcnow": staticmethod(lambda: fixed_dt), "strftime": datetime.strftime}))
    lambda1_app({}, {})
    # Debe descargar ambos diarios
    assert 'https://www.eltiempo.com' in fake_requests
    assert 'https://www.publimetro.co/' in fake_requests
    # Verificar key y bucket en S3
    bucket, key, body = fake_s3_client[0]
    assert bucket == 'pacialcorte3-2025'
    assert key.startswith('raw/contenido-eltiempo-2025-05-27-12-00')
    assert body == b"<html>OK</html>"

def test_lambda1_download_error(monkeypatch, fake_s3_client):
    # Simular error 404
    monkeypatch.setattr("lambda1.requests.get", lambda url: DummyResponse(404))
    # No debe lanzar excepción
    lambda1_app({}, {})


# --------- Lambda 2: procesa HTML y lanza tercera Lambda ---------

# Supondremos que tu segundo archivo se llama lambda2.py
from proyecto1 import app as lambda2_app, parse_el_tiempo, extraer_noticias_publimetro

@pytest.fixture
def html_event(monkeypatch, tmp_path):
    # Crear evento S3
    bucket = 'test-bucket'
    key = 'raw/contenido-eltiempo-2025-05-27-12-00.html'
    event = {'Records': [{
        's3': {'bucket': {'name': bucket}, 'object': {'key': key}}
    }]}
    # Mock download_file
    html_file = tmp_path / "page.html"
    html_file.write_text("<html><body><a href='/categoria/nota'>Un titular muy largo de prueba</a></body></html>", encoding='utf-8')
    monkeypatch.setattr(boto3.client('s3'), 'download_file', lambda b, k, dest: html_file.rename(dest) if dest != None else None)
    # Mock upload_file
    calls = []
    monkeypatch.setattr(boto3.client('s3'), 'upload_file', lambda src, b, k: calls.append((src, b, k)))
    # Mock Lambda invoke
    lambda_calls = []
    monkeypatch.setattr(boto3.client('lambda'), 'invoke', lambda **kw: lambda_calls.append(kw) or {'StatusCode':202})
    return event, calls, lambda_calls

def test_lambda2_ignora_no_html():
    evt = {'Records':[{'s3':{'bucket':{'name':'b'},'object':{'key':'foo.txt'}}}]}
    res = lambda2_app(evt, {})
    assert res['statusCode'] == 200
    assert 'ignoró el archivo' in res['body']

def test_lambda2_procesa_html(html_event):
    event, upload_calls, lambda_calls = html_event
    res = lambda2_app(event, {})
    # Debe devolver 200 y body con la ruta final
    assert res['statusCode'] == 200
    assert 'final/periodico=eltiempo' in res['body']
    # Verificar que se haya subido el CSV
    assert upload_calls, "No se subió CSV a S3"
    # La tercera Lambda debió ser invocada una vez
    assert lambda_calls, "No se invocó la tercera Lambda"


# --------- Lambda 3: inicia Glue Crawler ---------

# Supondremos que tu tercer archivo se llama lambda3.py
from proyecto2 import lambda_handler as lambda3_handler

class FakeGlue:
    class exceptions:
        class CrawlerRunningException(Exception):
            pass

    def __init__(self):
        self.started = 0

    def start_crawler(self, Name):
        self.started += 1
        if self.started > 1:
            raise FakeGlue.exceptions.CrawlerRunningException()
        return {}

@pytest.fixture
def glue_client(monkeypatch):
    glue = FakeGlue()
    monkeypatch.setattr(boto3, 'client', lambda service: glue)
    return glue

def make_s3_event(key):
    return {'Records': [{'s3': {'object': {'key': key}} }]}

def test_lambda3_inicia_glue(glue_client):
    evt = make_s3_event('final/periodico=eltiempo/year=2025/month=05/day=27/titulares.csv')
    res = lambda3_handler(evt, {})
    assert res['statusCode'] == 200

def test_lambda3_crawler_ya_running(glue_client, capsys):
    evt = make_s3_event('final/any/file.csv')
    # Primera invocación: inicia sin errores
    lambda3_handler(evt, {})
    # Segunda invocación: la excepción es capturada y no rompe
    lambda3_handler(evt, {})
    captured = capsys.readouterr()
    assert "ya está corriendo" in captured.out


def test_lambda3_ignora_otros_archivos(glue_client):
    # Evento cuyo key no cumple
    evt = make_s3_event('raw/contenido-eltiempo-2025-05-27.html')
    res = lambda3_handler(evt, {})
    assert res['statusCode'] == 200