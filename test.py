import pytest
import boto3
from io import BytesIO
from unittest.mock import patch, MagicMock
from bs4 import BeautifulSoup
from datetime import datetime
import re

# Simulación de contenido HTML real para pruebas
@pytest.fixture
def html_eltiempo():
    return """
    <html>
        <body>
            <a href="/cultura/cine/nueva-pelicula" class="headline">Nueva película en cines</a>
            <a href="/deportes/futbol/liga-aguila" class="title">Resultados de la Liga</a>
        </body>
    </html>
    """

@pytest.fixture
def html_publimetro():
    return """
    <html>
        <body>
            <a href="/noticias/bogota/incendio-en-suba">Incendio en Suba dejó 5 heridos</a>
            <a href="/deportes/futbol/final-nacional">Final del fútbol colombiano</a>
        </body>
    </html>
    """

@pytest.fixture
def s3_mock_client():
    s3 = MagicMock()
    s3.download_file.side_effect = lambda Bucket, Key, Filename: open(Filename, 'w', encoding='utf-8').write("<html><body>fake content</body></html>")
    s3.upload_file.return_value = None
    s3.put_object.return_value = {}
    return s3

@pytest.fixture
def lambda_mock_client():
    lambda_client = MagicMock()
    lambda_client.invoke.return_value = {"StatusCode": 202}
    return lambda_client

# ----------------------------
# PRUEBAS PARA LA LAMBDA 1
# ----------------------------
@patch("proyecto.requests.get")
@patch("proyecto.s3")
def test_lambda1_success(mock_s3, mock_get):
    """Prueba que verifica si los archivos HTML se descargan y suben correctamente"""
    from proyecto import app
    
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.content = b"<html>Contenido</html>"
    mock_get.return_value = mock_resp
    mock_s3.put_object.return_value = {}

    result = app({}, None)
    assert mock_get.call_count == 2
    assert mock_s3.put_object.call_count == 2

@patch("proyecto.requests.get")
@patch("proyecto.s3")
def test_lambda1_fails_on_request(mock_s3, mock_get):
    """Simula falla en la descarga HTTP"""
    from proyecto import app
    
    mock_resp = MagicMock()
    mock_resp.status_code = 404
    mock_get.return_value = mock_resp

    result = app({}, None)
    assert mock_s3.put_object.call_count == 0

# ----------------------------
# PRUEBAS PARA LA LAMBDA 2
# ----------------------------
@patch("proyecto1.s3")
@patch("proyecto1.boto3.client")
def test_lambda2_valid_eltiempo(mock_boto_client, mock_s3, tmp_path, html_eltiempo):
    """Prueba correcta con archivo de El Tiempo"""
    from proyecto1 import app, parse_el_tiempo

    fake_file = tmp_path / "page.html"
    fake_file.write_text(html_eltiempo, encoding='utf-8')

    # Mock descarga y carga en S3
    mock_s3.download_file.side_effect = lambda b, k, f: open(f, 'w', encoding='utf-8').write(html_eltiempo)
    mock_s3.upload_file.return_value = None
    mock_boto_client.return_value = MagicMock()

    event = {
        'Records': [{
            's3': {
                'bucket': {'name': 'pacialcorte3-2025'},
                'object': {'key': 'raw/contenido-eltiempo-2025-01-01-12-00.html'}
            }
        }]
    }

    result = app(event, None)
    assert result["statusCode"] == 200
    assert "Archivo procesado" in result["body"]

def test_parse_eltiempo_extracts_data(html_eltiempo):
    from proyecto1 import parse_el_tiempo

    data = parse_el_tiempo(html_eltiempo)
    assert isinstance(data, list)
    assert len(data) > 0
    assert all("titulo" in n and "categoria" in n and "enlace" in n for n in data)

@patch("proyecto1.s3")
def test_lambda2_non_html_file(mock_s3):
    """Prueba cuando el archivo no es HTML"""
    from proyecto1 import app

    event = {
        'Records': [{
            's3': {
                'bucket': {'name': 'pacialcorte3-2025'},
                'object': {'key': 'raw/archivo.pdf'}
            }
        }]
    }

    result = app(event, None)
    assert result["statusCode"] == 200
    assert "ignoró" in result["body"]

@patch("proyecto1.s3")
def test_lambda2_html_without_fecha(mock_s3, tmp_path):
    """Prueba cuando el nombre del archivo no tiene fecha válida"""
    from proyecto1 import app
    fake_html = "<html><body></body></html>"
    mock_s3.download_file.side_effect = lambda b, k, f: open(f, 'w', encoding='utf-8').write(fake_html)

    event = {
        'Records': [{
            's3': {
                'bucket': {'name': 'pacialcorte3-2025'},
                'object': {'key': 'raw/contenido-eltiempo-sinfecha.html'}
            }
        }]
    }

    with pytest.raises(ValueError, match="No se encontró una fecha válida"):
        app(event, None)

# ----------------------------
# PRUEBAS FUNCIONES DE EXTRACCIÓN
# ----------------------------

def test_parse_eltiempo_empty():
    from proyecto1 import parse_el_tiempo
    html = "<html><body>No news</body></html>"
    result = parse_el_tiempo(html)
    assert result == []

def test_parse_eltiempo_ignores_imagenes():
    from proyecto1 import parse_el_tiempo
    html = """
    <html>
        <body>
            <a href="/images/banner.jpg">Publicidad</a>
            <a href="/cultura/cine/titulo-largo">Una gran película que todos comentan</a>
        </body>
    </html>
    """
    result = parse_el_tiempo(html)
    assert len(result) == 1
    assert "titulo" in result[0]

# ----------------------------
# PRUEBA INVOCACIÓN A TERCERA LAMBDA
# ----------------------------
@patch("proyecto1.boto3.client")
@patch("proyecto1.s3")
def test_invoke_third_lambda(mock_s3, mock_boto_client, tmp_path):
    from proyecto1 import app

    html = "<a href='/deportes/futbol/partido'>Noticia</a>"
    mock_s3.download_file.side_effect = lambda b, k, f: open(f, 'w', encoding='utf-8').write(html)
    mock_s3.upload_file.return_value = None

    mock_lambda = MagicMock()
    mock_lambda.invoke.return_value = {"StatusCode": 202}
    mock_boto_client.side_effect = lambda service_name: mock_lambda if service_name == "lambda" else mock_s3

    event = {
        'Records': [{
            's3': {
                'bucket': {'name': 'pacialcorte3-2025'},
                'object': {'key': 'raw/contenido-eltiempo-2025-05-01.html'}
            }
        }]
    }

    result = app(event, None)
    assert result["statusCode"] == 200
    mock_lambda.invoke.assert_called_once()