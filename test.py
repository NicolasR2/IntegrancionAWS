import pytest
import boto3
from io import BytesIO
from unittest.mock import patch, MagicMock
from bs4 import BeautifulSoup
from datetime import datetime
import re

import pytest
import boto3
from io import BytesIO
from unittest.mock import patch, MagicMock
from bs4 import BeautifulSoup
from datetime import datetime
import re

# ----------------------------
# FIXTURES
# ----------------------------

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

# ----------------------------
# PRUEBAS PARA LA LAMBDA 2
# ----------------------------

@patch("proyecto1.boto3.client")
def test_lambda2_valid_eltiempo(mock_boto_client, tmp_path, html_eltiempo):
    from proyecto1 import app

    s3_mock = MagicMock()
    lambda_mock = MagicMock()
    s3_mock.download_file.side_effect = lambda b, k, f: open(f, 'w', encoding='utf-8').write(html_eltiempo)
    s3_mock.upload_file.return_value = None
    lambda_mock.invoke.return_value = {"StatusCode": 202}

    def client_side_effect(service_name):
        return lambda_mock if service_name == "lambda" else s3_mock

    mock_boto_client.side_effect = client_side_effect

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

@patch("proyecto1.boto3.client")
def test_lambda2_non_html_file(mock_boto_client):
    from proyecto1 import app

    s3_mock = MagicMock()
    mock_boto_client.return_value = s3_mock

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

@patch("proyecto1.boto3.client")
def test_lambda2_html_without_fecha(mock_boto_client):
    from proyecto1 import app

    fake_html = "<html><body></body></html>"
    s3_mock = MagicMock()
    s3_mock.download_file.side_effect = lambda b, k, f: open(f, 'w', encoding='utf-8').write(fake_html)
    mock_boto_client.return_value = s3_mock

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

@patch("proyecto1.boto3.client")
def test_invoke_third_lambda(mock_boto_client):
    from proyecto1 import app

    html = "<a href='/deportes/futbol/partido'>Noticia</a>"
    s3_mock = MagicMock()
    lambda_mock = MagicMock()

    s3_mock.download_file.side_effect = lambda b, k, f: open(f, 'w', encoding='utf-8').write(html)
    s3_mock.upload_file.return_value = None
    lambda_mock.invoke.return_value = {"StatusCode": 202}

    def client_side_effect(service_name):
        return lambda_mock if service_name == "lambda" else s3_mock

    mock_boto_client.side_effect = client_side_effect

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
    lambda_mock.invoke.assert_called_once()

# ----------------------------
# FUNCIONES AUXILIARES
# ----------------------------

def test_parse_eltiempo_extracts_data(html_eltiempo):
    from proyecto1 import parse_el_tiempo

    data = parse_el_tiempo(html_eltiempo)
    assert isinstance(data, list)
    assert len(data) > 0
    assert all("titulo" in n and "categoria" in n and "enlace" in n for n in data)

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
# PRUEBAS FUNCIONES DE EXTRACCIÓN
# ----------------------------

