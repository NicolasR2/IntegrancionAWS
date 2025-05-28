import pytest
import pandas as pd
from io import BytesIO
from unittest.mock import patch, MagicMock
from datetime import datetime
import json

# Imports para los tres proyectos
from proyecto import app as app_proyecto, parse_el_tiempo, extraer_noticias_publimetro
from proyecto1 import app as app_proyecto1
from proyecto2 import lambda_handler


# ========================
# FIXTURES - DATOS DE PRUEBA
# ========================

@pytest.fixture
def mock_s3_key_eltiempo():
    return "raw/eltiempo-2024-01-15.html"

@pytest.fixture
def mock_s3_key_publimetro():
    return "raw/publimetro-2024-01-15.html"

@pytest.fixture
def mock_s3_key_invalid():
    return "raw/periodicodesconocido-2024-01-15.html"

@pytest.fixture
def mock_s3_key_no_date():
    return "raw/eltiempo-sin-fecha.html"

@pytest.fixture
def mock_lambda_event_eltiempo(mock_s3_key_eltiempo):
    return {
        'Records': [{
            's3': {
                'bucket': {'name': 'test-bucket'},
                'object': {'key': mock_s3_key_eltiempo}
            }
        }]
    }

@pytest.fixture
def mock_lambda_event_publimetro(mock_s3_key_publimetro):
    return {
        'Records': [{
            's3': {
                'bucket': {'name': 'test-bucket'},
                'object': {'key': mock_s3_key_publimetro}
            }
        }]
    }

@pytest.fixture
def mock_lambda_event_invalid(mock_s3_key_invalid):
    return {
        'Records': [{
            's3': {
                'bucket': {'name': 'test-bucket'},
                'object': {'key': mock_s3_key_invalid}
            }
        }]
    }

@pytest.fixture
def sample_html_eltiempo():
    """HTML simulado de El Tiempo con noticias"""
    return """
    <html>
        <head>
            <script type="application/ld+json">
            [{
                "@type": "NewsArticle",
                "headline": "Noticia importante de política",
                "mainEntityOfPage": {"@id": "https://www.eltiempo.com/politica/noticia-importante-123"}
            }]
            </script>
        </head>
        <body>
            <article>
                <a href="/deportes/futbol/barcelona-vs-real-madrid">Barcelona vence al Real Madrid</a>
            </article>
            <div class="noticia">
                <a href="/economia/inflacion-enero-2024">La inflación baja en enero</a>
            </div>
            <a href="/cultura/festival-musica-bogota">Festival de música en Bogotá</a>
        </body>
    </html>
    """

@pytest.fixture
def sample_html_publimetro():
    """HTML simulado de Publimetro con noticias"""
    return """
    <html>
        <body>
            <article class="b-top-table-list-xl">
                <h2 class="c-heading">
                    <a class="c-link" href="/deportes/futbol-nacional">Millonarios clasificó a semifinales</a>
                </h2>
                <span class="c-overline">Deportes</span>
            </article>
            
            <div class="b-card-list">
                <article class="b-card-list__main-item">
                    <h3 class="c-heading">
                        <a class="c-link" href="/entretenimiento/cine-colombiano">Nueva película colombiana</a>
                    </h3>
                </article>
                <article class="b-card-list__secondary-item">
                    <h3 class="c-heading">
                        <a class="c-link" href="/noticias/politica-nacional">Reforma tributaria aprobada</a>
                    </h3>
                </article>
            </div>
            
            <article class="b-top-table-list-small">
                <h2 class="c-heading">
                    <a class="c-link" href="/barranquilla/carnaval-2024">Carnaval de Barranquilla 2024</a>
                </h2>
            </article>
        </body>
    </html>
    """

@pytest.fixture
def sample_html_empty():
    """HTML vacío sin noticias"""
    return "<html><body><h1>Sin noticias</h1></body></html>"

@pytest.fixture
def mock_csv_event():
    """Evento simulado para proyecto2 (Lambda que ejecuta Glue)"""
    return {
        'Records': [{
            's3': {
                'object': {'key': 'final/periodico=eltiempo/year=2024/month=01/day=15/titulares.csv'}
            }
        }]
    }


# ========================
# PRUEBAS PARA PROYECTO.PY
# ========================

@patch('proyecto.s3')
@patch('proyecto.boto3.client')  # Mock para el cliente Lambda
@patch('proyecto.time.sleep')  # Mock para evitar el sleep de 20 segundos
@patch('builtins.open')
def test_app_eltiempo_success(mock_open, mock_sleep, mock_boto3_client, mock_s3, 
                             mock_lambda_event_eltiempo, sample_html_eltiempo):
    """Prueba procesamiento exitoso de El Tiempo"""
    # Configurar mocks
    mock_s3.download_file.return_value = None
    mock_s3.upload_file.return_value = None
    mock_open.return_value.__enter__.return_value.read.return_value = sample_html_eltiempo
    
    # Mock del cliente Lambda
    mock_lambda_client = MagicMock()
    mock_boto3_client.return_value = mock_lambda_client
    mock_lambda_client.invoke.return_value = {}
    
    result = app_proyecto(mock_lambda_event_eltiempo, {})
    
    assert result['statusCode'] == 200
    assert 'final/periodico=eltiempo/year=2024/month=01/day=15/titulares.csv' in result['body']
    mock_s3.download_file.assert_called_once()
    mock_s3.upload_file.assert_called_once()
    mock_lambda_client.invoke.assert_called_once()

@patch('proyecto.s3')
@patch('proyecto.boto3.client')
@patch('proyecto.time.sleep')
@patch('builtins.open')
def test_app_publimetro_success(mock_open, mock_sleep, mock_boto3_client, mock_s3,
                               mock_lambda_event_publimetro, sample_html_publimetro):
    """Prueba procesamiento exitoso de Publimetro"""
    mock_s3.download_file.return_value = None
    mock_s3.upload_file.return_value = None
    mock_open.return_value.__enter__.return_value.read.return_value = sample_html_publimetro
    
    mock_lambda_client = MagicMock()
    mock_boto3_client.return_value = mock_lambda_client
    mock_lambda_client.invoke.return_value = {}
    
    result = app_proyecto(mock_lambda_event_publimetro, {})
    
    assert result['statusCode'] == 200
    assert 'final/periodico=publimetro/year=2024/month=01/day=15/titulares.csv' in result['body']

@patch('proyecto.s3')
def test_app_invalid_newspaper(mock_s3, mock_lambda_event_invalid):
    """Prueba error cuando no se reconoce el periódico"""
    mock_s3.download_file.return_value = None
    
    with pytest.raises(ValueError, match="No se pudo determinar el periódico del archivo"):
        app_proyecto(mock_lambda_event_invalid, {})

@patch('proyecto.s3')
@patch('builtins.open')
def test_app_no_news_extracted(mock_open, mock_s3, mock_lambda_event_eltiempo, sample_html_empty):
    """Prueba error cuando no se extraen noticias"""
    mock_s3.download_file.return_value = None
    mock_open.return_value.__enter__.return_value.read.return_value = sample_html_empty
    
    with pytest.raises(ValueError, match="No se extrajo ninguna noticia"):
        app_proyecto(mock_lambda_event_eltiempo, {})

def test_parse_el_tiempo_multiple_news(sample_html_eltiempo):
    """Prueba extracción de múltiples noticias de El Tiempo"""
    noticias = parse_el_tiempo(sample_html_eltiempo)
    
    assert len(noticias) > 0
    assert any('Barcelona' in noticia['titulo'] for noticia in noticias)
    assert any('política' in noticia['titulo'] for noticia in noticias)
    
    # Verificar estructura de los datos
    for noticia in noticias:
        assert 'categoria' in noticia
        assert 'titulo' in noticia
        assert 'enlace' in noticia
        assert noticia['enlace'].startswith('https://www.eltiempo.com')

def test_parse_el_tiempo_empty_html():
    """Prueba cuando El Tiempo no tiene noticias"""
    noticias = parse_el_tiempo("<html><body></body></html>")
    assert len(noticias) == 0

def test_extraer_noticias_publimetro_multiple_sections(sample_html_publimetro):
    """Prueba extracción de noticias de diferentes secciones de Publimetro"""
    noticias = extraer_noticias_publimetro(sample_html_publimetro)
    
    assert len(noticias) > 0
    assert any('Millonarios' in noticia['titular'] for noticia in noticias)
    assert any('Carnaval' in noticia['titular'] for noticia in noticias)
    
    # Verificar estructura de los datos
    for noticia in noticias:
        assert 'categoria' in noticia
        assert 'titular' in noticia
        assert 'link' in noticia
        assert 'https://www.publimetro.co' in noticia['link']

def test_extraer_noticias_publimetro_empty_html():
    """Prueba cuando Publimetro no tiene noticias"""
    noticias = extraer_noticias_publimetro("<html><body></body></html>")
    assert len(noticias) == 0


# ========================
# PRUEBAS PARA PROYECTO1.PY (son idénticas a proyecto.py)
# ========================

@patch('proyecto1.s3')
@patch('proyecto1.boto3.client')
@patch('proyecto1.time.sleep')
@patch('builtins.open')
def test_app_proyecto1_eltiempo_success(mock_open, mock_sleep, mock_boto3_client, mock_s3,
                                       mock_lambda_event_eltiempo, sample_html_eltiempo):
    """Prueba procesamiento exitoso de El Tiempo en proyecto1"""
    mock_s3.download_file.return_value = None
    mock_s3.upload_file.return_value = None
    mock_open.return_value.__enter__.return_value.read.return_value = sample_html_eltiempo
    
    mock_lambda_client = MagicMock()
    mock_boto3_client.return_value = mock_lambda_client
    mock_lambda_client.invoke.return_value = {}
    
    result = app_proyecto1(mock_lambda_event_eltiempo, {})
    
    assert result['statusCode'] == 200
    assert 'final/periodico=eltiempo/year=2024/month=01/day=15/titulares.csv' in result['body']


# ========================
# PRUEBAS PARA PROYECTO2.PY
# ========================

@patch('proyecto2.boto3.client')
def test_lambda_handler_start_crawler_success(mock_boto3_client, mock_csv_event):
    """Prueba inicio exitoso del crawler de Glue"""
    mock_glue_client = MagicMock()
    mock_boto3_client.return_value = mock_glue_client
    mock_glue_client.start_crawler.return_value = {}
    
    result = lambda_handler(mock_csv_event, {})
    
    assert result['statusCode'] == 200
    assert result['body'] == 'Evento procesado.'
    mock_glue_client.start_crawler.assert_called_once_with(Name='noticias-crawler')

@patch('proyecto2.boto3.client')
def test_lambda_handler_crawler_already_running(mock_boto3_client, mock_csv_event):
    """Prueba cuando el crawler ya está ejecutándose"""
    mock_glue_client = MagicMock()
    mock_boto3_client.return_value = mock_glue_client
    
    # Simular excepción de crawler ya ejecutándose
    from botocore.exceptions import ClientError
    mock_glue_client.start_crawler.side_effect = ClientError(
        {'Error': {'Code': 'CrawlerRunningException'}}, 'StartCrawler'
    )
    
    result = lambda_handler(mock_csv_event, {})
    
    assert result['statusCode'] == 200
    assert result['body'] == 'Evento procesado.'

@patch('proyecto2.boto3.client')
def test_lambda_handler_ignore_non_csv_files(mock_boto3_client):
    """Prueba que ignora archivos que no son CSV"""
    event_non_csv = {
        'Records': [{
            's3': {
                'object': {'key': 'raw/eltiempo-2024-01-15.html'}
            }
        }]
    }
    
    mock_glue_client = MagicMock()
    mock_boto3_client.return_value = mock_glue_client
    
    result = lambda_handler(event_non_csv, {})
    
    assert result['statusCode'] == 200
    # No debe llamar al crawler para archivos no CSV
    mock_glue_client.start_crawler.assert_not_called()

@patch('proyecto2.boto3.client')
def test_lambda_handler_ignore_non_final_csv(mock_boto3_client):
    """Prueba que ignora CSV que no están en la carpeta 'final/'"""
    event_non_final = {
        'Records': [{
            's3': {
                'object': {'key': 'raw/data.csv'}
            }
        }]
    }
    
    mock_glue_client = MagicMock()
    mock_boto3_client.return_value = mock_glue_client
    
    result = lambda_handler(event_non_final, {})
    
    assert result['statusCode'] == 200
    # No debe llamar al crawler para CSV fuera de 'final/'
    mock_glue_client.start_crawler.assert_not_called()


# ========================
# PRUEBAS DE CASOS EDGE
# ========================

def test_app_non_html_file():
    """Prueba que ignora archivos que no son HTML"""
    event_txt = {
        'Records': [{
            's3': {
                'bucket': {'name': 'test-bucket'},
                'object': {'key': 'raw/document.txt'}
            }
        }]
    }
    
    result = app_proyecto(event_txt, {})
    
    assert result['statusCode'] == 200
    assert 'Se ignoró el archivo' in result['body']

@patch('proyecto.s3')
def test_app_invalid_date_format(mock_s3):
    """Prueba error cuando el nombre del archivo no tiene fecha válida"""
    event_no_date = {
        'Records': [{
            's3': {
                'bucket': {'name': 'test-bucket'},
                'object': {'key': 'raw/eltiempo-sin-fecha.html'}
            }
        }]
    }
    
    mock_s3.download_file.return_value = None
    
    with pytest.raises(ValueError, match="No se encontró una fecha válida en el nombre del archivo"):
        app_proyecto(event_no_date, {})