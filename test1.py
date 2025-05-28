import pytest
from unittest.mock import MagicMock, mock_open
from datetime import datetime
import json
import pandas as pd

# Remove the global patch here
# import boto3 # No longer needed directly for global client if using mocker

# Import proyecto1 after all global mocks are set up by mocker
from proyecto1 import app, parse_el_tiempo, extraer_noticias_publimetro


@pytest.fixture
def mock_s3_event_eltiempo():
    """Event de S3 simulado para archivo de El Tiempo"""
    return {
        'Records': [{
            's3': {
                'bucket': {'name': 'pacialcorte3-2025'},
                'object': {'key': 'raw/contenido-eltiempo-2025-05-28-10-30.html'}
            }
        }]
    }

@pytest.fixture
def mock_s3_event_publimetro():
    """Event de S3 simulado para archivo de Publimetro"""
    return {
        'Records': [{
            's3': {
                'bucket': {'name': 'pacialcorte3-2025'},
                'object': {'key': 'raw/contenido-publimetro-2025-05-28-15-45.html'}
            }
        }]
    }

@pytest.fixture
def mock_s3_event_non_html():
    """Event de S3 con archivo que no es HTML"""
    return {
        'Records': [{
            's3': {
                'bucket': {'name': 'pacialcorte3-2025'},
                'object': {'key': 'raw/archivo.txt'}
            }
        }]
    }

@pytest.fixture
def mock_context():
    """Context básico para Lambda"""
    return {}

@pytest.fixture
def sample_eltiempo_html():
    """HTML simulado de El Tiempo con noticias"""
    return """
    <html>
        <head><title>El Tiempo</title></head>
        <body>
            <article>
                <a href="/politica/congreso/nueva-ley-aprobada">Nueva ley aprobada en el congreso</a>
            </article>
            <article>
                <a href="/deportes/futbol/colombia-gana-partido">Colombia gana importante partido</a>
            </article>
            <div class="noticia">
                <a href="/economia/inflacion/datos-economia">Nuevos datos sobre economía nacional</a>
            </div>
            <script type="application/ld+json">
            [
                {
                    "@type": "NewsArticle",
                    "headline": "Noticia desde JSON-LD",
                    "mainEntityOfPage": {
                        "@id": "https://www.eltiempo.com/tecnologia/ciencia/descubrimiento-cientifico"
                    }
                }
            ]
            </script>
        </body>
    </html>
    """

@pytest.fixture
def sample_publimetro_html():
    """HTML simulado de Publimetro con noticias"""
    return """
    <html>
        <head><title>Publimetro</title></head>
        <body>
            <article class="b-top-table-list-xl">
                <span class="c-overline">Deportes</span>
                <h2 class="c-heading">
                    <a class="c-link" href="/deportes/futbol-local">Fútbol local en auge</a>
                </h2>
            </article>
            <div class="b-card-list">
                <article class="b-card-list__main-item">
                    <span class="c-overline">Entretenimiento</span>
                    <h3 class="c-heading">
                        <a class="c-link" href="/entretenimiento/musica">Nueva música colombiana</a>
                    </h3>
                </article>
                <article class="b-card-list__secondary-item">
                    <h3 class="c-heading">
                        <a class="c-link" href="/noticias/cultura">Eventos culturales</a>
                    </h3>
                </article>
            </div>
            <article class="b-top-table-list-small">
                <h2 class="c-heading">
                    <a class="c-link" href="/barranquilla/noticias-locales">Noticias de Barranquilla</a>
                </h2>
            </article>
        </body>
    </html>
    """

@pytest.fixture
def sample_empty_html():
    """HTML sin noticias válidas"""
    return """
    <html>
        <head><title>Página vacía</title></head>
        <body>
            <div>Contenido sin noticias</div>
        </body>
    </html>
    """

# We'll use the 'mocker' fixture from pytest-mock to create the client mocks.
# No need for mock_s3_client_for_test_functions as a fixture.

@pytest.fixture
def mock_lambda_client():
    """Mock para simular invocación de Lambda"""
    mock_lambda = MagicMock()
    mock_lambda.invoke.return_value = {'StatusCode': 200}
    return mock_lambda

# Use mocker fixture provided by pytest-mock
@pytest.mark.parametrize("newspaper_type", ["eltiempo", "publimetro"])
@pytest.mark.patch("proyecto1.time.sleep", return_value=None) # No need to import time for patching
@pytest.mark.patch("builtins.open", new_callable=mock_open)
def test_app_success(mocker, mock_open, newspaper_type, request, mock_context, mock_lambda_client):
    """Prueba procesamiento exitoso de archivos de periódicos."""

    # Set up mocks for boto3.client
    mock_s3_client = mocker.MagicMock()
    mock_s3_client.download_file.return_value = None
    mock_s3_client.upload_file.return_value = None
    mock_s3_client.head_object.return_value = {'ContentLength': 123, 'ContentType': 'text/html'}

    # Patch the global 's3' variable in proyecto1
    mocker.patch.object(app.__globals__['boto3'], 'client', side_effect=lambda service: {
        's3': mock_s3_client,
        'lambda': mock_lambda_client
    }.get(service))

    # Determine event and HTML content based on newspaper_type
    if newspaper_type == "eltiempo":
        event = request.getfixturevalue("mock_s3_event_eltiempo")
        html_content = request.getfixturevalue("sample_eltiempo_html")
        expected_periodico = 'eltiempo'
    else: # publimetro
        event = request.getfixturevalue("mock_s3_event_publimetro")
        html_content = request.getfixturevalue("sample_publimetro_html")
        expected_periodico = 'publimetro'

    mock_open.return_value.read.return_value = html_content

    # Ejecutar función
    result = app(event, mock_context)

    # Verificaciones
    assert result['statusCode'] == 200
    assert f'final/periodico={expected_periodico}/year=2025/month=05/day=28/titulares.csv' in result['body']
    mock_s3_client.download_file.assert_called_once()
    mock_s3_client.upload_file.assert_called_once()
    mock_lambda_client.invoke.assert_called_once()


@pytest.mark.patch("proyecto1.boto3.client") # Patch only for this test, will be cleaned by pytest-mock
def test_app_non_html_file(mock_boto3_client, mock_s3_event_non_html, mock_context):
    """Prueba que se ignoren archivos que no son HTML"""
    # mock_boto3_client will be a MagicMock by default
    result = app(mock_s3_event_non_html, mock_context)
    
    assert result['statusCode'] == 200
    assert 'Se ignoró el archivo' in result['body']

@pytest.mark.patch("proyecto1.boto3.client")
@pytest.mark.patch("builtins.open", new_callable=mock_open)
def test_app_no_news_extracted(mock_boto3_client, mock_open,
                               mock_s3_event_eltiempo, mock_context,
                               sample_empty_html):
    """Prueba cuando no se extraen noticias del HTML"""
    # Configure the S3 client mock for this test
    mock_s3_client_instance = MagicMock()
    mock_s3_client_instance.download_file.return_value = None
    mock_s3_client_instance.head_object.return_value = {'ContentLength': 123, 'ContentType': 'text/html'}
    mock_boto3_client.return_value = mock_s3_client_instance

    mock_open.return_value.read.return_value = sample_empty_html
    
    # Ejecutar función y esperar excepción
    with pytest.raises(ValueError, match="No se extrajo ninguna noticia"):
        app(mock_s3_event_eltiempo, mock_context)

@pytest.mark.patch("proyecto1.boto3.client")
@pytest.mark.patch("builtins.open", new_callable=mock_open)
def test_app_invalid_date_format(mock_boto3_client, mock_open, mock_context,
                                 sample_eltiempo_html):
    """Prueba con formato de fecha inválido en el nombre del archivo"""
    # Configure the S3 client mock for this test
    mock_s3_client_instance = MagicMock()
    mock_s3_client_instance.download_file.return_value = None
    mock_s3_client_instance.head_object.return_value = {'ContentLength': 123, 'ContentType': 'text/html'}
    mock_boto3_client.return_value = mock_s3_client_instance

    # Event con nombre de archivo sin fecha válida
    event_invalid_date = {
        'Records': [{
            's3': {
                'bucket': {'name': 'pacialcorte3-2025'},
                'object': {'key': 'raw/contenido-eltiempo-invalid.html'}
            }
        }]
    }
    
    mock_open.return_value.read.return_value = sample_eltiempo_html
    
    # Ejecutar función y esperar excepción
    with pytest.raises(ValueError, match="No se encontró una fecha válida"):
        app(event_invalid_date, mock_context)

@pytest.mark.patch("proyecto1.boto3.client")
@pytest.mark.patch("builtins.open", new_callable=mock_open)
def test_app_unknown_newspaper(mock_boto3_client, mock_open, mock_context,
                               sample_eltiempo_html):
    """Prueba con periódico desconocido"""
    # Configure the S3 client mock for this test
    mock_s3_client_instance = MagicMock()
    mock_s3_client_instance.download_file.return_value = None
    mock_s3_client_instance.head_object.return_value = {'ContentLength': 123, 'ContentType': 'text/html'}
    mock_boto3_client.return_value = mock_s3_client_instance

    # Event con nombre que no contiene 'eltiempo' ni 'publimetro'
    event_unknown = {
        'Records': [{
            's3': {
                'bucket': {'name': 'pacialcorte3-2025'},
                'object': {'key': 'raw/contenido-unknown-2025-05-28.html'}
            }
        }]
    }
    
    mock_open.return_value.read.return_value = sample_eltiempo_html
    
    # Ejecutar función y esperar excepción
    with pytest.raises(ValueError, match="No se pudo determinar el periódico"):
        app(event_unknown, mock_context)

def test_parse_el_tiempo_multiple_news(sample_eltiempo_html):
    """Prueba extracción de múltiples noticias de El Tiempo"""
    noticias = parse_el_tiempo(sample_eltiempo_html)
    
    assert len(noticias) > 0
    
    # Verificar estructura de noticias
    for noticia in noticias:
        assert 'categoria' in noticia
        assert 'titulo' in noticia
        assert 'enlace' in noticia
        assert noticia['enlace'].startswith('https://www.eltiempo.com')

def test_parse_el_tiempo_empty_html():
    """Prueba con HTML vacío para El Tiempo"""
    noticias = parse_el_tiempo("<html><body></body></html>")
    
    assert len(noticias) == 0

def test_extraer_noticias_publimetro_multiple_sections(sample_publimetro_html):
    """Prueba extracción de noticias de diferentes secciones de Publimetro"""
    noticias = extraer_noticias_publimetro(sample_publimetro_html)
    
    assert len(noticias) > 0
    
    # Verificar estructura de noticias
    for noticia in noticias:
        assert 'categoria' in noticia
        assert 'titular' in noticia
        assert 'link' in noticia
        assert noticia['link'].startswith('https://www.publimetro.co')
    
    # Verificar que se extraigan diferentes categorías
    categorias = {noticia['categoria'] for noticia in noticias}
    assert len(categorias) > 1

def test_extraer_noticias_publimetro_empty_html():
    """Prueba con HTML vacío para Publimetro"""
    noticias = extraer_noticias_publimetro("<html><body></body></html>")
    
    assert len(noticias) == 0

def test_extraer_noticias_publimetro_no_duplicates():
    """Prueba que no se generen noticias duplicadas en Publimetro"""
    html_con_duplicados = """
    <html>
        <body>
            <article class="b-top-table-list-xl">
                <h2 class="c-heading">
                    <a class="c-link" href="/noticia-repetida">Noticia repetida</a>
                </h2>
            </article>
            <article class="b-top-table-list-small">
                <h2 class="c-heading">
                    <a class="c-link" href="/noticia-repetida">Noticia repetida</a>
                </h2>
            </article>
        </body>
    </html>
    """
    
    noticias = extraer_noticias_publimetro(html_con_duplicados)
    
    # Verificar que solo hay una noticia (sin duplicados)
    assert len(noticias) == 1
    assert noticias[0]['titular'] == 'Noticia repetida'

@pytest.mark.patch('proyecto1.pd.DataFrame.to_csv')
def test_csv_generation_format(mock_to_csv, sample_eltiempo_html):
    """Prueba que el CSV se genere con el formato correcto"""
    noticias = parse_el_tiempo(sample_eltiempo_html)
    
    # Simular creación de DataFrame y guardado
    df = pd.DataFrame(noticias)
    df.to_csv('/tmp/test.csv', index=False)
    
    # Verificar que to_csv se llamó con index=False
    mock_to_csv.assert_called_with('/tmp/test.csv', index=False)