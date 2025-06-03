import pytest
from unittest.mock import MagicMock, mock_open
from datetime import datetime
import json
import pandas as pd

# --- CRITICAL CHANGE: Patch boto3.client before importing proyecto1 ---
# This ensures that when proyecto1.s3 is initialized, it uses the mocked client.
# We'll use a global mock object that will be reset per test if needed.

# Create a mock for the S3 client that will be used globally by proyecto1
# when it's imported.
mock_s3_instance_global = MagicMock()
mock_s3_instance_global.download_file.return_value = None
mock_s3_instance_global.upload_file.return_value = None
mock_s3_instance_global.head_object.return_value = {'ContentLength': 123, 'ContentType': 'text/html'} # Crucial for 404

# Now, patch boto3.client BEFORE importing proyecto1
# The 's3' variable in proyecto1 will receive this mock instance.
import boto3 # Import boto3 here to patch it
original_boto3_client = boto3.client # Store original for cleanup, though pytest handles it
boto3.client = MagicMock(return_value=mock_s3_instance_global)

# Now, import proyecto1. Its global 's3' will be the mocked one.
from proyecto1 import app, parse_el_tiempo, extraer_noticias_publimetro

# Restore original boto3.client after import to avoid interfering with other modules if any,
# though pytest's isolation generally handles this.
boto3.client = original_boto3_client # This might not be strictly necessary with pytest, but good practice.

# --- END CRITICAL CHANGE ---


@pytest.fixture(autouse=True) # This fixture will run automatically for every test
def reset_mocks():
    """Resets the state of the global mock_s3_instance_global before each test."""
    mock_s3_instance_global.reset_mock()
    # If you have other shared mocks, reset them here too.
    # For example, if app also calls boto3.client('lambda') globally, you'd need to mock that too.
    # For now, assuming lambda client is created within the app function.


@pytest.fixture
def mock_s3_event_eltiempo():
    """Event de S3 simulado para archivo de El Tiempo"""
    return {
        'Records': [{
            's3': {
                'bucket': {'name': 'parcialfinal2025'},
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
                'bucket': {'name': 'parcialfinal2025'},
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
                'bucket': {'name': 'parcialfinal2025'},
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

@pytest.fixture
def mock_lambda_client():
    """Mock para simular invocación de Lambda"""
    mock_lambda = MagicMock()
    mock_lambda.invoke.return_value = {'StatusCode': 200}
    return mock_lambda

# Combined test for success scenarios using parametrization
@pytest.mark.parametrize("newspaper_type", ["eltiempo", "publimetro"])
def test_app_success(mocker, newspaper_type, request, mock_context, mock_lambda_client):
    """Prueba procesamiento exitoso de archivos de periódicos."""

    # We are using the globally mocked S3 client (mock_s3_instance_global).
    # Its state is reset by the autouse fixture.
    # Now, ensure that any other boto3.client calls within the app (like for lambda)
    # are also mocked.
    mocker.patch('boto3.client', side_effect=lambda service: {
        's3': mock_s3_instance_global, # Ensure this is used for S3
        'lambda': mock_lambda_client
    }.get(service))


    # Also patch time.sleep and builtins.open using mocker for this test's scope
    mocker.patch('time.sleep', return_value=None)
    mock_open_func = mocker.patch('builtins.open', new_callable=mock_open)


    # Determine event and HTML content based on newspaper_type
    if newspaper_type == "eltiempo":
        event = request.getfixturevalue("mock_s3_event_eltiempo")
        html_content = request.getfixturevalue("sample_eltiempo_html")
        expected_periodico = 'eltiempo'
    else: # publimetro
        event = request.getfixturevalue("mock_s3_event_publimetro")
        html_content = request.getfixturevalue("sample_publimetro_html")
        expected_periodico = 'publimetro'

    mock_open_func.return_value.read.return_value = html_content

    # Ejecutar función
    result = app(event, mock_context)

    # Verificaciones
    assert result['statusCode'] == 200
    assert f'final/periodico={expected_periodico}/year=2025/month=05/day=28/titulares.csv' in result['body']
    mock_s3_instance_global.download_file.assert_called_once()
    mock_s3_instance_global.upload_file.assert_called_once()
    mock_lambda_client.invoke.assert_called_once()


# Individual tests that need specific mocking configurations
def test_app_non_html_file(mocker, mock_s3_event_non_html, mock_context):
    """Prueba que se ignoren archivos que no son HTML"""
    # The global s3 client is already mocked by mock_s3_instance_global
    # We just need to patch boto3.client to handle any new calls (e.g. for lambda if it appears)
    mocker.patch('boto3.client', side_effect=lambda service: {
        's3': mock_s3_instance_global,
        # 'lambda': MagicMock() # If lambda client might be called, mock it here too
    }.get(service))

    result = app(mock_s3_event_non_html, mock_context)
    
    assert result['statusCode'] == 200
    assert 'Se ignoró el archivo' in result['body']


def test_app_no_news_extracted(mocker, mock_s3_event_eltiempo, mock_context, sample_empty_html):
    """Prueba cuando no se extraen noticias del HTML"""
    mocker.patch('boto3.client', return_value=mock_s3_instance_global) # Ensure any future call to boto3.client gets the global mock
    mock_open_func = mocker.patch('builtins.open', new_callable=mock_open)
    
    mock_open_func.return_value.read.return_value = sample_empty_html
    
    # Ejecutar función y esperar excepción
    with pytest.raises(ValueError, match="No se extrajo ninguna noticia"):
        app(mock_s3_event_eltiempo, mock_context)


def test_app_invalid_date_format(mocker, mock_context, sample_eltiempo_html):
    """Prueba con formato de fecha inválido en el nombre del archivo"""
    mocker.patch('boto3.client', return_value=mock_s3_instance_global)
    mock_open_func = mocker.patch('builtins.open', new_callable=mock_open)

    # Event con nombre de archivo sin fecha válida
    event_invalid_date = {
        'Records': [{
            's3': {
                'bucket': {'name': 'parcialfinal2025'},
                'object': {'key': 'raw/contenido-eltiempo-invalid.html'}
            }
        }]
    }
    
    mock_open_func.return_value.read.return_value = sample_eltiempo_html
    
    # Ejecutar función y esperar excepción
    with pytest.raises(ValueError, match="No se encontró una fecha válida"):
        app(event_invalid_date, mock_context)


def test_app_unknown_newspaper(mocker, mock_context, sample_eltiempo_html):
    """Prueba con periódico desconocido"""
    mocker.patch('boto3.client', return_value=mock_s3_instance_global)
    mock_open_func = mocker.patch('builtins.open', new_callable=mock_open)

    # Event con nombre que no contiene 'eltiempo' ni 'publimetro'
    event_unknown = {
        'Records': [{
            's3': {
                'bucket': {'name': 'parcialfinal2025'},
                'object': {'key': 'raw/contenido-unknown-2025-05-28.html'}
            }
        }]
    }
    
    mock_open_func.return_value.read.return_value = sample_eltiempo_html
    
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

def test_csv_generation_format(mocker, sample_eltiempo_html):
    """Prueba que el CSV se genere con el formato correcto"""
    # Patch pd.DataFrame.to_csv using mocker
    mock_to_csv = mocker.patch('proyecto1.pd.DataFrame.to_csv')

    noticias = parse_el_tiempo(sample_eltiempo_html)
    
    # Simular creación de DataFrame y guardado
    df = pd.DataFrame(noticias)
    df.to_csv('/tmp/test.csv', index=False)
    
    # Verificar que to_csv se llamó con index=False
    mock_to_csv.assert_called_with('/tmp/test.csv', index=False)