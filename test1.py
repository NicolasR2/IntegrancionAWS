import pytest
import boto3
import pandas as pd
from unittest.mock import patch, MagicMock, mock_open
from datetime import datetime
import json
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
            {
                "@type": "NewsArticle",
                "headline": "Noticia desde JSON-LD",
                "mainEntityOfPage": {
                    "@id": "https://www.eltiempo.com/tecnologia/ciencia/descubrimiento-cientifico"
                }
            }
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
def mock_s3_client():
    """Mock para simular interacciones con S3"""
    mock_s3 = MagicMock()
    mock_s3.download_file.return_value = None
    mock_s3.upload_file.return_value = None
    mock_s3.head_object.return_value = {'ContentLength': 123, 'ContentType': 'text/html'}
    return mock_s3

@pytest.fixture
def mock_lambda_client():
    """Mock para simular invocación de Lambda"""
    mock_lambda = MagicMock()
    mock_lambda.invoke.return_value = {'StatusCode': 200}
    return mock_lambda

@patch('proyecto1.boto3.client')
@patch('proyecto1.time.sleep')
@patch('builtins.open', new_callable=mock_open)
def test_app_eltiempo_success(mock_file_open, mock_sleep, mock_boto3_client, 
                              mock_s3_event_eltiempo, mock_context, 
                              sample_eltiempo_html, mock_s3_client, mock_lambda_client):
    """Prueba procesamiento exitoso de archivo de El Tiempo"""
    # Configurar mocks
    def client_side_effect(service):
        if service == 's3':
            return mock_s3_client
        elif service == 'lambda':
            return mock_lambda_client
    
    mock_boto3_client.side_effect = client_side_effect
    mock_file_open.return_value.read.return_value = sample_eltiempo_html
    
    # Ejecutar función
    result = app(mock_s3_event_eltiempo, mock_context)
    
    # Verificaciones
    assert result['statusCode'] == 200
    assert 'final/periodico=eltiempo/year=2025/month=05/day=28/titulares.csv' in result['body']
    mock_s3_client.download_file.assert_called_once()
    mock_s3_client.upload_file.assert_called_once()
    mock_lambda_client.invoke.assert_called_once()

@patch('proyecto1.boto3.client')
@patch('proyecto1.time.sleep')
@patch('builtins.open', new_callable=mock_open)
def test_app_publimetro_success(mock_file_open, mock_sleep, mock_boto3_client,
                               mock_s3_event_publimetro, mock_context,
                               sample_publimetro_html, mock_s3_client, mock_lambda_client):
    """Prueba procesamiento exitoso de archivo de Publimetro"""
    # Configurar mocks
    def client_side_effect(service):
        if service == 's3':
            return mock_s3_client
        elif service == 'lambda':
            return mock_lambda_client
    
    mock_boto3_client.side_effect = client_side_effect
    mock_file_open.return_value.read.return_value = sample_publimetro_html
    
    # Ejecutar función
    result = app(mock_s3_event_publimetro, mock_context)
    
    # Verificaciones
    assert result['statusCode'] == 200
    assert 'final/periodico=publimetro/year=2025/month=05/day=28/titulares.csv' in result['body']
    mock_s3_client.download_file.assert_called_once()
    mock_s3_client.upload_file.assert_called_once()
    mock_lambda_client.invoke.assert_called_once()

def test_app_non_html_file(mock_s3_event_non_html, mock_context):
    """Prueba que se ignoren archivos que no son HTML"""
    result = app(mock_s3_event_non_html, mock_context)
    
    assert result['statusCode'] == 200
    assert 'Se ignoró el archivo' in result['body']

@patch('proyecto1.boto3.client')
@patch('builtins.open', new_callable=mock_open)
def test_app_no_news_extracted(mock_file_open, mock_boto3_client,
                              mock_s3_event_eltiempo, mock_context,
                              sample_empty_html, mock_s3_client):
    """Prueba cuando no se extraen noticias del HTML"""
    mock_boto3_client.return_value = mock_s3_client
    mock_file_open.return_value.read.return_value = sample_empty_html
    
    # Ejecutar función y esperar excepción
    with pytest.raises(ValueError, match="No se extrajo ninguna noticia"):
        app(mock_s3_event_eltiempo, mock_context)

@patch('proyecto1.boto3.client')
@patch('builtins.open', new_callable=mock_open)
def test_app_invalid_date_format(mock_file_open, mock_boto3_client, mock_context,
                                sample_eltiempo_html, mock_s3_client):
    """Prueba con formato de fecha inválido en el nombre del archivo"""
    # Event con nombre de archivo sin fecha válida
    event_invalid_date = {
        'Records': [{
            's3': {
                'bucket': {'name': 'pacialcorte3-2025'},
                'object': {'key': 'raw/contenido-eltiempo-invalid.html'}
            }
        }]
    }
    
    mock_boto3_client.return_value = mock_s3_client
    mock_file_open.return_value.read.return_value = sample_eltiempo_html
    
    # Ejecutar función y esperar excepción
    with pytest.raises(ValueError, match="No se encontró una fecha válida"):
        app(event_invalid_date, mock_context)

@patch('proyecto1.boto3.client')
@patch('builtins.open', new_callable=mock_open)
def test_app_unknown_newspaper(mock_file_open, mock_boto3_client, mock_context,
                              sample_eltiempo_html, mock_s3_client):
    """Prueba con periódico desconocido"""
    # Event con nombre que no contiene 'eltiempo' ni 'publimetro'
    event_unknown = {
        'Records': [{
            's3': {
                'bucket': {'name': 'pacialcorte3-2025'},
                'object': {'key': 'raw/contenido-unknown-2025-05-28.html'}
            }
        }]
    }
    
    mock_boto3_client.return_value = mock_s3_client
    mock_file_open.return_value.read.return_value = sample_eltiempo_html
    
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

@patch('proyecto1.pd.DataFrame.to_csv')
def test_csv_generation_format(mock_to_csv, sample_eltiempo_html):
    """Prueba que el CSV se genere con el formato correcto"""
    noticias = parse_el_tiempo(sample_eltiempo_html)
    
    # Simular creación de DataFrame y guardado
    df = pd.DataFrame(noticias)
    df.to_csv('/tmp/test.csv', index=False)
    
    # Verificar que to_csv se llamó con index=False
    mock_to_csv.assert_called_with('/tmp/test.csv', index=False)