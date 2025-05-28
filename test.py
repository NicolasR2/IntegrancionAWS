import pytest
import os
import boto3
from io import BytesIO
from unittest.mock import patch, MagicMock
from proyecto1 import app, parse_el_tiempo, extraer_noticias_publimetro

# --- Fixtures HTML de ejemplo ---

@pytest.fixture
def html_eltiempo_minimo():
    """HTML de El Tiempo con dos enlaces válidos."""
    return '''
    <html><body>
      <a href="/deportes/futbol/2025-05-28/juego">Partido emocionante</a>
      <article class="headline"><a href="/mundo/politica/2025-05-28/noticia">Noticia de política nacional</a></article>
      <script type="application/ld+json">
      [{"@type":"NewsArticle","headline":"Titular json-ld ejemplo","mainEntityOfPage":{"@id":"https://www.eltiempo.com/cultura/2025-05-28/evento"}}]
      </script>
    </body></html>
    '''

@pytest.fixture
def html_eltiempo_vacio():
    """HTML sin noticias válidas."""
    return b'<html><body><p>Sin enlaces de noticia</p></body></html>'

@pytest.fixture
def html_publimetro_minimo():
    """HTML de Publimetro con noticias principales y secundarias."""
    return '''
    <html><body>
      <article class="b-top-table-list-xl">
        <h2 class="c-heading"><a class="c-link" href="/noticias/2025-05-28/titular1">Titular Publimetro 1</a></h2>
        <span class="c-overline">Noticias</span>
      </article>
      <div class="b-card-list">
        <article class="b-card-list__main-item">
          <h3 class="c-heading"><a class="c-link" href="/entretenimiento/2025-05-28/entretenimiento">Para entretenerse</a></h3>
        </article>
      </div>
    </body></html>
    '''

# --- Mocks de S3 y Lambda client ---

@pytest.fixture
def mock_s3_client(tmp_path, html_eltiempo_minimo):
    """Mock de boto3 S3 con descarga de El Tiempo."""
    mock = MagicMock()
    # Cuando download_file, escribimos html_eltiempo_minimo en local
    def fake_download(bucket, key, local_path):
        with open(local_path, 'wb') as f:
            f.write(html_eltiempo_minimo)
    mock.download_file.side_effect = fake_download
    mock.upload_file.return_value = None
    return mock

@pytest.fixture
def event_eltiempo(tmp_path):
    """Evento S3 simulando subida de archivo El Tiempo."""
    return {
        'Records': [{
            's3': {
                'bucket': {'name': 'mi-bucket'},
                'object': {'key': 'noticias/eltiempo_2025-05-28.html'}
            }
        }]
    }

# --- Tests de flujo principal ---

@patch.object(boto3, 'client')
def test_app_eltiempo_ok(mock_boto_client, mock_s3_client, event_eltiempo, tmp_path, html_eltiempo_minimo):
    # boto3.client('s3') devuelve nuestro mock_s3_client
    mock_boto_client.side_effect = lambda service: mock_s3_client if service=='s3' else MagicMock()
    resp = app(event_eltiempo, None)
    assert resp['statusCode'] == 200
    assert 'final/periodico=eltiempo/year=2025' in resp['body']

def test_parse_el_tiempo_extrae_noticias(html_eltiempo_minimo):
    noticias = parse_el_tiempo(html_eltiempo_minimo.decode('utf-8'))
    assert isinstance(noticias, list)
    assert any(n['titulo']=='Partido emocionante' for n in noticias)
    assert any('Titular json-ld ejemplo' in n['titulo'] for n in noticias)

@patch.object(boto3, 'client')
def test_app_eltiempo_sin_noticias(mock_boto_client, mock_s3_client, event_eltiempo, tmp_path, html_eltiempo_vacio):
    # Forzamos descarga de HTML vacío
    def fake_download(bucket, key, local_path):
        with open(local_path, 'wb') as f:
            f.write(html_eltiempo_vacio)
    mock_s3 = mock_s3_client
    mock_s3.download_file.side_effect = fake_download
    mock_boto_client.side_effect = lambda service: mock_s3 if service=='s3' else MagicMock()
    with pytest.raises(ValueError) as e:
        app(event_eltiempo, None)
    assert "No se extrajo ninguna noticia" in str(e.value)

@patch.object(boto3, 'client')
def test_app_key_no_html(mock_boto_client):
    event = {
        'Records':[{'s3':{'bucket':{'name':'b'},'object':{'key':'archivo.txt'}}}]
    }
    mock_s3 = MagicMock()
    mock_boto_client.return_value = mock_s3
    resp = app(event, None)
    assert resp['statusCode']==200
    assert "Se ignoró el archivo" in resp['body']

# --- Tests para Publimetro ---

def test_extraer_noticias_publimetro(html_publimetro_minimo):
    noticias = extraer_noticias_publimetro(html_publimetro_minimo.decode('utf-8'))
    titulos = [n['titular'] for n in noticias]
    assert "Titular Publimetro 1" in titulos
    assert "Para entretenerse" in titulos
