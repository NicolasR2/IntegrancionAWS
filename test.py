import pytest
import boto3
from unittest.mock import patch, MagicMock
from datetime import datetime
from proyecto import app

@pytest.fixture
def mock_event():
    """Event básico para Lambda"""
    return {}

@pytest.fixture
def mock_context():
    """Context básico para Lambda"""
    return {}

@pytest.fixture
def sample_eltiempo_html():
    """HTML simulado de El Tiempo"""
    return """
    <html>
        <head><title>El Tiempo - Noticias de Colombia</title></head>
        <body>
            <div class="article">
                <h1>Noticia Principal del Día</h1>
                <p>Contenido de la noticia de El Tiempo...</p>
            </div>
        </body>
    </html>
    """.encode('utf-8')

@pytest.fixture
def sample_publimetro_html():
    """HTML simulado de Publimetro"""
    return """
    <html>
        <head><title>Publimetro Colombia</title></head>
        <body>
            <div class="news-item">
                <h2>Última Hora</h2>
                <p>Noticias de Publimetro Colombia...</p>
            </div>
        </body>
    </html>
    """.encode('utf-8')

@pytest.fixture
def mock_s3_client():
    """Mock para simular interacciones con S3"""
    mock_s3 = MagicMock()
    # Simular subida exitosa a S3
    mock_s3.put_object.return_value = {}
    return mock_s3

@patch('proyecto.s3')
@patch('proyecto.requests')
@patch('proyecto.datetime')
def test_app_successful_downloads(mock_datetime, mock_requests, mock_s3, 
                                  mock_event, mock_context, sample_eltiempo_html, 
                                  sample_publimetro_html, mock_s3_client):
    """Prueba descarga exitosa de ambos diarios"""
    # Mock del timestamp fijo
    mock_now = MagicMock()
    mock_now.strftime.return_value = '2025-05-28-10-30'
    mock_datetime.utcnow.return_value = mock_now
    
    # Mock de requests exitosos
    mock_response_eltiempo = MagicMock()
    mock_response_eltiempo.status_code = 200
    mock_response_eltiempo.content = sample_eltiempo_html
    
    mock_response_publimetro = MagicMock()
    mock_response_publimetro.status_code = 200
    mock_response_publimetro.content = sample_publimetro_html
    
    # Configurar respuestas según la URL
    def side_effect(url):
        if 'eltiempo' in url:
            return mock_response_eltiempo
        elif 'publimetro' in url:
            return mock_response_publimetro
    
    mock_requests.get.side_effect = side_effect
    mock_s3.put_object.side_effect = mock_s3_client.put_object
    
    # Ejecutar función
    app(mock_event, mock_context)
    
    # Verificaciones
    assert mock_requests.get.call_count == 2
    assert mock_s3.put_object.call_count == 2
    
    # Verificar llamadas específicas
    mock_requests.get.assert_any_call('https://www.eltiempo.com')
    mock_requests.get.assert_any_call('https://www.publimetro.co/')

@patch('proyecto.s3')
@patch('proyecto.requests')
@patch('proyecto.datetime')
def test_app_partial_failure(mock_datetime, mock_requests, mock_s3, 
                            mock_event, mock_context, sample_eltiempo_html):
    """Prueba cuando un diario falla y otro es exitoso"""
    # Mock del timestamp
    mock_now = MagicMock()
    mock_now.strftime.return_value = '2025-05-28-10-30'
    mock_datetime.utcnow.return_value = mock_now
    
    # Mock de requests - uno exitoso, uno fallido
    mock_response_success = MagicMock()
    mock_response_success.status_code = 200
    mock_response_success.content = sample_eltiempo_html
    
    mock_response_failure = MagicMock()
    mock_response_failure.status_code = 404
    
    def side_effect(url):
        if 'eltiempo' in url:
            return mock_response_success
        elif 'publimetro' in url:
            return mock_response_failure
    
    mock_requests.get.side_effect = side_effect
    mock_s3.put_object.return_value = {}
    
    # Ejecutar función
    app(mock_event, mock_context)
    
    # Verificaciones
    assert mock_requests.get.call_count == 2
    assert mock_s3.put_object.call_count == 1  # Solo uno exitoso

@patch('proyecto.s3')
@patch('proyecto.requests')
@patch('proyecto.datetime')
def test_app_all_failures(mock_datetime, mock_requests, mock_s3, 
                         mock_event, mock_context):
    """Prueba cuando ambos diarios fallan"""
    # Mock del timestamp
    mock_now = MagicMock()
    mock_now.strftime.return_value = '2025-05-28-10-30'
    mock_datetime.utcnow.return_value = mock_now
    
    # Mock de requests - ambos fallan
    mock_response_failure = MagicMock()
    mock_response_failure.status_code = 500
    
    mock_requests.get.return_value = mock_response_failure
    
    # Ejecutar función
    app(mock_event, mock_context)
    
    # Verificaciones
    assert mock_requests.get.call_count == 2
    assert mock_s3.put_object.call_count == 0  # Ninguno exitoso

@patch('proyecto.s3')
@patch('proyecto.requests')
@patch('proyecto.datetime')
def test_app_s3_upload_failure(mock_datetime, mock_requests, mock_s3, 
                              mock_event, mock_context, sample_eltiempo_html):
    """Prueba cuando la descarga es exitosa pero falla la subida a S3"""
    # Mock del timestamp
    mock_now = MagicMock()
    mock_now.strftime.return_value = '2025-05-28-10-30'
    mock_datetime.utcnow.return_value = mock_now
    
    # Mock de requests exitoso
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.content = sample_eltiempo_html
    mock_requests.get.return_value = mock_response
    
    # Mock de S3 que falla
    mock_s3.put_object.side_effect = Exception("Error de S3")
    
    # Ejecutar función (debería manejar la excepción)
    with pytest.raises(Exception):
        app(mock_event, mock_context)

@patch('proyecto.s3')
@patch('proyecto.requests') 
@patch('proyecto.datetime')
def test_app_correct_s3_keys(mock_datetime, mock_requests, mock_s3,
                            mock_event, mock_context, sample_eltiempo_html):
    """Prueba que las keys de S3 se generen correctamente"""
    # Mock del timestamp específico
    mock_now = MagicMock()
    mock_now.strftime.return_value = '2025-05-28-15-45'
    mock_datetime.utcnow.return_value = mock_now
    
    # Mock de requests exitoso
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.content = sample_eltiempo_html
    mock_requests.get.return_value = mock_response
    
    mock_s3.put_object.return_value = {}
    
    # Ejecutar función
    app(mock_event, mock_context)
    
    # Verificar que las keys sean correctas
    expected_calls = [
        {
            'Bucket': 'pacialcorte3-2025',
            'Key': 'raw/contenido-eltiempo-2025-05-28-15-45.html',
            'Body': sample_eltiempo_html
        },
        {
            'Bucket': 'pacialcorte3-2025', 
            'Key': 'raw/contenido-publimetro-2025-05-28-15-45.html',
            'Body': sample_eltiempo_html
        }
    ]
    
    assert mock_s3.put_object.call_count == 2
    # Verificar que se llamó con los parámetros correctos
    for call in mock_s3.put_object.call_args_list:
        call_kwargs = call[1]
        assert call_kwargs['Bucket'] == 'pacialcorte3-2025'
        assert call_kwargs['Key'].startswith('raw/contenido-')
        assert '2025-05-28-15-45' in call_kwargs['Key']