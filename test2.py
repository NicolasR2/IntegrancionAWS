import pytest
import boto3
from unittest.mock import patch, MagicMock
from botocore.exceptions import ClientError
from proyecto2 import app

@pytest.fixture
def mock_context():
    """Context básico para Lambda"""
    return {}

@pytest.fixture
def s3_event_csv_valid():
    """Event de S3 con archivo CSV válido en carpeta final/"""
    return {
        'Records': [{
            's3': {
                'object': {'key': 'final/periodico=eltiempo/year=2025/month=05/day=28/titulares.csv'}
            }
        }]
    }

@pytest.fixture
def s3_event_multiple_csv():
    """Event de S3 con múltiples archivos CSV válidos"""
    return {
        'Records': [
            {
                's3': {
                    'object': {'key': 'final/periodico=eltiempo/year=2025/month=05/day=28/titulares.csv'}
                }
            },
            {
                's3': {
                    'object': {'key': 'final/periodico=publimetro/year=2025/month=05/day=28/titulares.csv'}
                }
            }
        ]
    }

@pytest.fixture
def s3_event_csv_wrong_folder():
    """Event de S3 con archivo CSV pero en carpeta incorrecta"""
    return {
        'Records': [{
            's3': {
                'object': {'key': 'raw/contenido-eltiempo-2025-05-28.csv'}
            }
        }]
    }

@pytest.fixture
def s3_event_non_csv():
    """Event de S3 con archivo que no es CSV"""
    return {
        'Records': [{
            's3': {
                'object': {'key': 'final/periodico=eltiempo/year=2025/month=05/day=28/titulares.html'}
            }
        }]
    }

@pytest.fixture
def s3_event_mixed_files():
    """Event de S3 con archivos mixtos (algunos válidos, otros no)"""
    return {
        'Records': [
            {
                's3': {
                    'object': {'key': 'final/periodico=eltiempo/year=2025/month=05/day=28/titulares.csv'}
                }
            },
            {
                's3': {
                    'object': {'key': 'raw/contenido-publimetro-2025-05-28.html'}
                }
            },
            {
                's3': {
                    'object': {'key': 'final/periodico=publimetro/year=2025/month=05/day=28/data.txt'}
                }
            }
        ]
    }

@pytest.fixture
def mock_glue_client():
    """Mock para simular cliente de Glue"""
    mock_glue = MagicMock()
    mock_glue.start_crawler.return_value = {'ResponseMetadata': {'HTTPStatusCode': 200}}
    mock_glue.exceptions.CrawlerRunningException = ClientError
    return mock_glue

@patch('proyecto2.boto3.client')
def test_lambda_handler_single_csv_success(mock_boto3_client, s3_event_csv_valid, 
                                          mock_context, mock_glue_client):
    """Prueba inicio exitoso del crawler con un archivo CSV válido"""
    mock_boto3_client.return_value = mock_glue_client
    
    result = lambda_handler(s3_event_csv_valid, mock_context)
    
    # Verificaciones
    assert result['statusCode'] == 200
    assert result['body'] == 'Evento procesado.'
    mock_boto3_client.assert_called_once_with('glue')
    mock_glue_client.start_crawler.assert_called_once_with(Name='noticias-crawler')

@patch('proyecto2.boto3.client')
def test_lambda_handler_multiple_csv_success(mock_boto3_client, s3_event_multiple_csv,
                                           mock_context, mock_glue_client):
    """Prueba con múltiples archivos CSV válidos"""
    mock_boto3_client.return_value = mock_glue_client
    
    result = lambda_handler(s3_event_multiple_csv, mock_context)
    
    # Verificaciones
    assert result['statusCode'] == 200
    assert result['body'] == 'Evento procesado.'
    # El crawler se debe intentar iniciar por cada archivo válido
    assert mock_glue_client.start_crawler.call_count == 2

@patch('proyecto2.boto3.client')
def test_lambda_handler_crawler_already_running(mock_boto3_client, s3_event_csv_valid,
                                               mock_context):
    """Prueba cuando el crawler ya está corriendo"""
    mock_glue = MagicMock()
    
    # Simular excepción de crawler ya corriendo
    error_response = {'Error': {'Code': 'CrawlerRunningException'}}
    
    mock_glue.exceptions.CrawlerRunningException = ClientError 
    mock_glue.start_crawler.side_effect = ClientError(error_response, 'StartCrawler')
   
    mock_boto3_client.return_value = mock_glue
    
    result = lambda_handler(s3_event_csv_valid, mock_context)
    
    # Verificaciones
    assert result['statusCode'] == 200
    assert result['body'] == 'Evento procesado.'
    mock_glue.start_crawler.assert_called_once()

@patch('proyecto2.boto3.client')
def test_lambda_handler_glue_error(mock_boto3_client, s3_event_csv_valid,
                                  mock_context):
    """Prueba manejo de errores generales de Glue"""
    mock_glue = MagicMock()
    
    # Simular error general
    mock_glue.start_crawler.side_effect = Exception("Error de Glue")
    mock_glue.exceptions.CrawlerRunningException = ClientError # Still good practice to include
    mock_boto3_client.return_value = mock_glue
    
    result = lambda_handler(s3_event_csv_valid, mock_context)
    
    # Verificaciones
    assert result['statusCode'] == 200
    assert result['body'] == 'Evento procesado.'
    mock_glue.start_crawler.assert_called_once()

@patch('proyecto2.boto3.client')
def test_lambda_handler_csv_wrong_folder(mock_boto3_client, s3_event_csv_wrong_folder,
                                        mock_context, mock_glue_client):
    """Prueba que no se inicie el crawler para CSV en carpeta incorrecta"""
    mock_boto3_client.return_value = mock_glue_client
    
    result = lambda_handler(s3_event_csv_wrong_folder, mock_context)
    
    # Verificaciones
    assert result['statusCode'] == 200
    assert result['body'] == 'Evento procesado.'
    # No debe intentar iniciar el crawler
    mock_glue_client.start_crawler.assert_not_called()

@patch('proyecto2.boto3.client')
def test_lambda_handler_non_csv_file(mock_boto3_client, s3_event_non_csv,
                                    mock_context, mock_glue_client):
    """Prueba que no se inicie el crawler para archivos que no son CSV"""
    mock_boto3_client.return_value = mock_glue_client
    
    result = lambda_handler(s3_event_non_csv, mock_context)
    
    # Verificaciones
    assert result['statusCode'] == 200
    assert result['body'] == 'Evento procesado.'
    # No debe intentar iniciar el crawler
    mock_glue_client.start_crawler.assert_not_called()

@patch('proyecto2.boto3.client')
def test_lambda_handler_mixed_files(mock_boto3_client, s3_event_mixed_files,
                                   mock_context, mock_glue_client):
    """Prueba con archivos mixtos (solo uno válido)"""
    mock_boto3_client.return_value = mock_glue_client
    
    result = lambda_handler(s3_event_mixed_files, mock_context)
    
    # Verificaciones
    assert result['statusCode'] == 200
    assert result['body'] == 'Evento procesado.'
    # Solo debe intentar iniciar el crawler una vez (para el archivo válido)
    mock_glue_client.start_crawler.assert_called_once_with(Name='noticias-crawler')

@patch('proyecto2.boto3.client')
def test_lambda_handler_empty_event(mock_boto3_client, mock_context, mock_glue_client):
    """Prueba con evento vacío (sin records)"""
    empty_event = {'Records': []}
    mock_boto3_client.return_value = mock_glue_client
    
    result = lambda_handler(empty_event, mock_context)
    
    # Verificaciones
    assert result['statusCode'] == 200
    assert result['body'] == 'Evento procesado.'
    # No debe intentar iniciar el crawler
    mock_glue_client.start_crawler.assert_not_called()

def test_csv_path_validation_final_folder():
    """Prueba validación de rutas - carpeta final/"""
    # Casos válidos
    valid_paths = [
        'final/periodico=eltiempo/year=2025/month=05/day=28/titulares.csv',
        'final/data.csv',
        'final/subfolder/another.csv'
    ]
    
    for path in valid_paths:
        assert path.startswith('final/') and path.endswith('.csv')

def test_csv_path_validation_invalid_cases():
    """Prueba validación de rutas - casos inválidos"""
    # Casos inválidos
    invalid_paths = [
        'raw/contenido-eltiempo-2025-05-28.csv',  # Carpeta incorrecta
        'final/titulares.html',  # Extensión incorrecta
        'other/data.csv',  # Carpeta incorrecta
        'final/titulares.txt'  # Extensión incorrecta
    ]
    
    for path in invalid_paths:
        is_valid = path.startswith('final/') and path.endswith('.csv')
        assert not is_valid

@patch('proyecto2.boto3.client')
def test_lambda_handler_crawler_name_consistency(mock_boto3_client, s3_event_csv_valid,
                                                mock_context, mock_glue_client):
    """Prueba que se use consistentemente el nombre correcto del crawler"""
    mock_boto3_client.return_value = mock_glue_client
    
    lambda_handler(s3_event_csv_valid, mock_context)
    
    # Verificar que se use el nombre correcto
    mock_glue_client.start_crawler.assert_called_with(Name='noticias-crawler')

@patch('proyecto2.boto3.client')
def test_lambda_handler_return_format(mock_boto3_client, s3_event_csv_valid,
                                     mock_context, mock_glue_client):
    """Prueba formato de respuesta de la función Lambda"""
    mock_boto3_client.return_value = mock_glue_client
    
    result = lambda_handler(s3_event_csv_valid, mock_context)
    
    # Verificar estructura de respuesta
    assert isinstance(result, dict)
    assert 'statusCode' in result
    assert 'body' in result
    assert result['statusCode'] == 200
    assert isinstance(result['body'], str)