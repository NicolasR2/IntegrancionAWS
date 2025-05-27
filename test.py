import json
import pytest
from unittest.mock import patch, MagicMock

@pytest.fixture
def html_eltiempo():
    return """
    <html>
    <body>
    <div class="fecha">2025-01-01</div>
    <div class="noticia">
        <h2>Título Noticia</h2>
        <p>Contenido Noticia</p>
    </div>
    </body>
    </html>
    """

@patch("proyecto1.boto3.client")
@patch("proyecto1.s3")
def test_lambda2_valid_eltiempo(mock_boto_client, mock_s3, tmp_path, html_eltiempo):
    from proyecto1 import app, parse_el_tiempo

    fake_file = tmp_path / "page.html"
    fake_file.write_text(html_eltiempo, encoding='utf-8')

    mock_s3.download_file.side_effect = lambda b, k, f: open(f, 'w', encoding='utf-8').write(html_eltiempo)
    mock_s3.upload_file.return_value = None
    mock_s3.head_object.return_value = {"ResponseMetadata": {"HTTPStatusCode": 200}}  # Mock adicional
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

@patch("proyecto1.boto3.client")
@patch("proyecto1.s3")
def test_lambda2_html_without_fecha(mock_boto_client, mock_s3, tmp_path):
    from proyecto1 import app

    html = """
    <html>
    <body>
    <div class="noticia">
        <h2>Título Noticia</h2>
        <p>Contenido Noticia</p>
    </div>
    </body>
    </html>
    """

    mock_s3.download_file.side_effect = lambda b, k, f: open(f, 'w', encoding='utf-8').write(html)
    mock_s3.upload_file.return_value = None
    mock_s3.head_object.return_value = {"ResponseMetadata": {"HTTPStatusCode": 200}}  # Mock adicional
    mock_boto_client.return_value = MagicMock()

    event = {
        'Records': [{
            's3': {
                'bucket': {'name': 'pacialcorte3-2025'},
                'object': {'key': 'raw/contenido-eltiempo-sinfecha.html'}
            }
        }]
    }

    result = app(event, None)
    assert result["statusCode"] == 400
    assert "No se pudo extraer la fecha" in result["body"]

@patch("proyecto1.boto3.client")
@patch("proyecto1.s3")
def test_invoke_third_lambda(mock_boto_client, mock_s3):
    from proyecto1 import invoke_third_lambda

    mock_s3.head_object.return_value = {"ResponseMetadata": {"HTTPStatusCode": 200}}  # Mock adicional
    mock_lambda = MagicMock()
    mock_lambda.invoke.return_value = {
        "StatusCode": 200,
        "Payload": MagicMock(read=lambda: json.dumps({"message": "ok"}).encode("utf-8"))
    }

    mock_boto_client.side_effect = lambda service: mock_lambda if service == "lambda" else MagicMock()

    bucket = "pacialcorte3-2025"
    key = "procesado/archivo.json"
    result = invoke_third_lambda(bucket, key)
    assert result["StatusCode"] == 200
