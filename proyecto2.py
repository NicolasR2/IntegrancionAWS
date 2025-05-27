import boto3

def lambda_handler(event, context):
    glue = boto3.client('glue')

    # Nombre del crawler
    crawler_name = 'noticias-crawler'
    
    # Puedes filtrar si quieres solo ciertos tipos de archivos (opcional)
    for record in event['Records']:
        s3_key = record['s3']['object']['key']
        
        if s3_key.startswith('final/') and s3_key.endswith('.csv'):
            try:
                response = glue.start_crawler(Name=crawler_name)
                print(f"Crawler '{crawler_name}' iniciado con éxito.")
            except glue.exceptions.CrawlerRunningException:
                print(f"El crawler '{crawler_name}' ya está corriendo.")
            except Exception as e:
                print(f"Error al iniciar el crawler: {str(e)}")

    return {
        'statusCode': 200,
        'body': 'Evento procesado.'
    }