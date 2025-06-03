import json
import boto3
import os
import logging
from datetime import datetime

# Configurar logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def app(event, context):
    """
    Lanza un clúster EMR, añade un paso para ejecutar un script de Spark y se configura para auto-terminarse.
    """
    emr_client = boto3.client('emr')

    # --- Configuración del Clúster EMR ---
    # ¡IMPORTANTE! Reemplaza estos valores con tu configuración específica.
    EMR_RELEASE_LABEL = 'emr-6.15.0'  # O la versión más reciente que necesites
    MASTER_INSTANCE_TYPE = 'm5.xlarge'
    CORE_INSTANCE_TYPE = 'm5.xlarge'
    CORE_INSTANCE_COUNT = 1  # Ajusta según tus necesidades (mínimo 1 para Spark)
    
    # Debes reemplazar esto con un ID de subred válido en tu VPC.
    # El clúster EMR se lanzará en esta subred.
    # Puedes obtenerlo desde la consola de VPC -> Subnets.
    # Asegúrate de que la subred tenga acceso a S3 (e.g., a través de un NAT Gateway o S3 VPC Endpoint).
    EC2_SUBNET_ID = os.environ.get('EC2_SUBNET_ID', 'subnet-xxxxxxxxxxxxxxxxx') 
    
    # Roles IAM para EMR. Usualmente 'EMR_EC2_DefaultRole' y 'EMR_DefaultRole'.
    # Asegúrate de que estos roles existen y tienen los permisos necesarios.
    JOB_FLOW_ROLE = os.environ.get('EMR_EC2_DEFAULT_ROLE', 'EMR_EC2_DefaultRole')
    SERVICE_ROLE = os.environ.get('EMR_DEFAULT_ROLE', 'EMR_DefaultRole')

    # Ubicación del script de Spark en S3
    SPARK_SCRIPT_S3_PATH = 's3://parcialfinal2025/app/script.py'
    
    # Nombre para el clúster EMR (puedes hacerlo dinámico)
    cluster_name = f"EMR-Spark-Job-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
    
    # Opcional: Bucket S3 para los logs del clúster EMR
    # Asegúrate de que este bucket exista y el rol de EMR tenga permisos para escribir en él.
    LOG_S3_URI = f"s3://{os.environ.get('EMR_LOG_BUCKET', 'tu-bucket-de-logs-emr')}/elasticmapreduce/"

    logger.info(f"Iniciando el lanzamiento del clúster EMR: {cluster_name}")
    logger.info(f"Script de Spark a ejecutar: {SPARK_SCRIPT_S3_PATH}")
    logger.info(f"Subred EC2: {EC2_SUBNET_ID}")

    if EC2_SUBNET_ID == 'subnet-xxxxxxxxxxxxxxxxx':
        logger.error("EC2_SUBNET_ID no está configurado correctamente. Por favor, actualiza la variable de entorno o el código.")
        raise ValueError("EC2_SUBNET_ID no configurado.")

    try:
        response = emr_client.run_job_flow(
            Name=cluster_name,
            LogUri=LOG_S3_URI, # Opcional, pero recomendado
            ReleaseLabel=EMR_RELEASE_LABEL,
            Instances={
                'InstanceGroups': [
                    {
                        'Name': 'Master nodes',
                        'Market': 'ON_DEMAND', # O 'SPOT'
                        'InstanceRole': 'MASTER',
                        'InstanceType': MASTER_INSTANCE_TYPE,
                        'InstanceCount': 1,
                    },
                    {
                        'Name': 'Core nodes',
                        'Market': 'ON_DEMAND', # O 'SPOT'
                        'InstanceRole': 'CORE',
                        'InstanceType': CORE_INSTANCE_TYPE,
                        'InstanceCount': CORE_INSTANCE_COUNT,
                    }
                ],
                'Ec2KeyName': os.environ.get('EC2_KEY_NAME', ''), # Opcional: si necesitas acceso SSH al clúster
                'KeepJobFlowAliveWhenNoSteps': False, # Clave para que se termine automáticamente
                'TerminationProtected': False, # Permite que se termine mediante API/Auto-terminación
                'Ec2SubnetId': EC2_SUBNET_ID,
                # 'EmrManagedMasterSecurityGroup': 'sg-xxxxxxxx', # Opcional: Especificar SGs
                # 'EmrManagedSlaveSecurityGroup': 'sg-xxxxxxxx',  # Opcional: Especificar SGs
            },
            Steps=[
                {
                    'Name': 'Run Spark Application',
                    'ActionOnFailure': 'TERMINATE_CLUSTER', # Termina el clúster si el paso falla
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': [
                            'spark-submit',
                            SPARK_SCRIPT_S3_PATH
                            # Puedes añadir más argumentos para spark-submit aquí si es necesario
                            # Por ejemplo:
                            # '--deploy-mode', 'cluster',
                            # '--conf', 'spark.executor.memory=2g',
                            # 's3://pacialcorte3-2025/app/script.py',
                            # 'arg1', 'arg2' # Argumentos para tu script
                        ]
                    }
                }
            ],
            Applications=[{'Name': 'Spark'}], # Asegúrate de que Spark esté incluido
            JobFlowRole=JOB_FLOW_ROLE, # Rol para las instancias EC2 del clúster
            ServiceRole=SERVICE_ROLE,  # Rol para el servicio EMR
            VisibleToAllUsers=True,
            AutoTerminationPolicy={ # Política explícita de auto-terminación (opcional si KeepJobFlowAliveWhenNoSteps=False)
                'IdleTimeout': int(os.environ.get('EMR_IDLE_TIMEOUT_SECONDS', 3600)) # Termina después de 1 hora de inactividad (si no hay pasos)
            }
        )
        
        job_flow_id = response['JobFlowId']
        logger.info(f"Clúster EMR lanzado con éxito. JobFlowId: {job_flow_id}")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Cluster EMR lanzado exitosamente',
                'jobFlowId': job_flow_id
            })
        }

    except Exception as e:
        logger.error(f"Error al lanzar el clúster EMR: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Error al lanzar el clúster EMR',
                'error': str(e)
            })
        }

# Para pruebas locales (simulando un evento Lambda)
if __name__ == "__main__":
    # Configura estas variables de entorno si pruebas localmente
    # os.environ['EC2_SUBNET_ID'] = 'subnet-tu_subnet_id_real' 
    # os.environ['EMR_LOG_BUCKET'] = 'tu-bucket-de-logs-emr-real'
    # os.environ['EC2_KEY_NAME'] = 'tu-llave-ec2' # Opcional

    # Verifica que la subred esté configurada para pruebas locales
    if os.environ.get('EC2_SUBNET_ID', 'subnet-xxxxxxxxxxxxxxxxx') == 'subnet-xxxxxxxxxxxxxxxxx':
        print("Por favor, configura la variable de entorno EC2_SUBNET_ID para pruebas locales.")
    else:
        mock_event = {}
        mock_context = {}
        print(app(mock_event, mock_context))
