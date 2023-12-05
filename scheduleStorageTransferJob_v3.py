import pandas as pd
from google.cloud import storage_transfer_v1
from google.cloud import storage
from google.protobuf.timestamp_pb2 import Timestamp
from google.api_core.exceptions import AlreadyExists
from datetime import datetime, timedelta
import logging
import os
import time
from dotenv import load_dotenv
load_dotenv()
def enableLogging():
    logging.basicConfig(filename='transfer_job.log', level=logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    file_handler = logging.FileHandler('transfer_job.log')
    file_handler.setFormatter(formatter)
    
    logger = logging.getLogger()
    logger.addHandler(file_handler)

    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger = logging.getLogger()
    logger.addHandler(handler)



"""
Script for creating Google Cloud Storage transfer jobs using the Storage Transfer Service API.
- Handles logging, error handling, and security considerations.
"""

def delete_jobs(_job,project_id):
    try:
        transfer_client.delete_transfer_job(
            request=storage_transfer_v1.DeleteTransferJobRequest(
                job_name="transferJobs/" + _job,
                project_id=project_id
            )
        )
        print(f'Transfer job deleted: {job_name}')
    except Exception as e:
        print(f"An error occurred while deleting the transfer job: {e}")


def is_directory_exists(bucket_name, directory_path):
    client = storage.Client(project=project_id)
    bucket = client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=directory_path)
    return any(blobs)

def checkPath(bucket_name, directory_path):
    if is_directory_exists(bucket_name, directory_path):
        logging.info(f"The directory **{directory_path}** exists in the bucket **{bucket_name}**.")
        return True
    else:
        logging.info(f"The directory **{directory_path}** does not exists in the bucket **{bucket_name}**.")
        return False

if __name__== '__main__':
    enableLogging()
    project_id = os.environ.get("PROJECT_ID", "meesho-supply-prd-0622")
    excel_file_path= os.environ.get("EXCEL_FILE_PATH", "jobs_info.xlsx")
    dest_bucket_name=os.environ.get("DEST_BUCKET_NAME", "meesho-supply-v2")
    source_bucket_name=os.environ.get("SOURCE_BUCKET_NAME", "gcs-supl-fnf-pdf-prd")
    jobs_df = pd.read_excel(excel_file_path, sheet_name='Sheet1', engine='openpyxl')
    # pool_option1 = "meesho-dataengg-sts-transfer-pool-1"
    # pool_option2 = "meesho-dataengg-sts-transfer-pool-2"
    # endpoint1= "bucket.vpce-0dc2fb490b50f02e2-uwoenjfk.s3.ap-southeast-1.vpce.amazonaws.com"
    # endpoint2= "bucket.vpce-0fb63f3ffd470c949-ntbhdiq9.s3.ap-southeast-1.vpce.amazonaws.com"
    # switchEndpoint=True
    # switchPool = True
    transfer_client = storage_transfer_v1.StorageTransferServiceClient()
    cnt=0
    for index, row in jobs_df.iterrows():
        cnt += 1
        if cnt == 50:
            time.sleep(20)
            cnt=0
        job_name = row['job_name']
        location = row['location']
        logging.info(f'job_name: {job_name}, location: {location}')
        # delete_jobs(job_name,project_id)
        # continue
        # if not checkPath("gcs-deng-dping-data-platform-hive-prd",location):
        #     logging.info(f"skipping this job..")
        #     continue

        new_transfer_job = storage_transfer_v1.TransferJob()
        new_transfer_job.name = "transferJobs/" + job_name
        new_transfer_job.description= 'S3 Data Transfer for meesho-supply-v2 for Expressbees'
        new_transfer_job.project_id = project_id
        new_transfer_job.status = 1

        current_time = datetime.utcnow()
        current_time= current_time+ timedelta(minutes=20)
        timestamp_pb = Timestamp()
        timestamp_pb.FromDatetime(current_time)

        future_time = current_time + timedelta(minutes=30)
        future_timestamp_pb = Timestamp()
        future_timestamp_pb.FromDatetime(future_time)

        start_seconds = timestamp_pb.seconds % 60
        end_seconds = future_timestamp_pb.seconds % 60

        schedule_dict = {
            "schedule_start_date": {"year": current_time.year, "month": current_time.month, "day": current_time.day},
            "start_time_of_day": {"hours": current_time.hour, "minutes": current_time.minute, "seconds": start_seconds},
            # "end_time_of_day": {"hours": future_time.hour, "minutes": future_time.minute, "seconds": end_seconds},
            "schedule_end_date": {"year": future_time.year, "month": future_time.month, "day": future_time.day},
        }

        schedule = storage_transfer_v1.Schedule(**schedule_dict)
        new_transfer_job.schedule = schedule
        transfer_spec = storage_transfer_v1.TransferSpec(
            gcs_data_sink=storage_transfer_v1.GcsData(
                bucket_name=dest_bucket_name,
                # path=f'{location}/'
                path=f'{location}'
            ),
            aws_s3_compatible_data_source=storage_transfer_v1.AwsS3CompatibleData(
                bucket_name=source_bucket_name,
                # path=f'{location}/',
                path=f'{location}',
                endpoint="bucket.vpce-057a79cdc14bd9f9a-m1g7h019.s3.ap-southeast-1.vpce.amazonaws.com",
                # endpoint=f"{endpoint1 if switchEndpoint else endpoint2 }",
                region='ap-southeast-1',
                s3_metadata=storage_transfer_v1.S3CompatibleMetadata(
                    auth_method=storage_transfer_v1.S3CompatibleMetadata.AuthMethod.AUTH_METHOD_AWS_SIGNATURE_V4,
                    request_model=storage_transfer_v1.S3CompatibleMetadata.RequestModel.REQUEST_MODEL_VIRTUAL_HOSTED_STYLE,
                    protocol=storage_transfer_v1.S3CompatibleMetadata.NetworkProtocol.NETWORK_PROTOCOL_HTTPS,
                    list_api=storage_transfer_v1.S3CompatibleMetadata.ListApi.LIST_OBJECTS_V2
                )
            ),
            # object_conditions=storage_transfer_v1.ObjectConditions(
            #     include_prefixes=['year=2023/', '_delta_log/']
            # ),
            transfer_options=storage_transfer_v1.TransferOptions(
                overwrite_when=storage_transfer_v1.TransferOptions.OverwriteWhen.DIFFERENT,
                metadata_options=storage_transfer_v1.MetadataOptions(
                    storage_class=storage_transfer_v1.MetadataOptions.StorageClass.STORAGE_CLASS_DESTINATION_BUCKET_DEFAULT,
                    time_created=storage_transfer_v1.MetadataOptions.TimeCreated.TIME_CREATED_PRESERVE_AS_CUSTOM_TIME
                )
            ),
            source_agent_pool_name=f'projects/{project_id}/agentPools/meesho-supply-prd-sts-transfer-pool'
            # source_agent_pool_name=f'projects/{project_id}/agentPools/{pool_option1 if switchPool else pool_option2}'
        )
        # switchPool = not switchPool
        # switchEndpoint = not switchEndpoint
        new_transfer_job.transfer_spec = transfer_spec

        try:
            # print(new_transfer_job)
            response = transfer_client.create_transfer_job(
                request=storage_transfer_v1.CreateTransferJobRequest(transfer_job=new_transfer_job)
            )
            logging.info(f'Transfer job created: {response.name}')
        except AlreadyExists as e:
            logging.error(f"Resource already exists: {e}")
        except Exception as e:
            logging.error(f"An unexpected error occurred: {e}")
        # exit(0)
        

    logging.info('Script completed.')
