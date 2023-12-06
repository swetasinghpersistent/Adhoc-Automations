# Adhoc-Automations

AdHoc Automations

> Prerequisite: Python3.11

> gcloud auth application-default login    		# makesure to have required access

Get started

```
git clone https://github.com/imRohitsahu/Adhoc-Automations.git
cd Adhoc-Automations
```

Step 1:  Install python required libraries.

```
python3 -m venv venv
source venv/bin/activate
pip3 install -r requirements.txt
```

Step 2: Add all required variables in the .env file. For ex.

```
PROJECT_ID=meesho-dataengg-prd-0622
EXCEL_FILE_PATH=jobs_info.xlsx          # excel sheet must have two column job_name and location
DEST_BUCKET_NAME=gcs-deng-dping-data-platform-hive-prd       # gcp bucket
SOURCE_BUCKET_NAME=data-platform-hive   #aws bucket
POOL_OPTION1=meesho-dataengg-sts-transfer-pool-1
POOL_OPTION2=meesho-dataengg-sts-transfer-pool-2
ENDPOINT1=bucket.vpce-0dc2fb490b50f02e2-uwoenjfk.s3.ap-southeast-1.vpce.amazonaws.com
ENDPOINT2=bucket.vpce-0fb63f3ffd470c949-ntbhdiq9.s3.ap-southeast-1.vpce.amazonaws.com
PREFIX="year=2023/,_delta_log/"           # Optional if require to add prefix
START_TIMEDELTA=10                     #Jobs will be schedule after X minutes of creation, this is to intertain the execution time delay
```

Step 3: Create an Excel sheet with two columns `job_name` and `location` and save it as `jobs_info.xlsx`

|     job_name     | location           |
| :--------------: | ------------------ |
| unique_job_name1 | path/to/directory1 |
| unique_job_name2 | path/to/directory2 |

Step 4: Just run the script.

```
python3 scheduleStorageTransferJob_v3.py
```

Step 5: Observe the  `transfer_job.log` and review the created jobs in the gcloud console.

`Feel free to customize it as per requirement.`
