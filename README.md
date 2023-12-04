# Adhoc-Automations

AdHoc Automations

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
PROJECT_ID='meesho-supply-prd-0622'
EXCEL_FILE_PATH="jobs_info.xlsx"
DEST_BUCKET_NAME="meesho-supply-v2"
SOURCE_BUCKET_NAME="gcs-supl-fnf-pdf-prd"
```
Step 3: Create an Excel sheet with two columns `job_name` and `location` and save it as `jobs_info.xlsx`

Step 4: Just run the script.
```
python3 scheduleStorageTransferJob_v3.py
```
Step 5: Observe the  `transfer_job.log` and review the created jobs in the gcloud console.

`Feel free to customize it as per requirement.`
