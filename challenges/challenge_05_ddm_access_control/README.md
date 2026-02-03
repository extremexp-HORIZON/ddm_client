# Challenge 05 — DDM & Access Control

This folder contains the scripts needed to complete **Challenge 05**.

In this challenge you will explore the **core functionalities of Decentralized Data Management (DDM)** for **data and knowledge management**.


**A) Data / Metadata management & catalog**
- Upload datasets and manage them under a project (Steps **1–3**)
- Run advanced catalog queries and save them (Steps **5–6**)
- Download data and list catalog entries (Steps **9–10**)
- Attach custom uploader metadata (Step **2**)
- Inspect how uploader metadata is stored and retrieved via the catalog (Steps **3, 10**)


**B) DDM services and generated artifacts**
- **DDM metadata generation** (Step **3**)
- **Profiling / Profile Reports** and Data Duality Metrics (Step **4**)
- **Expectations Suite creation** and **Validations** (Steps **7–8**)
- **Metadata enrichment with LLM** (column descriptions / enrichment shown in the Expectations UI) (Step **7**)

**C) Access Control**
- Understand how DDM links files and actions to a **user identity** 
- Explore how uploaded datasets are associated with a specific **project_id** and uploader

✅ By the end, you should understand how DDM produces artifacts (metadata, reports, expectations, validations), how they are stored, and how they can be explored through both the SDK outputs and the DDM UI.

---

## Prerequisites

### 1) Install dependencies

From the project root:

```powershell
pip install -e ".[dev]"
```

### 2) Configure `.env`

Make sure you have a `.env` in the **project root** with at least:

```env
DDM_BASE_URL=https://ddm.extremexp-icom.intracom-telecom.com
DDM_AUTH_URL=https://ddm.extremexp-icom.intracom-telecom.com
DDM_USERNAME=...
DDM_PASSWORD=...
DDM_STORAGE_DIR=out/runtime
```

If you intend to try blockchain interactions (Sepolia), also set:

```env
SEPOLIA_RPC_URL=https://sepolia.infura.io/v3/<YOUR_INFURA_PROJECT_ID>
DDM_RPC_URL=https://sepolia.infura.io/v3/<YOUR_INFURA_PROJECT_ID>
DDM_USER_PK=0x...
```

> ⚠️ Never commit your private key.

---

## Sample files

Example inputs used in this guide : `sample_files/`
- `\challenges\sample_files\Titanic-Dataset.csv`
- `.\challenges\sample_files\titanic.parquet`
- `.\challenges\sample_files\titanic-sample.csv`
- `.\challenges\sample_files\titanic_large.csv`
- `.\challenges\sample_files\expectations.json`
- `.\challenges\sample_files\filters.json`
- `.\challenges\sample_files\uploader_metadata.json`



---

## Step-by-step

> Run these commands **from the project root**.

### 1) Upload files
- Upload three files under project_id: tutorial-username
```powershell
python .\challenges\challenge_05_ddm_access_control\01_upload_file.py `
  --project_id "tutorial-<username>" `
  --name "Titanic-Dataset.csv" `
  --name "titanic.parquet" `
  --name "titanic_large.csv" `
  --description "Challenge 05 CSV upload" `
  --description "Challenge 05 Parquet upload" `
  --description "Challenge 05 Large CSV upload" `
  --use-case "crisis" `
  --use-case "crisis" `
  --use-case "crisis" `
  ".\challenges\sample_files\Titanic-Dataset.csv" `
  ".\challenges\sample_files\titanic.parquet" `
  ".\challenges\sample_files\titanic_large.csv"        
```

✅ After upload, **copy the returned `file_id` values** — you will use them in the next steps.

---

### 2) Create uploader metadata JSON and attach it

Edit JSON file  (`uploader_metadata.json`) in sample_files folder to include your name and upload it as uploader metadata for parquet file

```powershell
python .\challenges\challenge_05_dmm_access_control\02_attach_metadata.py `
  --project_id <tutorial-username> `
  --file_id <PASTE_FILE_ID_FROM_STEP_1> `
  --json-file ".\challenges\sample_files\uploader_metadata.json"

```

---

### 3) Download DDM generated file metadata for both files

```powershell
python .\challenges\challenge_05_ddm_access_control\03_download_file_metadata.py `
  --project_id <tutorial-username> `
  --file-id <CSV_FILE_ID> `
  --file-id <PARQUET_FILE_ID>
```

Check file metadata.
Outputs will be written under:

```text
out/runtime/projects/<project_id>/files/<file_id>/
```

---

### 4) Download profiling report for csv file (HTML) and open it 

```powershell
python .\challenges\challenge_05_dmm_access_control\04_download_report_html.py `
  --project_id <tutorial-username> `
  --file_id <CSV_FILE_ID>
```

Open the report (saved_to path) and check data duality metrics:

```powershell
start "saved_to_path"
```

---

### 5) Set / Run custom advanced catalog query

> Tip: Fill in project_id as tutorial-username.
Using filers JSON file in sample files folders (`filters.json`) run:

```powershell
python .\challenges\challenge_05_dmm_access_control\05_catalog_advanced.py `
  --project_id tutorial-<username> `
  --json-file ".\challenges\sample_files\filters.json"
```

See response.
What is the count of listed files?

Can you fix the filters so that the query returns only the csv you uploaded in step 1?

Rerun until you see file "Files listed: 1" 

### 6) Save Custom Advanced Query

> Tip: Save the query as "turorial-username".

```powershell
python .\challenges\challenge_05_dmm_access_control\06_save_advanced_query.py `
  --username "<demo_user>" `
  --name "<tutorial-username>" `
  --json-file ".\challenges\sample_files\filters.json"

```

### 7) Create expectations suite 

This step will:

    Upload a sample (for expectations generation & column descriptions enrichment using LLM service)
    Create an expectation suite using sample titanic file from sample_files folder
> Tip: Set project_id and suite-name as tutorial-username

Print a suite_id in the output (keep it for Step 8)
```powershell
python .\challenges\challenge_05_dmm_access_control\07_create_suite.py `
   ".\challenges\sample_files\titanic_sample.csv" `
   --project_id <tutorial-username> `
   --suite-name <tutorial-username> `
   --datasource-name default `
   --user_id <username>> `
   --file_type csv `
   --expectations-file ".\challenges\sample_files\expectations.json" `
   --poll --timeout 600 --interval 1 `
   --category "crisis" `
   --use_case "crisis" `
   --description "Titanic dataset expectations suite (Challenge 05)"
```
✅ Copy the suite_id printed by this command — you will use it in Step 8.

Navigate to: https://ddm.extremexp-icom.intracom-telecom.com/expectations
and see the Detailed Expectation Suite with LLM column descriptions endrichment and Graphs.

---
### 8) Validate dataset(s) against the suite

In this step you will validate a file against the expectation suite created in **Step 7**.

You need:
- **`--suite-id`** → the `suite_id` printed at the end of **Step 7**
- **`--file-id`** → the `file_id` for **Titanic-Dataset.csv** printed in **Step 1**
> Tip: Fill in project_id as tutorial-username.

```powershell
python .\challenges\challenge_05_dmm_access_control\08_validate.py `
  --project_id "tutorial-<username>" `
  --file-id "<CSV_FILE_ID_FROM_STEP_1>" `
  --suite-id "<SUITE_ID_FROM_STEP_7>" `
  --poll --timeout 600 --interval 1 `
  --lookback-minutes 180
```

✅ After it completes, answer these questions:

#### A) Did the validation succeed?
Open the validation results page:
- https://ddm.extremexp-icom.intracom-telecom.com/validation-results

Find your latest run and check the status (**SUCCESS / FAILURE**) and summary.

#### B) Which expectations failed and why?
Use these hints to debug:

- **Validation Results UI** (shows failed checks + messages):  
  https://ddm.extremexp-icom.intracom-telecom.com/validation-results

- **Profile / Report Viewer** (helps explain failures like nulls, distributions, missing columns):  
  https://ddm.extremexp-icom.intracom-telecom.com/report_viewer/<FILE_ID>

- **DDM Catalog entry** (metadata + artifacts for the file):  
  https://ddm.extremexp-icom.intracom-telecom.com/my-catalog

> Tip: If an expectation fails (example: `Age` has null values), open the report viewer for the same file and locate the column statistics.

---

#### C) Repeat the validation using the large dataset
Run the same validation again, but now use the `file_id` for the **large dataset** you uploaded in Step 1 (the parquet / large file entry).
> Tip: Fill in project_id as tutorial-username.

```powershell
python .\challenges\challenge_05_dmm_access_control\08_validate.py `
  --project_id <tutorial-username> `
  --file-id "<LARGE_FILE_ID_FROM_STEP_1>" `
  --suite-id "<SUITE_ID_FROM_STEP_7>" `
  --poll --timeout 600 --interval 1 `
  --lookback-minutes 180
```

✅ Compare results:

- Do the same expectations pass or fail this time?
- What changed in the dataset that explains the difference?


---
### 9) Download file
Use:
- file_id = the file you want to download (the Titanic-Dataset CSV file id from Step 1)
> Tip: Fill in project_id as tutorial-username.

```powershell
python .\challenges\challenge_05_dmm_access_control\09_download_file.py `
  --project_id tutorial-<usename> `
  --file_id "<FILE_ID_TO_DOWNLOAD>"
```

After this, check output under your runtime storage directory.
out/runtime/projects/tutorial-<username>/files/<file_id>/

---
### 10) Get Catalog

This task will list catalog entries for the project (useful to verify the uploaded files appear in the project catalog).
> Tip: Fill in project_id as tutorial-username.

```powershell
python .\challenges\challenge_05_dmm_access_control\10_catalog_list.py `                                     
   --project_id tutorial-<username> `
```
---                                
 ## What to submit

You need to provide:
- file_ids from Step 1
- query_id from Step 6
- suite_id from Step 7
- Screenshots of Expectations suites and Validation Results
