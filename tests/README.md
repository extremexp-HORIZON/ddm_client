# Tests (ddm sdk)

This folder contains the **pytest** test suites for the ddm sdk.

> ✅ Run tests from the **project root** (recommended).

---

## Install test dependencies

From project root:

```bash
pip install -e ".[dev]"
```

---

## Configuration (.env) for tests

Most tests require a `.env` file in the project root (recommended).  
Blockchain tests run on **Ethereum Sepolia** and require:

- `SEPOLIA_RPC_URL` (recommended: Infura)
- a signing private key (e.g. `DDM_USER_PK`) for transactions

> ⚠️ Never commit real private keys.

### Example `.env` (test-related variables)

```env
# --- tests / convenience ---
DDM_TEST_NETWORK=sepolia
DDM_TEST_PROJECT_ID=tutorial/test

# file upload tests
DDM_SAMPLE_PATH=C:\Users\orest\Downloads\result.csv
DDM_SAMPLE_METADATA_PATH=C:\Users\orest\Downloads\file_metadata.json

# expectations validations tests
DDM_EXPECTATIONS_SAMPLE_FILE_PATH=C:\Users\orest\Downloads\expectations_sample.csv
DDM_TEST_EXPECTATIONS_DATASET_ID=2813033a-daff-458c-98f3-37330490d84d
DDM_TEST_FILE_ID=2813033a-daff-458c-98f3-37330490d84d
DDM_TEST_FILE_IDS=2813033a-daff-458c-98f3-37330490d84d
DDM_TEST_FILES_DELETE_IDS=

DDM_TEST_SUITE_ID=f82ea898-43c4-4d39-9946-8fe8dfa7aa44
DDM_TEST_SUITE_IDS=f82ea898-43c4-4d39-9946-8fe8dfa7aa44,another-suite-id
DDM_TEST_EXPECTATIONS_SUITE_ID=f82ea898-43c4-4d39-9946-8fe8dfa7aa44
DDM_TEST_EXPECTATIONS_CATEGORY=dataset
DDM_TEST_EXPECTATIONS_FILE_TYPE=csv

DDM_TEST_SAVE_RESULT=1
DDM_TEST_REQUESTER=0x11B1C48B2f084ff1b5E49eD872bf9A90092A899a
DDM_TEST_UPLOADER=0x11B1C48B2f084ff1b5E49eD872bf9A90092A899a

# enable web3 tests:
DDM_TEST_PREPARE_SUITE=1
DDM_TEST_PREPARE_REWARD=1
DDM_TEST_PREPARE_VALIDATION=1
DDM_TEST_PREPARE_REPORT=1

# optional
DDM_TEST_LINK_URL=https://example.com/data.csv
DDM_BIG_PATH=C:\Users\orest\Downloads\big.parquet
```

---


## Run tests by folder 


1) **catalog**
```bash
pytest tests/catalog
```
![catalog tests](../docs/images/tests/catalog_tests.png)

2) **file**
```bash
pytest tests/file
```
![file tests](../docs/images/tests/file_tests.png)

3) **files**
```bash
pytest tests/files
```
![files tests](../docs/images/tests/files_tests.png)

4) **file_metadata**
```bash
pytest tests/file_metadata
```
![file metadata tests](../docs/images/tests/file_metadata.png)

5) **uploader_metadata**
```bash
pytest tests/uploader_metadata
```
![uploader metadata tests](../docs/images/tests/uploader_metadata.png)

6) **expectations**
```bash
pytest tests/expectations
```
![expectations tests](../docs/images/tests/expectations_tests.png)

7) **validations**
```bash
pytest tests/validations
```
![validations tests](../docs/images/tests/validations_tests.png)

8) **parametrics**
```bash
pytest tests/parametrics
```
![parametrics tests](../docs/images/tests/parametrics_tests.png)

9) **user**
```bash
pytest tests/user
```
![user tests](../docs/images/tests/user_tests.png)

10) **blockchain**
```bash
pytest tests/blockchain
```
![blockchain contracts tests](../docs/images/tests/blockchain_contracts_tests.png)


![blockchain dataset artifacts tests](../docs/images/tests/blockchain_dataset_artifacts_tests.png)


![blockchain reward artifacts tests](../docs/images/tests/blockchain_reward_artifacts_tests.png)

---


## Run all tests

```bash
pytest
```

---

## Helpful pytest flags

Verbose output:
```bash
pytest -vv
```

Stop after first failure:
```bash
pytest -x
```
