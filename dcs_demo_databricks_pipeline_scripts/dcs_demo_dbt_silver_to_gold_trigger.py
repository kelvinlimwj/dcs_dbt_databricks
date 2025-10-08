import requests
import time

DBT_ACCOUNT_ID = "70471823498813"           
DBT_JOB_ID = "70471823515327"             
DBT_API_KEY = "dbtc_0a4t4bgy9DnZZs9ZKKR5DUKDTOhM9U3LhBus4XvpQCBOcAv_pM"    
DBT_BASE_URL = "https://cloud.getdbt.com/api/v2"

trigger_url = f"{DBT_BASE_URL}/accounts/{DBT_ACCOUNT_ID}/jobs/{DBT_JOB_ID}/run/"
headers = {
    "Authorization": f"Token {DBT_API_KEY}",
    "Content-Type": "application/json"
}

print("🔹 Triggering dbt Cloud job...")
resp = requests.post(trigger_url, headers=headers)

if resp.status_code != 200:
    raise Exception(f"❌ Failed to trigger dbt job: {resp.status_code} {resp.text}")

run_data = resp.json()
run_id = run_data["data"]["id"]
print(f"✅ dbt Cloud job triggered successfully! Run ID: {run_id}")


run_status_url = f"{DBT_BASE_URL}/accounts/{DBT_ACCOUNT_ID}/runs/{run_id}/"
poll_interval = 30

print("⏳ Waiting for dbt run to complete...")
while True:
    status_resp = requests.get(run_status_url, headers=headers)
    if status_resp.status_code != 200:
        print("⚠️ Failed to fetch run status, retrying...")
        time.sleep(poll_interval)
        continue

    run_status = status_resp.json()["data"]["status"]
    status_human = {
        1: "Queued",
        2: "Starting",
        3: "Running",
        10: "Success",
        20: "Error",
        30: "Cancelled",
        40: "Skipped",
        50: "Failed"
    }.get(run_status, "Unknown")

    print(f"🔄 Current run status: {status_human}")

    if run_status in [10, 20, 30, 40, 50]:
        break

    time.sleep(poll_interval)

if run_status == 10:
    print(f"dbt run {run_id} completed successfully!")
else:
    raise Exception(f"❌ dbt run {run_id} finished with status: {status_human}")