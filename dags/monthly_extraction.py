from datetime import datetime
from airflow.providers.standard.operators.python import PythonOperator
from airflow import DAG
import requests, json, time, os

def fetch_uncomtrade_data(flow_code, flow_name, **kwargs):
    current_year = datetime.now().year 
    years_to_fetch = [current_year]

    headers = {
        "Ocp-Apim-Subscription-Key": "0111bba81dba4693a82c31dcb7c4490a"
    }

    os.makedirs("raw_data", exist_ok=True)

    for year in years_to_fetch:
        output_path = f"raw_data/uncomtrade_{flow_name}_{year}.json"

        url = "https://comtradeapi.un.org/data/v1/get/C/A/HS"
        params = {
            "reporterCode": "818",      
            "period": year,
            "cmdCode": "ALL",
            "flowCode": flow_code,      
            "includeDesc": "true"
        }

        response = requests.get(url, headers=headers, params=params)
        data = response.json()

        if "data" not in data: 
            print(f"No data found for {flow_name} in {year}") 
            continue

        filtered_data = [
            {
                "Year": item["refYear"],
                "Flow_Description": item["flowDesc"],
                "Partner_Code": item["partnerCode"],
                "Partner_ISO": item["partnerISO"],
                "Partner_Country": item["partnerDesc"],
                "Traded_Commodities": item["cmdDesc"],
                "Trade_Value": item["primaryValue"],
                "WeightofTradedGoods": item.get("grossWgt")
            }
            for item in data["data"]
        ]

        # Save merged file
        with open(output_path, "w") as f:
            json.dump(filtered_data, f, indent=2)

        print(f"Updated {flow_name} data for {year} (old data preserved).")
        time.sleep(2)


def fetch_worldbank_data(**kwargs):
    current_year = datetime.now().year

    os.makedirs("raw_data", exist_ok=True)

    output_path = "raw_data/worldbank_indicators_2020-2024.json"

    # Load existing data if file exists
    if os.path.exists(output_path):
        with open(output_path, "r") as f:
            existing_data = json.load(f)
    else:
        existing_data = []

    url = "https://data360api.worldbank.org/data360/data?DATABASE_ID=WB_WDI&INDICATOR=WB_WDI_NY_GDP_MKTP_CD,WB_WDI_NY_GDP_MKTP_KD_ZG,WB_WDI_FP_CPI_TOTL_ZG,WB_WDI_NE_EXP_GNFS_ZS,WB_WDI_NE_IMP_GNFS_ZS,WB_WDI_DT_DOD_DECT_GN_ZS,WB_WDI_FI_RES_TOTL_MO&REF_AREA=EGY&skip=0"
    response = requests.get(url)
    data = response.json()

    filtered_data = [
        {
            "Year": item["TIME_PERIOD"],
            "Indicator_Code": item["INDICATOR"],
            "Indicator_Value": item["OBS_VALUE"],
            "Description": item["COMMENT_TS"]
        }
        for item in data["value"]
        if int(item["TIME_PERIOD"]) >= current_year - 1
    ]

    
    # Remove any existing entry for the same year
    existing_data = [d for d in existing_data if d["Year"] != str(current_year)]

    # Add new data
    updated_data = existing_data + filtered_data

    # Save merged dataset
    with open(output_path, "w") as f:
        json.dump(updated_data, f, indent=2)

    print(f"World Bank data updated with {current_year} values (old years preserved).")


with DAG(
    dag_id='monthly_data_extraction',
    start_date=datetime(2025,9,28),
    schedule="@monthly",
    catchup=False, 
    default_args={'owner':'data team'},
    description="Automates UN Comtrade & World Bank data extraction", 

) as dag: 
    extract_UNimports = PythonOperator( 
        task_id="extract_uncomtrade_imports", 
        python_callable=fetch_uncomtrade_data, 
        op_kwargs={"flow_code": "M", "flow_name": "imports"}, 
    ) 
    
    extract_UNexports = PythonOperator( 
        task_id="extract_uncomtrade_exports", 
        python_callable=fetch_uncomtrade_data, 
        op_kwargs={"flow_code": "X", "flow_name": "exports"}, 
    ) 
    
    extract_worldbank = PythonOperator( 
        task_id="extract_worldbank_indicators", 
        python_callable=fetch_worldbank_data, 
    ) 

    [extract_UNimports, extract_UNexports] >> extract_worldbank
