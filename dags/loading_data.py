from airflow.providers.standard.operators.python import PythonOperator
from airflow import DAG
import os,sys
from datetime import datetime , timedelta
from scripts import load_world_bank_data, load_uncomtrade_data

with DAG(
    dag_id='Loading_Trade_and_Economic_Data',
    start_date=datetime(2025,9,29),
    schedule="@monthly",
    catchup=False, 
    default_args={'owner':'data team'},
    description="Automates UN Comtrade & World Bank data Loading", 

) as dag: 
    load_UNcom = PythonOperator( 
        task_id="load_uncomtrade", 
        python_callable=load_world_bank_data, 
    ) 
    
    load_worldbank = PythonOperator( 
        task_id="load_worldbank_indicators", 
        python_callable=load_uncomtrade_data, 
    ) 

    load_worldbank >> load_UNcom