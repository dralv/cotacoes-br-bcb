from datetime import datetime
from airflow import DAG

from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

import pandas as pd
import requests
import logging
import pendulum


from io import StringIO

dag = DAG(
    'fin_cotacoes_bcb_classic',
    schedule_interval = '@daily',
    default_args = {
      'owner': 'airflow',
      'retries': 1,
      'start_date': datetime(2024, 11, 1)
    },
    catchup = False,
    tags = ["bcb"]
)

def extract():
    yesterday = pendulum.yesterday("America/Sao_Paulo").format("YYYYMMDD")
    base_url = "https://www4.bcb.gov.br/Download/fechamento/"
    full_url = base_url + yesterday + ".csv"
    logging.warning(full_url)
  
    try:
      respose = requests.get(full_url)
      if respose.status_code == 200:
        csv_data = respose.content.decode('utf-8')
        return csv_data
    except Exception as ex:
      logging.error(ex)
          
extract_task = PythonOperator(
    task_id = 'extract',
    python_callable = extract,
    provide_context = True,
    dag = dag
)

def transform(**kwargs):
    #ti = task instance
    cotacoes =  kwargs['ti'].xcom_pull(task_ids = 'extract')
    csvStringIO = StringIO(cotacoes)
    
    column_names = [
        "DT_FECHAMENTO",
        "COD_MOEDA",
        "TIPO_MOEDA",
        "DESC_MOEDA",
        "TAXA_COMPRA",
        "TAXA_VENDA",
        "PARIDADE_COMPRA",
        "PARIDADE_VENDA"
    ]
    
    data_types = {
        "DT_FECHAMENTO" : str,
        "COD_MOEDA" : str,
        "TIPO_MOEDA" : str,
        "DESC_MOEDA" : str,
        "TAXA_COMPRA" : float,
        "TAXA_VENDA" : float,
        "PARIDADE_COMPRA" : float,
        "PARIDADE_VENDA" : float
    }
    
    parse_dates = ["DT_FECHAMENTO"]
    
    df = pd.read_csv(
        csvStringIO,
        sep = ";",
        decimal = ",",
        thousands = ".",
        encoding = "utf-8",
        header = None,
        names = column_names,
        dtype = data_types,
        parse_dates = parse_dates
    )
    
    df['DT_PROCESSAMENTO'] = pendulum.now()
    logging.info(f"{df.head()}")
    return df

transform_task = PythonOperator(
    task_id = 'transform',
    python_callable = transform,
    provide_context = True,
    dag = dag
)

def create_table():
    create_table_ddl = """
        CREATE TABLE IF NOT EXISTS cotacoes (
          dt_fechamento DATE,
          cod_moeda TEXT,
          tipo_moeda TEXT,
          desc_moeda TEXT,
          taxa_compra REAL,
          taxa_venda REAL,
          paridade_compra REAL,
          paridade_venda REAL,
          dt_processamento TIMESTAMP,
          CONSTRAINT table_pk PRIMARY KEY (dt_fechamento, cod_moeda)
        )
    """
    return create_table_ddl

create_table_postgres_task = PostgresOperator(
    task_id = "create_table_postgres",
    postgres_conn_id = "postgres_astro",
    sql = create_table(),
    dag = dag
)

def load(**kwargs):
    cotacoes_df = kwargs['ti'].xcom_pull(task_ids = 'transform')
    table_name = "cotacoes"
    
    postgres_hook = PostgresHook(postgres_conn_id = "postgres_astro", schema = "astro")
    
    rows = list(cotacoes_df.itertuples(index = False))
    
    #realiza um upsert
    postgres_hook.insert_rows(
        table_name,
        rows,
        replace = True,
        replace_index = ["DT_FECHAMENTO", "COD_MOEDA"],
        target_fields = ["DT_FECHAMENTO", "COD_MOEDA", "TIPO_MOEDA", "DESC_MOEDA", "TAXA_COMPRA","TAXA_VENDA", "PARIDADE_COMPRA", "PARIDADE_VENDA","DT_PROCESSAMENTO"]
    )
    
load_task = PythonOperator(
    task_id = 'load',
    python_callable = load,
    provide_context = True,
    dag = dag
)

extract_task >> transform_task >> create_table_postgres_task >> load_task