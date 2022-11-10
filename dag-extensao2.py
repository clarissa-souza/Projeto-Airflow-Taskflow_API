import os
import zipfile
import requests
import pandas as pd
from numpy import NaN
from datetime import datetime, timedelta
from airflow import DAG 
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

#cwd = os.getcwd()
cwd = 'gs://projeto-raizen/'

def extracao_2018():
  os.makedirs (cwd + 'dadosOriginais',exist_ok = True)
  url='https://download.inep.gov.br/microdados/microdados_censo_da_educacao_superior_2018.zip'
  response = requests.get(url, verify=False).content
  open(cwd + 'dadosOriginais/microdados_censo_da_educacao_superior_2018.zip','wb').write(response)

def extracao_2019():
  os.makedirs (cwd +'dadosOriginais',exist_ok = True)
  url='https://download.inep.gov.br/microdados/microdados_censo_da_educacao_superior_2019.zip'
  response = requests.get(url, verify=False).content
  open(cwd + 'dadosOriginais/microdados_censo_da_educacao_superior_2019.zip','wb').write(response)

def extracao_2020():
  os.makedirs (cwd + 'dadosOriginais',exist_ok = True)
  url='https://download.inep.gov.br/microdados/microdados_censo_da_educacao_superior_2020.zip'
  response = requests.get(url, verify=False).content
  open(cwd + 'dadosOriginais/microdados_censo_da_educacao_superior_2020.zip','wb').write(response)

def extracao_2021():
  os.makedirs (cwd + 'dadosOriginais',exist_ok = True)
  url='https://download.inep.gov.br/microdados/microdados_censo_da_educacao_superior_2021.zip'
  response = requests.get(url, verify=False).content
  open(cwd + 'dadosOriginais/microdados_censo_da_educacao_superior_2021.zip','wb').write(response)

def tratamento():
  columns = ['NU_ANO_CENSO','NO_REGIAO','SG_UF','TP_GRAU_ACADEMICO','QT_VG_TOTAL','QT_INSCRITO_TOTAL']
  with zipfile.ZipFile ('dadosOriginais/microdados_censo_da_educacao_superior_2018.zip') as z:
    with z.open('Microdados do Censo da Educaç╞o Superior 2018/dados/MICRODADOS_CADASTRO_CURSOS_2018.CSV') as f:
      df=pd.read_csv(f,encoding='ISO-8859-1',sep=';', usecols=columns)
  with zipfile.ZipFile ('dadosOriginais/microdados_censo_da_educacao_superior_2019.zip') as z:
    with z.open('Microdados do Censo da Educaç╞o Superior 2019/dados/MICRODADOS_CADASTRO_CURSOS_2019.CSV') as f:
      df1=pd.read_csv(f,encoding='ISO-8859-1',sep=';', usecols=columns)
  df = pd.concat([df, df1])
  with zipfile.ZipFile (cwd + 'dadosOriginais/microdados_censo_da_educacao_superior_2020.zip') as z:
    with z.open('Microdados do Censo da Educaç╞o Superior 2020/dados/MICRODADOS_CADASTRO_CURSOS_2020.CSV') as f:
      df1=pd.read_csv(f,encoding='ISO-8859-1',sep=';', usecols=columns)
  df = pd.concat([df, df1])
  with zipfile.ZipFile (cwd + 'dadosOriginais/microdados_censo_da_educacao_superior_2021.zip') as z:
    with z.open('Microdados do Censo da Educaç╞o Superior 2021/dados/MICRODADOS_CADASTRO_CURSOS_2021.CSV') as f:
      df1=pd.read_csv(f,encoding='ISO-8859-1',sep=';', usecols=columns)
  df = pd.concat([df, df1])
  df = df.groupby(['NU_ANO_CENSO','NO_REGIAO','SG_UF','TP_GRAU_ACADEMICO']).sum()
  df.rename(columns={'NU_ANO_CENSO':'ano','NO_REGIAO':'regiao','SG_UF':'UF','TP_GRAU_ACADEMICO':'grau_academico','QT_VG_TOTAL':'vagas_totais','QT_INSCRITO_TOTAL':'inscritos_totais'},inplace=True)
  return df

def grava_dados(**kwargs):
  ti=kwargs['ti']
  df=ti.xcom_pull(task_ids='tratamento')
  os.makedirs (cwd + 'dadosTratados',exist_ok = True)
  df.to_json(cwd + 'dadosTratados/Censo_regiao.json',orient = 'table')
  df.to_csv(cwd + 'dadosTratados/Censo_regiao.csv')

default_args = {
        'owner': 'Clarissa Souza',
        'depends_on_past': False,
        'email': ['clarissasouza950@gmail.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=1)
      }

with DAG('extensao_airflow_2', start_date = datetime(2022,11,8),schedule_interval='30 * * * *', catchup=False, default_args=default_args) as dag:

  begin = DummyOperator(
      task_id='begin'
  )

  extracao_2018 = PythonOperator(
      task_id='extracao_2018',
      python_callable = extracao_2018
  )

  extracao_2019 = PythonOperator(
      task_id='extracao_2019',
      python_callable = extracao_2019
  )

  extracao_2020 = PythonOperator(
      task_id='extracao_2020',
      python_callable = extracao_2020
  )

  extracao_2021 = PythonOperator(
      task_id='extracao_2021',
      python_callable = extracao_2021
  )

  tratamento = PythonOperator(
      task_id='tratamento',
      python_callable = tratamento
  )

  grava_dados = PythonOperator(
      task_id='grava_dados',
      provide_context=True,
      python_callable = grava_dados
  )

  end = DummyOperator(
      task_id='end'
  )

  begin >> extracao_2018 >> extracao_2019 >> extracao_2020 >> extracao_2021 >> tratamento >> grava_dados >> end