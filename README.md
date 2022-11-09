# Projeto-Extensao-Airflow

Projeto para conclusão do curso de extensão de Airflow da SoulCode https://soulcodeacademy.org/

Objetivo: Fazer o processo de ETL na DAG do Airflow
  1 - Fazer a extração dos dados para uma pasta local
  2 - Fazer o tratamento dos dados na própria DAG
  3 - Gravar o arquivo tratado em json e em csv
  4 - Executar o Airflow a partir de um container local
  5 - Executar o Airflow na plataforma Google Cloud 

Dataset escolhido: Dados do Censo escolar do INEP dos anos de 2018, 2019, 2020 e 2021.
  1 - Os arquivos originais podem ser obtidos em: https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados/censo-escolar
  2 - Motivação: Ilustrar a extração de colunas específicas de um arquivo compactado. Nesse caso não será necessário descompactar o arquivo na máquina local. 
  
Escopo: No final do tratamento teremos dois arquivos gravasdos na pasta local (um no formato CSV e outro no formato json). O arquivo deve ser no formato tabela e conter as seguintes colunas: Ano, Regiao, UF, Grau Escolaridade, Quantidade Total de Vagas, Quantidade Total de Inscritos.
