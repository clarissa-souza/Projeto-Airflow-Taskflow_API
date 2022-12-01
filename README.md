# Projeto-Airflow-Taskflow API para o Censo Escolar dos anos de 2018, 2019, 2020 e 2021

Escopo do projeto: Extrair do Censo Escolar do período especificado os dados de Totais de Vagas e totais de Inscritos por Ano, Região, UF e Grau Acadêmico. 
    Os arquivos originais podem ser obtidos em: https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados/censo-escolar

Objetivo do Projeto: Fazer o processo de ETL utilizando o Taskflow API do Airflow

    1 - Fazer o download dos dados para uma pasta local
  
    2 - Fazer o tratamento dos dados na própria DAG
  
    3 - Gravar o arquivo tratado em json
  
    4 - Executar o Airflow a partir de um container 
  

Execução do projeto:

    1 - Preparação do ambiente: como o projeto exige que seja gravado o arquivo original e tratado em uma pasta local, foi necessário  
      criar os volumes do docker-compose.yaml
        - ./dadosOriginais:/opt/airflow/dadosOriginais
        - ./dadosTratados:/opt/airflow/dadosTratados
    
    2 - Os arquivos originais estão compactados então foi necessário verificar qual arquivo tem as informações do projeto. O objetivo  
      é não ter que descompactar todos os arquivos na pasta local. Vou utilizar apenas o arquivo que tem a informação que preciso.  
      Este processo pode ser verificado no arquivo Extensao_airflow.ipynb
      
    3 - Depois de identificado o arquivo, selecionei apenas as colunas que foram solicitadas para compor o arquivo final.  
      Este processo pode ser verificado no arquivo Extensao_airflow.ipynb
    
    4 - O processo de ETL na Dag pode ser verificado no arquivo Airflow_taskflow_api.ipynb
    
