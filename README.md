# olist-dda-etl

## Overview

pt-br :brazil::

Esse projeto faz parte de um teste técnico definido pela bemol. O desafio consiste em construir uma solução de BigData utiizando Spark para comportar os dados do dataset *Brazilian E-Commerce Public Dataset by Olist*, disponível no [Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce).

A ideia principal foi criar uma estrutura de dados automatizada e que pudesse ser utilizada por diferentes setores da empresa, disponibilizando consultas iniciais para a geração de insights focados em dois times: ***Sales*** (Vendas) e ***CS*** (Suporte e Sucesso do Cliente).

### Ferramentas utilizadas

As tecnologias aplicadas foram:

- **Airflow num ambiente Astro**: Para orquestração de dados.
- **Databricks AWS**: Como plataforma de processamento dos dados.
- **SQL Warehouse**: Banco de dados distribuído que compõe o ecossistema Databricks.
- **dbt Core**: Ferramenta para criar pipelines de agregação e formatação final de dados, com o intuito de criar modelos dedicados para diferentes setores da empresa (*Data marts*).
- **Astronomer Cosmos**: Biblioteca para executar projetos dbt no Airflow.

### Arquitetura da Solução

![Alt text](include/images/arch.png?raw=true)

#### Explicação

1. Arquivos CSV são transportados de um diretório local para *delta tables* no SQL Warehouse do ambiente Databricks, num schema '**default**'.
2. Um notebook databricks executa código Spark para transformar, formatar e reindexar os dados, e os resultados são armazenados num schema '**dev**'.
3. Utilizando Astronomer Cosmos, executamos o projeto dbt que agrega e junta os dados relevantes, separando-os em ambientes destinados às áreas de negócio.

#### Arquivos e pastas

Os diretórios e arquivos relevantes estão divididos nas seguintes pastas


```
/
│
├── .astro/
|
├── airflow_settings.yaml/
│
├── dags/
│   │
│   ├── dbt/
│   │   │
│   │   ├── models/
│   │   │   │
│   │   │   ├── cs/
│   │   │   │
│   │   │   ├── sales/
│   │   │   └── sources.yml
│   │   │
│   │   ├── tests/
│   │   │
│   │   ├── dbt_project.yml
│   │   └── profiles.yml
│   │
│   ├── .airflowignore
│
├── include/
│   │
│   ├── notebooks/
│   │
│   └── raw_data/
│       │
│       └── csv/
│
├── .dockerignore
├── .env
├── .gitignore
├── Dockerfile
├── README.md
├── packages.txt
└── requirements.txt
```

### Documentação

A documentação final dos modelos dbt e banco de dados pode ser acessada [aqui](https://samurai-py.github.io/olist-dda-etl/#!/overview)


## Instructions

- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- [Astro CLI](https://docs.astronomer.io/astro/cli/install-cli)
- [Astro Python SDK](https://github.com/astronomer/astro-sdk)

### 1) Install Docker Desktop
Install docker desktop to run airflow locally
```shell
https://www.docker.com/products/docker-desktop/
```

### 2) Install Astro-CLI
Install astro-cli to develop DAGs
```shell
https://github.com/astronomer/astro-cli

curl -sSL install.astronomer.io | sudo bash -s
brew install astro

astro dev init
```

### 3) Add Airflow Connections
Add these configurations into the airflow_settings.yaml file
```yaml
airflow:
  connections: databricks_conn
```

### 4) Setting environment variables
Add these configurations into the .env file
```
AIRFLOW__CORE__TEST_CONNECTION=enabled
PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python
AIRFLOW__ASTRO_SDK__DATABRICKS_CLUSTER_ID=<your-databricks-cluster-id>

DATABRICKS_JOB_ID=<your-databricks-job-id>
```

### 4) Init Airflow Project
Initialize project using the astro-cli
```shell
astro dev start
http://localhost:8080
astro dev restart
```

### 5) Install Libraries for Development
Install the required libraries for the project to develop the DAGs locally
```shell
pip install apache-airflow
pip install astro-sdk-python
pip install dbt-databricks
pip install astronomer-cosmos
```

### 6) Run dags:
Execute the following dags on Airflow UI:

```
1.load_to_databricks
2.process_data
3.create_dbt_models
```

### 7) Generate dbt docs for database and views
Execute these commands in dags/dbt
```shell
dbt generate docs
dbt docs serve --port 4041
```

## Etapas de desenvolvimento

### Definição da arquitetura

Primeiro, a ideia era usar Spark e Google Cloud (Até foram criadas *dags* voltadas ao GCP), mas por causa de problemas na hospedagem do databricks em ambientes Google, optou-se pela AWS. 

A partir daí, criei o ambiente Databricks. Após isso, iniciei um projeto dbt e o movi para a pasta ```dags/dbt```.

### Criação de dags

![Alt text](include/images/dags.png?raw=true)
*Resumo de dags no ambiente*

Com os projetos prontos, desenvolvi a dag ```load_to_databricks.py```, que pega os arquivos **.csv** e os coloca num schema chamado **'default'** no SQL Warehouse do Databricks. Ela tem duas tasks 'dummy' para sinalizar o início e o fim da dag.

Depois de criar o notebook com todo o processamento, implementei tudo na dag ```process_data.py```. Aqui, só três tasks foram necessárias, uma de início, de fim e uma terceira com o ```DatabricksRunNowOperator()```, que executa o job programado para executar nosso notebook databricks.

Por último, com o projeto dbt pronto. a dag ```create_dbt_models.py``` mapeia o profile do nosso ```profiles.yml``` com o operator ```DatabricksTokenProfileMapping()``` e depois executa nosso projeto dbt com o ```DbtTaskGroup()```.

### Notebook databricks

O notebook databricks utilizado se chama [***```process_data.ipynb```***](include/notebooks/process_data.ipynb), e ele executa as seguintes etapas:

1. Visualiza as tabelas como spark dataframes.
2. Como nosso projeto é focado na construção de futuros dashboards e geração de insights, troca-se os ids que estão codificados por índices inteiros, a fim de melhorar a performance das consultas.
3. Tradução dos nomes de colunas
4. Conversão de valores para **timestamp**, na tabela ```reviews```, e também da coluna *pontuacao* para **integer**.
5. Exclusão das colunas **comprimento_nome_produto** e **comprimento_descricao_produto** na tabela ```products```.
6. Substituição dos valores da coluna ```status_pedido```para seus equivalentes em pt-br.
7. Transformação dos dataframes corrigidos e processados em novas tabelas do banco, desta vez para o schema **'dev'**.

### dbt Models

Nosso projeto dbt possui dois modelos: ```Sales ```e ```CS```.

O primeiro tem dados para a criação de dashboards para o monitoramento de dados relevantes ao time de vendas, tendo como resultados 5 tabelas:

- ```clients_per_state```: Clientes por estado.
- ```sales_per_state```: Clientes por estado.
- ```orders_per_client```:  Pedidos por cliente (Para uma recomendação mais personalizada).
- ```sales_per_category```: Vendas por categoria de produto.
- ```reviews_per_product```: Avaliação média por produto.

O modelo de ```CS``` vem com o objetivo de trazer a informação de quais clientes precisam de suporte com mais velocidade, buscando agilizar os processos de suporte ao cliente.

- ```orders_not_shipped```: Visualizar os pedidos não enviados.
- ```reviews_clients```: Avaliações de clientes e produtos nos últimos 30 dias, para o time de suporte.
- ```positive_reviews_30_days```: Filtrando apenas avaliações com nota 4 ou 5.
- ```negative_reviews_30_days```: Filtrando apenas avaliações com nota igual ou inferior a 3.

É possível visualizar a organização final dos modelos no databricks [clicando aqui](include/images/databricks_bd.png)

### Melhorias

1. Adicionar etapas para levar em consideração a inserção de novos dados, com monitoramento de índices.
2. Fazer particionamento por mês (Com o próprio dbt).
3. Criar etapas de testes de dados utilizando **great-expectations** ou **soda**, para dar suporte aos testes do dbt.
4. Deploy da solução na Astronomer.

### Conclusão

Se quiser conhecer outro projeto meu usando airflow e dbt, pode conferir esse [documento](https://github.com/samurai-py/zebrinha-azul-dash-data-app/blob/main/assets/zebra.md).

É isso! :pray:
