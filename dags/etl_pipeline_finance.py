# Importa o objeto DAG do Airflow
from airflow import DAG

# Importa operador para executar funções Python como tarefas no Airflow
from airflow.operators.python import PythonOperator

# Importa datetime para definir a data de início do DAG
from datetime import datetime

# Biblioteca para baixar dados financeiros (Yahoo Finance)
import yfinance as yf

# Biblioteca para manipulação de dados (DataFrames)
import pandas as pd

# Biblioteca para interagir com S3 (ou MinIO)
import boto3

# -----------------------------
# Função para extrair dados financeiros
# -----------------------------
def extract_finance_data():
    # Lista de ativos/tickers a serem baixados
    tickers = ["PETR4.SA", "VALE3.SA", "ITUB4.SA", "^BVSP", "BTC-USD"]
    
    # Dicionário para armazenar os DataFrames
    data = {}
    
    # Loop sobre cada ticker
    for ticker in tickers:
        # Baixa dados históricos de 5 anos, com frequência diária
        df = yf.download(ticker, period="5y", interval="1d")
        
        # Reseta o índice para que a coluna 'Date' volte a ser coluna normal
        df.reset_index(inplace=True)
        
        # Armazena o DataFrame no dicionário (opcional, não usado depois)
        data[ticker] = df
        
        # Salva cada DataFrame em CSV temporário local, removendo '^' do nome
        df.to_csv(f"/tmp/{ticker.replace('^','')}.csv", index=False)

# -----------------------------
# Função para carregar dados para MinIO/S3
# -----------------------------
def load_to_s3():
    # Cria cliente S3 apontando para o MinIO local
    s3 = boto3.client(
        's3',
        endpoint_url="http://minio:9000",   # URL do MinIO
        aws_access_key_id="minio",          # Usuário
        aws_secret_access_key="minio123"    # Senha
    )

    # Lista de buckets que vamos usar
    buckets = ["raw", "curated"]
    
    # Criar buckets caso não existam
    for bucket in buckets:
        try:
            s3.head_bucket(Bucket=bucket)
        except:
            s3.create_bucket(Bucket=bucket)
    
    # Mesma lista de tickers
    tickers = ["PETR4.SA", "VALE3.SA", "ITUB4.SA", "^BVSP", "BTC-USD"]
    
    # Loop sobre cada ticker para enviar para o bucket
    for ticker in tickers:
        file = f"/tmp/{ticker.replace('^','')}.csv"  # Arquivo temporário
        # Faz upload para o bucket "raw" na pasta "finance"
        s3.upload_file(file, "raw", f"finance/{ticker.replace('^','')}.csv")

# -----------------------------
# Função para transformar dados financeiros
# -----------------------------
def transform_finance_data():
    # Lista de tickers
    tickers = ["PETR4.SA", "VALE3.SA", "ITUB4.SA", "^BVSP", "BTC-USD"]
    
    # Cria cliente S3 para enviar os dados transformados
    s3 = boto3.client(
        's3',
        endpoint_url="http://minio:9000",
        aws_access_key_id="minio",
        aws_secret_access_key="minio123"
    )
    
    # Loop para processar cada ticker
    for ticker in tickers:
        # Lê CSV temporário
        df = pd.read_csv(f"/tmp/{ticker.replace('^','')}.csv")

        # Converte para float, valores inválidos viram NaN
        df["Close"] = pd.to_numeric(df["Close"], errors="coerce")
        df = df.dropna(subset=["Close"])
        
        # Calcula o retorno diário percentual
        df["Return"] = df["Close"].pct_change()
        df["Return"].fillna(0, inplace=True)  # substitui NaN por 0
        
        # Calcula média móvel de 7 dias
        df["MA7"] = df["Close"].rolling(window=7).mean()
        
        # Calcula média móvel de 30 dias
        df["MA30"] = df["Close"].rolling(window=30).mean()
        
        # Calcula volatilidade de 30 dias (desvio padrão do retorno)
        df["Volatility"] = df["Return"].rolling(window=30).std().fillna(0)
        
        # Nome do arquivo transformado
        file_curated = f"/tmp/{ticker.replace('^','')}_curated.csv"
        
        # Salva CSV transformado localmente
        df.to_csv(file_curated, index=False)
        
        # Envia CSV transformado para o bucket "curated" no MinIO
        s3.upload_file(file_curated, "curated", f"finance/{ticker.replace('^','')}_curated.csv")

# -----------------------------
# Definição da DAG
# -----------------------------
with DAG(
    "etl_pipeline_finance",           # Nome do DAG
    start_date=datetime(2025,1,1),   # Data de início do DAG
    schedule_interval="@daily",       # Frequência: diariamente
    catchup=False                     # Não executar backfill para datas passadas
) as dag:

    # Tarefas da DAG, usando PythonOperator
    extract = PythonOperator(
        task_id="extract",              # ID da tarefa
        python_callable=extract_finance_data  # Função a ser chamada
    )
    
    load = PythonOperator(
        task_id="load",
        python_callable=load_to_s3
    )
    
    transform = PythonOperator(
        task_id="transform",
        python_callable=transform_finance_data
    )

    # Define a ordem das tarefas (dependências)
    extract >> load >> transform
    # Significa: primeiro extract, depois load, depois transform
