# as funções que serão utilizadas no Flow.

from io import StringIO

import pandas as pd
from prefect import task
import requests

import psycopg2
from psycopg2 import sql

from utils import log

# Define the connection parameters
DB_NAME = "dadosAbertosTOF"
DB_USER = "cliente"
DB_PASSWORD = "your_password"  # replace with your actual password
DB_HOST = "localhost"
DB_PORT = "5432"

@task
def download_data(n_users: int) -> str:
    """
    Baixa dados da API https://randomuser.me e retorna um texto em formato CSV.

    Args:
        n_users (int): número de usuários a serem baixados.

    Returns:
        str: texto em formato CSV.
    """
    response = requests.get(
        "https://dados.mobilidade.rio/gps/brt".format(n_users)
    )
    log("Dados baixados com sucesso!")
    return response.text

@task
def parse_data(data: str) -> pd.DataFrame:
    """
    Transforma os dados em formato CSV em um DataFrame do Pandas, para facilitar sua manipulação.

    Args:
        data (str): texto em formato CSV.

    Returns:
        pd.DataFrame: DataFrame do Pandas.
    """
    df = pd.read_csv(StringIO(data))
    log("Dados convertidos em DataFrame com sucesso!")
    return df

@task
def save_report(dataframe: pd.DataFrame) -> None:
    """
    Salva o DataFrame em um arquivo CSV.

    Args:
        dataframe (pd.DataFrame): DataFrame do Pandas.
    """
    dataframe.to_csv("report.csv", index=False)
    log("Dados salvos em report.csv com sucesso!")

@task
def connect_to_db():
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        print("Connection successful")
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        raise

@task
def execute_query(conn):
    try:
        cursor = conn.cursor()
        query = sql.SQL("SELECT * FROM your_table_name")  # replace with your actual query
        cursor.execute(query)
        results = cursor.fetchall()
        print("Query executed successfully")
        return results
    except Exception as e:
        print(f"Error executing query: {e}")
        raise
    finally:
        cursor.close()
        conn.close()