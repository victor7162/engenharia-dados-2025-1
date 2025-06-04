#app/script.py

import os
import psycopg2
import time

def connect_to_db():
    db_host = os.getenv("DB_HOST", "dbpg") # 'db' e o nome do servico PostgreSQL no docker-compose
    db_name = os.getenv("DB_NAME", "mydatabase")
    db_user = os.getenv("DB_USER", "user")
    db_password = os.getenv("DB_PASSWORD", "password")
    
    conn_string = f"host='{db_host}' dbname='{db_name}' user='{db_user}' password='{db_password}'"
    
    max_retries = 10
    retries = 0
    conn = None

    while retries < max_retries:
        try:
            print(f"Tentando conectar ao banco de dados ({retries+1}/{max_retries})...")
            conn = psycopg2.connect(conn_string)
            print("Conexao com PostgreSQL bem-sucedida!")
            conn.close()
            return True
        except psycopg2.OperationalError as e:
            print(f"Erro ao conectar: {e}")
            retries += 1
            time.sleep(5) # Espera 5 segundos antes de tentar novamente
    print("Nao foi possivel conectar ao banco de dados apos varias tentativas.")
    return False

if __name__ == "__main__":
    connect_to_db()

