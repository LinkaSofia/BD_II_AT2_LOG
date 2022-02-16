import psycopg2
from beautifultable import BeautifulTable
import re

# conexão 
def Conectar():
    try:
        conexao = psycopg2.connect(database= 'doredo',
                                    host = 'localhost',
                                    user = 'postgres',
                                    password = '123456')
        print("Conectou!")
        return conexao     
    except psycopg2.DatabaseError as e:
        print("Erro ao conectar o banco:", e)
        return None

def main():
    arquivo = open('arquivo.txt', 'r') 
    #imprimindo para ver se conseguiu abrir o arquivo:
    print(arquivo.read())

main()
