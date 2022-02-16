import psycopg2
from beautifultable import BeautifulTable
import re

comitado = []
transacaoAberta = []

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
    arquivo = open('teste1.txt', 'r') 
    #imprimindo para ver se conseguiu abrir o arquivo:
    #print(arquivo.read())
    linhas = []

    for i in arquivo:
        linhas.append(i)
    linhas = limpar(linhas)
    print(linhas)

#tirar os \n, >, < do arquivo
def limpar(linhas):
    for linha in range(len(linhas)):
        linhas[linha] = re.sub('\n', '', linhas[linha])
        linhas[linha] = re.sub('<', '',linhas[linha])
        linhas[linha] = re.sub('>', '',linhas[linha])
    
    return linhas

main()
