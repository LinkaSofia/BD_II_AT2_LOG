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


con = Conectar()

def main():
    arquivo = open('teste1.txt', 'r') 
    #imprimindo para ver se conseguiu abrir o arquivo:
    #print(arquivo.read())
    linhas = []

    for i in arquivo:
        linhas.append(i)
    linhas = limpar(linhas)
    
    criaTblBD()

    linha = 0
    for i in range(len(linhas)):
        if linhas[i] == '':
            linha = i
            inserir(linhas, i)
            break

#tirar os \n, >, < das linhas do arquivo
def limpar(linhas):
    for linha in range(len(linhas)):
        linhas[linha] = re.sub('\n', '', linhas[linha])
        linhas[linha] = re.sub('<', '',linhas[linha])
        linhas[linha] = re.sub('>', '',linhas[linha])
    
    return linhas

def criaTblBD():
    cur = con.cursor()
    cur.execute("create table if not exists tabela (id int not null, A int, B int, primary key(id))")
    con.commit
    print("Tabela criada com sucesso")

#inserir números que vem no começo do arquivo
def inserir(linhas, x):
    cur = con.cursor()
    sql = "truncate table tabela"
    cur.execute(sql)
    inseridos = linhas[0: x]
    for linha in inseridos:
        linha = re.sub('=', ',', linha)
        quebra = linha.split(',')
        sql = "select * from tabela where id = {}".format (quebra[1])
        cur.execute(sql)
        r = cur.fetchall()
        sql = "update tabela set {} = {} where id = {}".format (quebra[0], quebra[2], quebra[1])
        cur.execute(sql)
    con.commit()
    print("\n------\nValores iniciais")
    valorVariaveis()

#Imprime as váriaveis do bd
def valorVariaveis():
    cur = con.cursor()
    sql = "select * from tabela order by id"
    cur.execute(sql)
    # vê se deu certo o select
    r = cur.fetchall()
    #uma biblioteca para a tabela ficar bonita
    table = BeautifulTable()
    #cabeçalhos da tabela
    table.columns.header = ["ID", "A", "B"]
    for i in range (len(r)):
        table.rows.append(r[i])
    print(table)


main()
