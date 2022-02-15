import psycopg2
from beautifultable import BeautifulTable
import re


def Conectar():
    try:
        conexao = psycopg2.connect(database= 'doredo',
                                    host = 'localhost',
                                    user = 'postgres',
                                    password = 'senha')
        print("Conectou!")
        return conexao     
    except psycopg2.DatabaseError as e:
        print("Erro ao conectar o banco:", e)
        
        return None

    con = Conectar()
con.autocommit = False

def compararValores(linhas, t):
    print("-- Transação --", t)
    for linha in linhas:
        
        if t in linha and 'start' not in linha and 'commit' not in linha and 'CKPT' not in linha:
         quebra = linha.split(',')
            id = quebra[1]
            letra = quebra[2]
            valor = quebra[3]
            cur = con.cursor()
            sql = "select {} from tabela where id = {}".format(letra, id)
            cur.execute(sql)
            r = cur.fetchall()
            var = r[0][0]
            if var != valor:
                sql = "update tabela set {} = {} where id = {}".format(letra, valor, id)
                cur.execute(sql)
                sql = "{},{} atualizado para {}".format(letra, id, valor)
                print(sql)
                con.commit()
                
                

def redo(linhas, linhasCkpt, endCKPT, StartCkpt = 0):

    for i in range(len(linhasCkpt)-1, 0, -1):
        if 'commit' in linhasCkpt[i]:
            comitado.append(linhasCkpt[i].split()[1])
        if 'start' in linhasCkpt[i]:
            transacaoAberta.append(linhasCkpt[i].split()[1])
        
    if endCKPT:
        res = re.findall(r'\(.*?\)', linhas[StartCkpt])
        res = "".join([x for x in res[0] if x != '(' and x != ")"])
        print(res)
        [transacaoAberta.append(x) for x in res.split(',')]

    comitado.reverse()
    for t in comitado:
        compararValores(linhas, t)
        

def limpar(linhas):
    for linha in range(len(linhas)):
        linhas[linha] = re.sub('\n', '',  linhas[linha])
        linhas[linha] = re.sub('\n', '',  linhas[linha])
        linhas[linha] = re.sub('<', '',linhas[linha])
        linhas[linha] = re.sub('>', '',linhas[linha])
    
    return linhas

def valorVariaveis():
    cur = con.cursor()
    sql = "select * from tabela order by id"
    cur.execute(sql)
    r = cur.fetchall()

    table = BeautifulTable()
    table.columns.header = ["ID", "A", "B"]

    for i in range (len(r)):
        table.rows.append(r[i])

    print(table)

    
def inserir( linhas, x):
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
        if r:
            sql = "update tabela set {} = {} where id = {}".format (quebra[0], quebra[2], quebra[1])
            cur.execute(sql)
        else:
            sql = "insert into tabela (id, {}) valors ({}, {})".format (quebra[0], quebra[1], quebra[2])
            cur.execute(sql)

    con.commit()
    print("\n------\nValores iniciais")
    valorVariaveis()

def encontrarCkpt(linhas):
    start = 0
    end = False
    for i, linha in enumerate(linhas):
        if 'Start CKPT' in linha:
            start = i 
        if 'End CKPT' in linha:
            end = True
    return start, end

def main():
    arquivo = open('arquivo.txt', 'r') 
    linhas = []

    for i in arquivo:
        linhas.append(i)
    
    linhas = limpar(linhas)
    linha = 0
    for i in range(len(linhas)):
        if linhas[i] == '':
            linha = i
            inserir(linhas, i)
            break
    
    linhas = linhas[linha+1::]
    startCkpt, endCkpt = encontrarCkpt(linhas)

    if endCkpt == False:
        print("CKPT não foi finalizado")
        linhasCkpt = linhas
        redo(linhas, linhasCkpt, endCkpt, startCkpt)
    else:
        linhasCkpt = linhas[startCkpt::]
        redo(linhas, linhasCkpt, endCkpt, startCkpt)

    print("\n")

    for i in transacaoAberta:
        if i in comitado:
            print(i, 'fez REDO')
        else:
            print(i, 'não fez REDO')

    valorVariaveis()


main()

con.close()
