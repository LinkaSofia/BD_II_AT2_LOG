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
    arquivo = open('arquivo.txt', 'r') 
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
        if r:
            sql = "update tabela set {} = {} where id = {}".format(quebra[0], quebra[2], quebra[1])
            cur.execute(sql)
        else:
            sql = "insert into tabela (id, {}) values ({}, {})".format(quebra[0], quebra[1], quebra[2])
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
    print(r)
    #uma biblioteca para a tabela ficar bonita
    table = BeautifulTable()
    #cabeçalhos da tabela
    table.columns.header = ["ID", "A", "B"]
    for i in range (len(r)):
        table.rows.append(r[i])
    print(table)

#Vai linha a linha buscando a palavra Start CKPT ou END CKPT
def encontrarCkpt(linhas):
    start = 0
    end = False
    for i, linha in enumerate(linhas):
        if 'Start CKPT' in linha:
            start = i 
        if 'End CKPT' in linha:
            end = True
    return start, end

def redo(linhas, linhasCkpt, endCKPT, StartCkpt = 0):
    #pega o "tamanho" das linhasCkpt e vê de tras para frente o que tem commit
    for i in range(len(linhasCkpt)-1, 0, -1):
        # se tem commit ele insere na tabela dos comitados
        if 'commit' in linhasCkpt[i]:
            comitado.append(linhasCkpt[i].split()[1])
        if 'start' in linhasCkpt[i]:
            #se tiver o start nas linhasCKPT ele coloca essa linhas nas transações que começaram
            transacaoAberta.append(linhasCkpt[i].split()[1])
    
    #se endCKPT = true
    #se tem "( e )" é pq tem transação aberta quando eu inicio o startCKPT, então ele coloca essa transação dentro de transaçõesAbertas
    if endCKPT:
        res = re.findall(r'\(.*?\)', linhas[StartCkpt])
        res = "".join([x for x in res[0] if x != '(' and x != ")"])
        print(res)
        [transacaoAberta.append(x) for x in res.split(',')]

    #começa as transações ao contrário, como é feito no REDO
    comitado.reverse()
    #compara para ver se já foi alterado o valor no bd
    for t in comitado:
        compararValores(linhas, t)

# faz o REDO se ainda não foi feito
def compararValores(linhas, t):
    print("Transação: ", t)
    #para linha (indices) em linhas (linhas do arquivo)
    for linha in linhas:
        # t é as linhas comitadas
        # se t estiver na linha e start, commit e checkpoint não, então
        if t in linha and 'start' not in linha and 'commit' not in linha and 'CKPT' not in linha:
            quebra = linha.split(',')
            id = quebra[1]
            letra = quebra[2]
            valor = quebra[3]
            #mesma biblioteca que usamos para abrir o arquivo, serve para dar os comandos em sql
            cur = con.cursor()
            sql = "select {} from tabela where id = {}".format(letra, id)
            cur.execute(sql)
            # vê se deu certo o select
            r = cur.fetchall()
            var = r[0][0]
            if var != valor:
                sql = "update tabela set {} = {} where id = {}".format(letra, valor, id)
                cur.execute(sql)
                sql = "{}, id {} atualizado para {}".format(letra, id, valor)
                print(sql)
                con.commit()

main()
