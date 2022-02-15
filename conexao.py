from sqlite3 import connect
import psycopg2

class Conexao:

    def __init__(self, banco, usuario, senha, hospedeiro, porta):
        self.banco = banco
        self.usuario = usuario
        self.senha = senha
        self.hospedeiro = hospedeiro
        self.porta = porta

    def Conexao(self):
        try:
            conexao = psycopg2.connect(database= self.banco,
                                user = self.usuario, 
                                password = self.senha, 
                                host = self.hospedeiro, 
                                port = self.porta)
            print("Conectou!")
            return conexao     
        except psycopg2.DatabaseError as e:
            print("Erro ao conectar o banco:", e)
            
            return None