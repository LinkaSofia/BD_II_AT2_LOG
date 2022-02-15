import psycopg2


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
