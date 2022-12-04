import psycopg2

#Fazer conexao com o BD e executar comando
def connectExecuteDatabaseOperation(sql):
    try: 

        # Conecta na base de dados
        connection = psycopg2.connect(user="postgres",
                                    password="postgres",
                                    host="127.0.0.1",
                                    database="enfe")

        #Cria um cursor para executar comandos
        cursor = connection.cursor()

        #Executa o comando passado
        cursor.execute(sql)

        #Retorna resultado
        result = cursor.fetchall()

    except (Exception, Error) as error:
        print("Erro ao conectar com a base")
    finally:
        if (connection):
            cursor.close()
            connection.close()
            return result