import psycopg2

#Fazer conexao com o BD e executar comando
def connectExecuteDatabaseOperation(sql, type):
    try: 
        #Conecta na base de dados
        connection = psycopg2.connect(user="postgres",
                                      password="postgres",
                                      host="127.0.0.1",
                                      port="5432",
                                      database="enfe")

        #Define commit como acao padrao ao final da conexao
        connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)                              

        #Cria um cursor para executar comandos
        cursor = connection.cursor()

        #Executa o comando passado
        cursor.execute(sql)

        #Retorna resultado
        if (type == 1):
            result = cursor.fetchall()
    except (Exception, psycopg2.Error) as error:
        print(error)
        exit()
    else:
        if (connection):
            cursor.close()
            connection.close()
        if (type == 1):
            return result
        return 1

def execInsert(sql, table):
    command = 'INSERT INTO '+table+' (id,a,b) VALUES ' + sql
    # print(command)
    connectExecuteDatabaseOperation(command, 0)

def execUpdate(sql, condition, table):
    command = 'UPDATE '+table+' SET '+sql+' WHERE '+condition
    # print(command)
    connectExecuteDatabaseOperation(command, 0)    

