import json
import dbConnectExec
import re


# Possivel implementar identificador de argumentos na execucao do programa
# Ex: caminho dos arquivos e informacoes a respeito da base de dados
# Retorno final em json? 
# Separar em objetos? funcoes?

# ====== INSERINDO DADOS NO BANCO ===== #
# ---- utilizando o metadados.json ---- #
# ===================================== #

dbConnectExec.connectExecuteDatabaseOperation("TRUNCATE initial;", 0)

with open("metadado.json","r") as f: 
    dados = json.load(f) # Pegando os dados do arquivo
f.close()

dados = dados["INITIAL"]
sql = "" 

for x in range(len(dados['A'])):
    #Aqui ira o comando para inserir no BD
    print("INSERT INTO initial (id,a,b) VALUES ("+str(x+1)+","+str((dados['A'][x]))+","+  str((dados['B'][x]))+")")
    #Apos funcao que insere ser feita comentar/remover print acima
    if (sql == ""):
        sql = "("+str(x+1)+","+str((dados['A'][x]))+","+  str((dados['B'][x]))+")"
    else:
        sql = sql + ",("+str(x+1)+","+str((dados['A'][x]))+","+  str((dados['B'][x]))+")"

result = dbConnectExec.execInsert(sql, "initial")

# ========== REALIZANDO UNDO ========== #
# -------- utilizando o log.txt ------- #
# ===================================== #

file = open('log.txt', 'rb')
file.seek(0)
log = re.sub('>|<', '',file.read().decode())
file.close()

transacoes=[]
tCommitadas=[]
tCKPT=[]
acoes=[]
tRedo=[]

# Possivel brincar de otimizacao, olhando de baixo pra cima o arquivo
# caso existir CKPT, buscar todas as operacoes ate o start das transacoes dentro da lista do CKPT que sofreram commit
# para arquivos de log mais longos as checagens extras provavelmente sao mais eficientes 

# Primeira passada pelo arquivo de log para identificar TRANSACOES, COMMITS E CKPT
linhas = log.split('\n') # remove quebras de linha
for linha in linhas:
    aux=linha.split(" ") #separa string da linha por espaço em branco
    if aux[0]=="start":
        transacoes.append(aux[1])
    if aux[0]=="commit":
        tCommitadas.append(aux[1])
    if aux[0]=="CKPT":
        aux=str(re.sub('\(|\)', '', aux[1])).split(",") # remove parenteses da string da linha e separa ela pela ","
        for x in range(len(aux)):
            tCKPT.append(aux[x])
    aux=linha.split(",")
    if aux[0] in transacoes:
        acoes.append(aux)


print("\nTransacoes Identificadas")
for x in range(len(transacoes)):
    print(transacoes[x])

print("\nTransacoes Commitadas")
for x in range(len(tCommitadas)):
    print(tCommitadas[x])

print("\nTransacoes em CHECKPOINT")
for x in range(len(tCKPT)):
    print(tCKPT[x])

print("\nAcoes")
for x in range(len(acoes)):
    print(acoes[x])
# Se a transacao da acao estiver na lista de transacoes que devem ser refeitas, fazemos o update com ela!

for x in range(len(transacoes)):
    if transacoes[x] in tCommitadas and transacoes[x] in tCKPT:
        tRedo.append(transacoes[x])

print("\nTrasacoes que serão Refeitas:")
for x in range(len(tRedo)):
    print(tRedo[x])


# Possivel otimizar se validar qual a ultima transacao que altera um determinado campo de uma tupla
# validar apenas esta operacao e aplica-la se necessario
# Pensar em mudar como a montagem dos SQLs sao realizadas, admitidamente estao de maneira improvisada

print("\nAcoes que serao refeitas")
for x in range(len(acoes)):
    if acoes[x][0] in tRedo:
        print(acoes[x])

        acoesValores = re.sub('\]|\[', '', str(acoes[x])).split(',') # Ajusta a string para um vetor com as informacoes
        
        sql = "SELECT %s FROM initial WHERE id = %s" % (re.sub("'", "", acoesValores[2])
                                                      , re.sub("'", "", acoesValores[1])) # Formatacao string para consulta 

        result = dbConnectExec.connectExecuteDatabaseOperation(sql, 1)
        valorTabela = re.sub("[^0-9]", "", str(result[0])) # Ajusta retorno da consulta
                                                           # Com apenas 1 colunas a biblioteca
                                                           # Adiciona uma virgula desnecessaria

        if acoesValores[4] != valorTabela:                       # Valida se update e necessario e caso sim, realiza
             dbConnectExec.execUpdate('%s = %s' %(re.sub("'", "", acoesValores[2]), re.sub("'", "", acoesValores[4]))
                                    , 'id = %s' % re.sub("'", "", acoesValores[1]) 
                                    ,'initial')
                                    
print(dbConnectExec.connectExecuteDatabaseOperation('SELECT * FROM initial', 1))










