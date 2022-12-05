import json
import dbConnectExec
import re

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

# Primeira passada pelo arquivo de log para identificar TRANSACOES, COMMITS E CKPT
linhas = log.split('\n') # remove quebras de linha
procurando=1
achouCKPT=0
linhas.reverse()

for linha in linhas:
    linha=linha.rstrip()
    if(procurando==0):
        break
    aux=linha.split(" ") #separa string da linha por espaço em branco
    if aux[0]=="commit":
        tCommitadas.append(aux[1])
    if aux[0]=="CKPT" and not achouCKPT:
        achouCKPT=1
        aux=str(re.sub('\(|\)', '', aux[1])).split(",") # remove parenteses da string da linha e separa ela pela ","
        for x in range(len(aux)):
            if(aux[x] in tCommitadas):
                tRedo.append(aux[x])
        procurando=len(tRedo)
    if aux[0]=="start":
        if aux[1] in tRedo:
            procurando=procurando-1
    aux=linha.split(",")
    if aux[0] in tCommitadas:
        acoes.append(aux)

if not achouCKPT:   #Caso nao tenha achado checkpoint refaz tudo q encontrou e foi commitado
    for x in range(len(tCommitadas)):
        tRedo.append(tCommitadas[x])

print("\nTrasacoes que serão Refeitas:")
for x in range(len(tRedo)):
    print(tRedo[x])

print("\nAcoes que serao refeitas")
for x in range(len(acoes)):
    acoes.reverse()
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










