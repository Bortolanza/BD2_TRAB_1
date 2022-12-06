import json
import dbConnectExec
import re

# ====== INSERINDO DADOS NO BANCO ===== #
# ---- utilizando o metadados.json ---- #
# ===================================== #

with open("metadado.json","r") as f: 
    dados = json.load(f) # Pegando os dados do arquivo
f.close()

dados = dados["INITIAL"]
nomeTabela = 'initial'
sql = "" 

dbConnectExec.connectExecuteDatabaseOperation("TRUNCATE %s;" % re.sub("'", "",nomeTabela), 0)

for x in range(len(dados['A'])):

    print("INSERT INTO "+re.sub("'", "",nomeTabela)+"(id,a,b) VALUES ("+str(x+1)+","+str((dados['A'][x]))+","+  str((dados['B'][x]))+")")
    
    if (x == 0):
        sql = "("+str(x+1)+","+str((dados['A'][x]))+","+  str((dados['B'][x]))+")"
    else:
        sql = sql + ",("+str(x+1)+","+str((dados['A'][x]))+","+  str((dados['B'][x]))+")"

result = dbConnectExec.execInsert(sql, re.sub("'", "",nomeTabela))

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

print("\nTransações que serão refeitas:")
for x in range(len(tRedo)):
    print(tRedo[x])

print("\nAções que serao refeitas:")
acoes.reverse()
for x in range(len(acoes)):
    if acoes[x][0] in tRedo:
        print(acoes[x])

        acoesValores = re.sub('\]|\[', '', str(acoes[x])).split(',') # Ajusta a string para um vetor com as informacoes
        
        sql = "SELECT %s FROM %s WHERE id = %s" % (re.sub("'", "", acoesValores[2])
                                                 , re.sub("'", "", nomeTabela)
                                                 , re.sub("'", "", acoesValores[1])) # Formatacao string para consulta 

        result = dbConnectExec.connectExecuteDatabaseOperation(sql, 1)
        valorTabela = result[0][0]
    
        if acoesValores[4] != valorTabela:                       # Valida se update e necessario e caso sim, realiza
            dbConnectExec.execUpdate('%s = %s' %(re.sub("'", "", acoesValores[2]), re.sub("'", "", acoesValores[4]))
                                    , 'id = %s' % re.sub("'", "", acoesValores[1]) 
                                    ,re.sub("'", "",nomeTabela))

print('\n Tabela após operações de REDO: \n')                                    
print( '\t'+str(dbConnectExec.connectExecuteDatabaseOperation
   ("""SELECT JSON_BUILD_OBJECT('INITIAL', JSON_BUILD_OBJECT('id', ARRAY_AGG(id)
                                                           , 'A' , ARRAY_AGG(a)
                                                           , 'B' , ARRAY_AGG(b))
                                                                            )
         FROM %s""" %re.sub("'", "",nomeTabela), 1)[0][0]))










