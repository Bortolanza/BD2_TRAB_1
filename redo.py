import json
import dbConnectExec


# ====== INSERINDO DADOS NO BANCO ===== #
# ---- utilizando o metadados.json ---- #
# ===================================== #

with open("metadado.json","r") as f: 
    dados = json.load(f) # Pegando os dados do arquivo
f.close()

dados = dados["INITIAL"] 

for x in range(len(dados['A'])):
    #Aqui ira o comando para inserir no BD
    print("INSERT INTO table (id,A,B) VALUES ("+str(x+1)+","+str((dados['A'][x]))+","+str((dados['B'][x]))+")")
    #Apos funcao que insere ser feita comentar/remover print acima


# ========== REALIZANDO UNDO ========== #
# -------- utilizando o log.txt ------- #
# ===================================== #

file = open('log.txt', 'rb')
file.seek(0)
log = file.read().decode().replace('>', '').replace('<','')
file.close()

transacoes=[]
tCommitadas=[]
tCKPT=[]
acoes=[]
tRedo=[]

# Primeira passada pelo arquivo de log para identificar TRANSACOES, COMMITS E CKPT
linhas = log.split('\n') # remove quebras de linha
for linha in linhas:
    aux=linha.split(" ") #separa string da linha por espaço em branco
    if aux[0]=="start":
        transacoes.append(aux[1])
    if aux[0]=="commit":
        tCommitadas.append(aux[1])
    if aux[0]=="CKPT":
        aux=aux[1].replace(')','').replace('(','').split(",") # remove parenteses da string da linha e separa ela pela ","
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

# --- Disclaimer --- 
# Não sei se a lógica pra selecionar qual as transacoes serão refeitas está certa.
# Mas pelo oq eu entendi são as transações que são comittadas mas não tao em nenhum checkpoint
# ------------------

for x in range(len(transacoes)):
    if transacoes[x] in tCommitadas and transacoes[x] in tCKPT:
        tRedo.append(transacoes[x])

print("\nTrasacoes que serão Refeitas:")
for x in range(len(tRedo)):
    print(tRedo[x])

print("\nAcoes que serao refeitas")
for x in range(len(acoes)):
    if acoes[x][0] in tRedo:
        print(acoes[x])

resultSet = dbConnectExec.connectExecuteDatabaseOperation("SELECT VERSION();")
print("O RESULT SET: ", resultSet)