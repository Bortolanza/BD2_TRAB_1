import json

#Fazer conexao com o BD

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

# Primeira passada pelo arquivo de log para identificar TRANSACOES, COMMITS E CKPT
linhas = log.split('\n') # remove quebras de linha
for linha in linhas:
    linha=linha.split(" ") #separa string da linha por espaço em branco
    if linha[0]=="start":
        transacoes.append(linha[1])
    if linha[0]=="commit":
        tCommitadas.append(linha[1])
    if linha[0]=="CKPT":
        linha=linha[1].replace(')','').replace('(','').split(",") # remove parenteses da string da linha e separa ela pela ","
        for x in range(len(linha)):
            tCKPT.append(linha[x])

print("\nTransacoes Identificadas")
for x in range(len(transacoes)):
    print(transacoes[x])

print("\nTransacoes Commitadas")
for x in range(len(tCommitadas)):
    print(tCommitadas[x])

print("\nTransacoes em CHECKPOINT")
for x in range(len(tCKPT)):
    print(tCKPT[x])