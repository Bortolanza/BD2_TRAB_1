import json

#Fazer conexao com o BD

# ====== INSERINDO DADOS NO BANCO ===== #
# ---- utilizando o metadados.json ---- #
# ===================================== #

with open("metadado.json","r") as f: 
    dados = json.load(f) # Pegando os dados do arquivo
dados = dados["INITIAL"] 

for x in range(len(dados['A'])):
    #Aqui ira o comando para inserir no BD
    print("INSERT INTO table (id,A,B) VALUES ("+str(x+1)+","+str((dados['A'][x]))+","+str((dados['B'][x]))+")")
    #Apos funcao que insere ser feita comentar/remover print acima
