h1. *Integrantes*

* Enzo H. Bortolanza
* Fernando de Souza 

h1. *Modificacoes possivelmente necessarias*

* Para configurar corretamente o programa e necessario ajustar corretamente os parametros de conexao
  com o banco de dados, as informacoes se encontram no arquivo dbConnectExec.py e sao passadas para
  o metodo "psycopg2.connect", codigo demonstrado abaixo.

* * connection = psycopg2.connect(user="USUARIO_BD",
                                  password="SENHA",
                                  host="IP_HOST",
                                  port="PORTA",
                                  database="*NOME_DA_BASE*")

* Outra possivel alteracao tem relacao com o nome da tabela, para ajustar basta alterar o valor 
  informado na variavel "nomeTabela" para o nome da tabela correta. Esta variavel se encontra no
  arquivo redo.py, linha 14

h1. *Requisitos*

* E necessario possuir a biblioteca "psycopg2", pois atraves dela e realizada a conexao com o banco

* * Instalacao em ambiente linux basta executar o seguinte comando:
    pip install psycopg2