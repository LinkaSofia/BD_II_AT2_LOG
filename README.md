# BD_II_AT2_LOG
Trabalho Prático - LOG
(15/02)
Objetivo: implementar o mecanismo de log Redo com checkpoint usando o SGBD 
Funcionamento: 
O código, que poderá utilizar qualquer linguagem de programação, deverá ser capaz de ler o arquivo de log (entradaLog) e validar as informações no banco de dados através do modelo REDO. Ou seja, você irá implementar o mecanismo de log, validando os dados a partir do modelo Redo com checkpoints. 
O código receberá como entrada o arquivo de log (dados salvos) e os dados da tabela que irá operar no banco de dados. 
Exemplo de tabela do banco de dados: 
Id
A
B
1
100
20
2
20
30


Arquivo de log no formato <transação, “id da tupla”,”coluna”, “valor novo”>. Exemplo:
A,1=100
A,2=20  
B,1=20
B,2=30







<start T1>
<T1,1, A,30>
<start T2>
<commit T1>
<Start Checkpoint (T2)>
<T2,2, A,50>
<End Checkpoint>
    <start T3>
<Start Checkpoint (T2, T3)>
<start T4>
<T4,1, A,100>
<End Checkpoint>
<commit T4>

Saída: 
“Transação T2 não realizou Redo”
“Transação T3 não realizou Redo”
“Transação T4 realizou Redo”
 
Imprima as variáveis
1,A=
2,A=
1,B=
2,B=

O checkpoint Redo permite que parte do log já processada seja descartada para evitar o reprocessamento. 

Detalhes:
Funções a serem implementadas:
1- carregar o banco de dados com a tabela antes do SGBD finalizar a sua execução (dados parciais)
2- Carregar o arquivo de log
3- Verifique quais transações devem realizar REDO. Imprimir o nome das transações que irão sofrer Redo. Observem a questão do checkpoint.
4- Checar quais valores estão salvos nas tabelas (com o select) e atualizar valores inconsistentes (update)
5- Reportar quais dados foram atualizados. 


