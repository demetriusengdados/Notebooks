create database tabelas; --Vamos usar o postgres como banco 

spark-submit --jars c:/Usuários/Demetrius/Downloads/postgresql-42.2.23.jar aplicacao_final -a c:/Usuários/Demetrius/Atividades/Vendas.parquet -t Vendas

select * from Vendas
