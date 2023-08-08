create database vendas;

CREATE DATABASE;

\i /C:/Usuários/Demetrius/Downloads/demo/1.CreateTable.sql

\i /C:/Usuários/Demetrius/Downloads/demo/2.InsertIntoCliente.sql

\i /C:/Usuários/Demetrius/Downloads/demo/3.InsertIntoProduto.sql

\i /C:/Usuários/Demetrius/Downloads/demo/4.InsertIntoVendedores.sql

\i /C:/Usuários/Demetrius/Downloads/demo/1.InsertIntoVendas.sql

\i /C:/Usuários/Demetrius/Downloads/demo/6.InsertIntoItensVenda.sql

select * from clientes;

select * from vendedores;

#caminho para o arquivo do banco postgres /Downloads pyspark --jars postgresql-42.2.23.jar (comando para executar o banco no pyspark)

from pyspark import SparkSession

resumo = spark.read.format('jdbc').option("url",'jdbc:postgresql://localhost:5432/vendas')
    .option("dbtable","Vendas")
    .option('user','postgres')
    .option("password",'123456')
    .option('driver', 'org.postgresql.Driver').load()

resumo.show()

clientes = spark.read.format('jdbc').option("url",'jdbc:postgresql://localhost:5432/vendas')
    .option("dbtable","Clientes")
    .option('user','postgres')
    .option("password",'123456')
    .option('driver', 'org.postgresql.Driver').load()

cliente.show()

vendadata = resumo.select("data", 'total')
vendadata.show()

vendadata.write.format('jdbc').option("url",'jdbc:postgresql://localhost:5432/vendas')
    .option("dbtable","vendadata")
    .option('user','postgres')
    .option("password",'123456')
    .option('driver', 'org.postgresql.Driver').save()

select * from vendadata