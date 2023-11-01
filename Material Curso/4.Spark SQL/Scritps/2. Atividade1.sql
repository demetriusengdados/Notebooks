clientes = spark.read.load("/home/demetrius/download/Atividades/Clientes.parquet")
vendas = spark.read.load("/home/demetrius/download/Atividades/Vendas.parquet")
itensvendas = spark.read.load("/home/demetrius/download/Atividades/ItensVendas.parquet")
produtos = spark.read.load("/home/demetrius/download/Atividades/Produtos.parquet")
vendedores = spark.read.load("/home/demetrius/download/Atividades/Vendedores.parquet")

spark.sql("create database VendasVarejo")

spark.sql("use VendasVarejo")

clientes.write.saveAsTable("clientes")
vendas.write.saveAsTable("vendas")
itensvendas.write.saveAsTable("itensvendas")
produtos.write.saveAsTable("produtos")
vendedores.write.saveAsTable("vendedores")














