from pyspark.sql import SparkSession
from pyspark.sql.functions import *

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Teste").getOrCreate()
    arqschema = "id INT, nome STRING, status STRING, cidade STRING, vendas INT, data STRING"
    despachante = spark.read.csv("c://Usuários/Demetrius/Downloads/despachantes.csv", header=False, schema = arqschema)
    calculo = despachante.select('data').groupBy(year("data")).count()
    calculo.write.format("console").save()
    spark.stop()

#spark-submit escrevendo_no_console (comando para rodar a aplicação)


