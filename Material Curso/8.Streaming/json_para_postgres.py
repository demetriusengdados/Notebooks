from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Streaming2").getOrCreate()
    jsonschema = 'nome STRING, postagem STRING, data INT'

    df = spark.readStream.json("/home/Demetrius/stream", schema=jsonschema)

    diretorio = '/home/Demetrius/temp/'

    def atualiza_postgres(dataf, batchId):
        dataf.write.format("jdbc").option("url", 'jdbc:postgresql://localhost:5432/posts').option("dbtable", 'posts').option("user", 'postgres').option('password','123456').option("driver",'org.postgresql.Driver').mode('append').save()
        Stcal = df.writeStream.foreachBatch(atualiza_postgres).outputMode('append').trigger(processingTime = '5 second').option("checkpointlocation", diretorio).start()

        Stcal.awaitTermination()




    #spark-submit --jars /home/demetrius/Downloads/postgresql-42.2.23.jar json_para_postgres.py