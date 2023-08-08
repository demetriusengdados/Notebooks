mongo import --db post --collection post --legacy --file c://Usuários/Demetrius/Downloads/mongo/posts.json (comando para startar o mongo e o caminho onde o arquivo está)

show dbs (expondo os bancos existentes)

use post (comando para setar o banco a ser usado)

db.post.find()

db.post.find().pretty()

pyspark --packages org.mongodb.spark:mongo-spark-connector_2.12.3.0.1 (extraindo o conector do mongo para pyspark)

posts = spark.read.format("mongo").option("uri","mongodb://127.0.0.1/posts.post").load()

posts.show()

posts.write.format("mongo").option("uri","mongodb://127.0.0.1/posts2.post").save()

