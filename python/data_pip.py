from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("test rdd").master("local").getOrCreate()
sc=spark.sparkContext
l=['india','china','usa','india','usa','usa']  #-- india,2  , usa,3,china,1
rdd=sc.parallelize(l)
rdd1=rdd.map(lambda x:(x,1))    #-- ['india':1,'china':1,'usa':1,'india':1,'usa':1,'usa':1]
rdd2=rdd1.reduceByKey(lambda x,y:x+y)

print(rdd2.getNumPartitions())

rdd3=rdd2.repartition(4)

print(rdd3.getNumPartitions())


rdd4=rdd3.coalesce(3)
print(rdd4.getNumPartitions())
