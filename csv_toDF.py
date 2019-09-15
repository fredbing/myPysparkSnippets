
from pyspark.sql.types import IntegerType

# from pyspark.sql import SparkSession
# spark = SparkSession \
#     .Builder \
#     .appName("My Python Spark SQL") \
#     .config("spark.config.option", "some value") \
#     .getOrCreate()

#df = spark.read.csv('mydata.csv')

df2 = spark.read.csv('./mysubdirectory/mydata.csv')
# df2 = spark.read.csv('/mysubdirectory/mydata.csv')  this won't work without the dot in the directory

df.take()
df.printSchema()
df.head()
df.count()
df.describe().show()
df.dropna().count()
df.fillna(-1).show(5)

df2 = spark.read.csv('./mysubdirectory/mydata.csv').toDF('ID',
                                                         'Name', 'Gender', 'Age', 'Status')
df2.printSchema()
df2.select('ID', 'Name').show()
for i in df2.select('ID', 'Name').take(5):
    print(i)

df3 = df2.withColumn('ID', df2.ID.cast(IntegerType())) \
         .withColumn('Age', df2.Age.cast(IntegerType()))

df3.printSchema

help(spark.read)

'''with multiLine=True, it will load file without stripping new line character \n, 
but without multiLine=True, it won't load the JSON file. need to try other options to work on JSON'''

df7 = spark.read.json('./mysubdirectory/myjfile.json', multiLine=True)

df8 = sqlContext.read.json('./mysubdirectory/myjfile.json', multiLine=True)
