# class pyspark.sql.DataFrameReader(spark)
# Interface to load a DataFrame from external storage system
# use spark.read() to access: txt, CSV, JSON, Parquet, ORC, AVRO, XML
# specify the spark.read() method with: (1)format, (2)option, (3)schema, (4)load or save
# format includes: parquet, json, orc, com.databricks.spark.avro, com.databricks.spark.xml
# for "option", there are many options availables. "mode" can be specified inside the option method, e.g.,
#  .option("mode", "failfast"). there are 3 modes to choose when reading a file: permissive, dropmalformaed, failfast


s = spark.read.schema("col0 INT, col1 DOUBLE")

df = spark.read.parquet('pyth/test/sql/parquet_partitioned')
df.creatOrReplaceTempView('tmpTable')
spark.read.table('tmpTable').dtypes[(
    'name', 'string'), ('year', 'int'), ('month', 'int'), ('day', 'int')]

df = spark.read.load("examples/src/people.csv",
                     format="csv", sep=":", inferSchema="true", header="true")

df = spark.read.text('pyth/test/sql/test.txt')
df.collect()
df = spark.read.text('pyth/test/sql/test.txt', Wholetext=True)

df = spark.read.load("examples/src/users.parquet")
df.select("name", "favorite_color").write.save(
    "namesAndFavoriteColors.parquet")

# class pyspark.sql.DataFrameWriter(df)
# Interface to write a DataFrame to external storage system
# use spark.write() to write to format: CSV, JSON, Parquet, ORC, AVRO, XML
# specify the spark.write() methods
# major methods are: (1)format, (2)option, (3)mode, (4)save
# additional methods include: partitionBy, bucketBy, sortBy
# mode includes append, overwrite, errorIfExists, ignore

df = spark.read.parquet("examples/src/users.parquet")
(df
    .write
    .partitionBy("favorite_color")
    .bucketBy(42, "name")
    .saveAsTable("people_partitioned_bucketed"))

df = spark.read.orc("examples/src/users.orc")
(df.write.format("orc")
    .option("orc.bloom.filter.columns", "favorite_color")
    .option("orc.dictionary.key.threshhold", "1.0")
    .save("users_with_options.orc"))


(df.write.foramt('parquet')
    .bucketBy(100, 'year', 'month')
    .mode("overwrite")
    .saveAsTable('bucketed_table'))


df.write.csv(os.path.join(tempfile.mkdtemp(), 'data'))

df.write.format('json').save(os.path.join(tempfile.mkdtemp(), 'data'))

df.write.mode('append').parquet(os.path.join(tempfile.mkdtemp(), 'data'))
