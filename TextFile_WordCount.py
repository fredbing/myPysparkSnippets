# pyspark code for word count of a text file

from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    conf = SparkConf().setAppName("word count").setMaster("local[3]")
    sc = SparkContext(conf=conf)

    lines = sc.textFile("xxx/mytextfile.text")

    words = lines.flatMap(lambda line: line.split(" "))

    wordCounts = words.countByValue()

    for word, count in wordCounts.items():
        print("{} : {}".format(word, count))
