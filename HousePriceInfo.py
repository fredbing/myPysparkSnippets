# reads from a csv file containing real estate info
# and outputs the house price info

from pyspark.sql import SparkSession

UNIT_PRICE = "Price SQ Ft"

if __name__ == "__main__":

    session = SparkSession.builder.appName(
        "HousePriceInfo").master("local[*]").getOrCreate()

    HouseInfo = session.read \
        .option("header", "true") \
        .option("inferSchema", value=True) \
        .csv("xxx/house_information.csv")

    HouseInfo.groupBy("Location") \
        .avg(UNIT_PRICE) \
        .orderBy("avg(Price SQ FT)") \
        .show()
