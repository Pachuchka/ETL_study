import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col


logging.getLogger('org.apache.spark').setLevel(logging.CRITICAL + 1)

#spark = SparkSession.builder.appName("Read Excel File").getOrCreate()


def rename_id_columns(file_path):
    spark = SparkSession.builder.appName("Rename ID Columns").getOrCreate()
    df = spark.read.format("com.crealytics.spark.excel").option("header", "true").load(file_path)
    file_name = file_path.split("\\")[-1].split(".")[0]
    id_cols = [col for col in df.columns if col == "ID"]
    for col in id_cols:
        df = df.withColumnRenamed(col, f"{file_name.split("\\")[-1]}_{col}")
    return df

df_Sales = rename_id_columns("D:\DataScience\ETL\sem 3\Sales.xlsx")
df_Advertisers = rename_id_columns("D:\DataScience\ETL\sem 3\Advertisers.xlsx")
df_Campaign = rename_id_columns("D:\DataScience\ETL\sem 3\Campaign.xlsx")


#Объединяем df_Sales с df_Advertisers по advertise_id

merged_sales_advertisers = df_Sales.join(df_Advertisers, df_Sales.advertise_id == df_Advertisers.Advertisers_ID)
#df_Sales.show()
#df_Advertisers.show()
merged_sales_advertisers.show()

# Группируем по ID и считаем количество уникальных campaign_id
sales_counts = merged_sales_advertisers.groupBy('Advertisers_ID').agg({'Sales_ID': 'count'}).withColumnRenamed('count(Sales_ID)', 'count of Sales').select('Advertisers_ID', 'count of Sales')
sales_counts = sales_counts.withColumnRenamed("Advertisers_ID", "Delete")
sales_counts.show()

#campaign_counts = merged_sales_advertisers.groupBy('Advertisers_ID').agg({'Campaign_ID': 'count'}).withColumnRenamed('count(Campaign_ID)', 'unique_campaigns').select('Advertisers_ID', 'unique_campaigns')

campaign_counts = merged_sales_advertisers.groupBy('Advertisers_ID') \
                                         .agg({'Campaign_ID': 'collect_set'}) \
                                         .withColumnRenamed('collect_set(Campaign_ID)', 'unique_campaigns') \
                                         .selectExpr('Advertisers_ID', 'size(unique_campaigns) as unique_campaigns')
campaign_counts = campaign_counts.withColumnRenamed("Advertisers_ID", "Delete")
campaign_counts.show()

# Объединяем df_Sales с df_Advertisers по advertise_id снова
result = df_Advertisers.join(sales_counts, sales_counts.Delete == df_Advertisers.Advertisers_ID)
result = result.drop("Delete")
result = result.join(campaign_counts, campaign_counts.Delete == df_Advertisers.Advertisers_ID)
result = result.drop("Delete")
result.orderBy("Advertisers_ID").show()

# Объединяем df_Advertisers с df_Campaign по Advertisers_ID
merged_Sales_Campaign = df_Sales.join(df_Campaign, df_Sales.campaign_id == df_Campaign.Campaign_ID)

# Сгруппируем по Advertisers_ID и соберем названия Campaign
campaigns_df = merged_Sales_Campaign.groupBy("advertise_id").agg(
    F.collect_list("Name").alias("Campaigns")
)

# Объединим названия Campaign через точку с запятой
campaigns_df = campaigns_df.withColumn(
    "Campaigns", F.concat_ws("; ", "Campaigns")
)
campaigns_df.show()
campaigns_df = campaigns_df.withColumnRenamed("advertise_id", "Advertisers_ID")
# Объединим result с новым столбцом
result = result.join(campaigns_df, on="Advertisers_ID", how="left")
result.orderBy("Advertisers_ID").show()

spark = SparkSession.builder.appName("MySQL load").getOrCreate()
from pyspark.sql.types import *


# Определяем параметры подключения к MySQL
username = "root"
password = "root"
url = "jdbc:mysql://localhost:3306/GeekBrains"
driver = "com.mysql.cj.jdbc.Driver"

# Записываем датафрейм в MySQL
df_Sales.write.format("jdbc") \
    .option("url", url) \
    .option("dbtable", "Sales") \
    .option("user", username) \
    .option("password", password) \
    .option("driver", driver) \
    .save()
df_Advertisers.write.format("jdbc") \
    .option("url", url) \
    .option("dbtable", "Advertisers") \
    .option("user", username) \
    .option("password", password) \
    .option("driver", driver) \
    .save()
df_Campaign.write.format("jdbc") \
    .option("url", url) \
    .option("dbtable", "Campaign") \
    .option("user", username) \
    .option("password", password) \
    .option("driver", driver) \
    .save()
result.write.format("jdbc") \
    .option("url", url) \
    .option("dbtable", "denormolize_Advertisers") \
    .option("user", username) \
    .option("password", password) \
    .option("driver", driver) \
    .save()
