from pyspark.sql import SparkSession, Row
from pyspark.logger import PySparkLogger
import pyspark.sql.functions as sf
import json
import random

logger = PySparkLogger.getLogger("MySparkApp")

spark = (
    SparkSession.builder
        .master('spark://spark-master:7077')
        .appName('Spark Start')
        .getOrCreate()
)

df = spark.read.csv('/opt/airflow/dags/data_mall/Mall_Customers.csv', header = True)

mean_age = df.select(sf.mean('Age').alias('av_age'))
mean_anual_income = df.select(sf.mean('Annual Income (k$)').alias('av_income'))
mean_spending_score = df.select(sf.mean('Spending Score (1-100)').alias('av_sp'))

men = df.select(sf.count('Gender').alias('men_count'))
women = df.select(sf.count('Gender').alias('women_count'))

men = men.collect()[0].men_count
women = women.collect()[0].women_count

if men >= women:
    total_people = men
else:
    total_people = women

mean_age = round(mean_age.collect()[0].av_age)
mean_anual_income = round(mean_anual_income.collect()[0].av_income)
mean_spending_score = round(mean_spending_score.collect()[0].av_sp)

res_dict = {
    'mean_age': mean_age,
    'mean_anual_income': mean_anual_income,
    'mean_spending_score': mean_spending_score,
    'total_people': total_people
}

income = sf.col("Annual Income (k$)").cast("int")

max_age = df.select(sf.max('Age').alias('mAge')).collect()[0].mAge
min_age = df.select(sf.min('Age').alias('mAge')).collect()[0].mAge

max_in = df.select(sf.max(income)).first()[0]
min_in = df.select(sf.min(income)).first()[0]

print(f"Age form {min_age, max_age}")
print(f"Annual Income {min_in, max_in}")

row_new = Row(**{
    df.columns[0] : df.count() + 1,
    df.columns[1]: random.choice(['Male', 'Female']),
    df.columns[2]: random.randint(int(min_age), int(max_age)),
    df.columns[3]: random.randint(int(min_in), int(max_in)),
    df.columns[4]: random.randint(0, 101)  
})

df = df.collect()
df.append(row_new)

df = spark.createDataFrame(df)

pandas_df = df.toPandas()

with open('spark/report/res_dict.json', 'w', encoding='utf-8') as f:
    json.dump(res_dict, f, ensure_ascii=False, indent=4)
    
pandas_df.to_csv("/opt/airflow/dags/data_mall/Mall_Customers.csv", index=False)

spark.stop()


