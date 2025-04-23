from pyspark.sql import SparkSession
from pyspark.sql.functions import col, abs, avg, sum, count, when
from pyspark.sql.types import IntegerType, StringType

# 1. Initialize Spark session
spark = SparkSession.builder \
    .appName("Calculate MAE and Accuracy (Full Clean Version)") \
    .getOrCreate()

# 2. Load dataset  (need to change path for every dataset)
df = spark.read.option("header", "true").option("sep", "\t").csv("C:/Users/chava/Big Data Inputs/amazon_reviews_us_Mobile_Apps_v1_00.tsv")

# 3. Replace nulls in all columns with 0
df = df.fillna(0)

# 4. Replace non-numeric values in 'star_rating' with 0 using regex
df = df.withColumn(
    "star_rating",
    when(col("star_rating").cast("string").rlike(r"^\d+$"), col("star_rating").cast(IntegerType()))
    .otherwise(0)
)

# 5. Replace invalid ratings (<1 or >5) with 0
df = df.withColumn(
    "star_rating",
    when((col("star_rating") >= 1) & (col("star_rating") <= 5), col("star_rating"))
    .otherwise(0)
)

# 6. Cast product_category to string (in case it got affected by fillna)
df = df.withColumn("product_category", col("product_category").cast(StringType()))

# 7. Filter only valid ratings (1 to 5) for MAE calculation
df_valid = df.filter((col("star_rating") >= 1) & (col("star_rating") <= 5))

# 8. Compute average rating for each product category
avg_rating_df = df_valid.groupBy("product_category").agg(avg("star_rating").alias("avg_rating"))

# 9. Join original valid ratings with average ratings
df_with_avg = df_valid.join(avg_rating_df, on="product_category", how="inner")

# 10. Compute absolute error
df_with_error = df_with_avg.withColumn("abs_error", abs(col("star_rating") - col("avg_rating")))

# 11. Calculate MAE
mae_df = df_with_error.groupBy("product_category").agg(
    (sum("abs_error") / count("star_rating")).alias("mae")
)

# 12. Calculate accuracy
MAX_RATING = 5
mae_df = mae_df.withColumn("accuracy", 100 - ((col("mae") / MAX_RATING) * 100))

# 13. Show final results
mae_df.show(truncate=False)