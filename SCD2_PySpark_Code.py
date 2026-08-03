from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date, lit

spark = SparkSession.builder.appName("SCD2").getOrCreate()

# Existing SCD2 table
target_df = spark.createDataFrame([
    (1, "John", "Chennai", "2025-01-01", None, "Y"),
    (2, "David", "Mumbai", "2025-01-01", None, "Y")
], ["id", "name", "city", "start_date", "end_date", "is_current"])

# New source snapshot
source_df = spark.createDataFrame([
    (1, "John", "Bangalore"),   # city changed
    (2, "David", "Mumbai"),     # no change
    (3, "Peter", "Delhi")       # new record
], ["id", "name", "city"])

# Find changed records
changed_df = (
    source_df.alias("s")
    .join(
        target_df.filter(col("is_current") == "Y").alias("t"),
        "id"
    )
    .filter(col("s.city") != col("t.city"))
)

# Expire old records
expired_df = (
    target_df.alias("t")
    .join(changed_df.select("id"), "id")
    .withColumn("end_date", current_date())
    .withColumn("is_current", lit("N"))
)

# Create new versions of changed records
new_versions_df = (
    changed_df.select(
        col("s.id"),
        col("s.name"),
        col("s.city")
    )
    .withColumn("start_date", current_date())
    .withColumn("end_date", lit(None))
    .withColumn("is_current", lit("Y"))
)

# Find brand-new records
new_records_df = (
    source_df.alias("s")
    .join(target_df.alias("t"), "id", "left_anti")
    .withColumn("start_date", current_date())
    .withColumn("end_date", lit(None))
    .withColumn("is_current", lit("Y"))
)

# Unchanged current records
unchanged_df = (
    target_df.alias("t")
    .join(changed_df.select("id"), "id", "left_anti")
)

# Final SCD2 table
final_df = (
    unchanged_df
    .unionByName(expired_df)
    .unionByName(new_versions_df)
    .unionByName(new_records_df)
)

final_df.show(truncate=False)