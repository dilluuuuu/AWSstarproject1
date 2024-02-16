from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from pyspark.sql.functions import col, sha2, concat_ws
from pyspark.sql.types import DecimalType
import sys
from pyspark.sql.functions import current_date as spark_current_date, lit
from pyspark.sql.functions import monotonically_increasing_id
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.functions import when


class DatasetTransformer1:
    def __init__(self, spark, app_config):
        self.spark = spark
        self.app_config = app_config
        self.source_bucket = app_config["source_bucket"]
        self.destination_bucket = app_config["destination_bucket"]
        self.transformations = app_config.get("transformations", {}).asDict()
        self.masking = app_config.get("masking", {}).asDict()

    def read_data(self):
        raw_data = self.spark.read.parquet('s3://dileep-landingbucket-batch01/datasets/Actives.parquet')
        raw_data.write.parquet('s3://dileep-rawbucket-batch01/actives/', mode="overwrite")
        return self.spark.read.format("parquet").load('s3://dileep-rawbucket-batch01/actives/')

    def apply_transformations(self, df):
        for column, transformation in self.transformations.items():
            df = df.withColumn(column, expr(transformation))
        return df

    def apply_masking(self, df):
        for column, hashing_expr in self.masking.items():
            if column == "user_id":
                df = df.withColumn(column, sha2(expr(f"cast({column} as binary)"), 256))
            else:
                df = df.withColumn(column, sha2(column, 256))
        return df

    def show_data(self, df, num_rows=5):
        df.show(num_rows)

    def write_data(self, df):
        df.write.partitionBy("month", "date").format("parquet").mode("overwrite").save(self.destination_bucket)
        
    def lookup_table(self,df1):
        try:
            current_df=spark.read.format("delta").load("s3://dileep-stagingbucket-batch01/lookup/delta_table/")   
            df = spark.read.format("parquet").load("s3://dileep-rawbucket-batch01/actives/")
            print(df.count())
            df=df[['advertising_id', 'user_id']]
            df1=df1[['advertising_id', 'user_id']]
            df1 = df1.withColumnRenamed('advertising_id', 'masked_advertising_id') \
                     .withColumnRenamed('user_id', 'masked_user_id')
            print(df1.count())
            df = df.withColumn("row_index", monotonically_increasing_id())
            df1 = df1.withColumn("row_index", monotonically_increasing_id())

            df2 = df.join(df1, "row_index").drop("row_index")
            new_df = df2.withColumn("start_date", spark_current_date().cast("string")) \
                        .withColumn("end_date", lit("0/0/0000")) \
                        .withColumn("flag_active", lit("True"))
            #print(merged_df)
            current_date = datetime.now().strftime('%Y-%m-%d')
            print(current_df.count())
            current_date_str = datetime.now().strftime('%Y-%m-%d')
            # Update end_date and flag in current_df_spark if advertising_id is matched but user_id is not matched in new_df_spark
            #current_df = current_df.withColumn("end_date", F.when(~F.col("user_id").isin([row.user_id for row in new_df.select("user_id").collect()]), current_date).otherwise(F.col("end_date")))

            #current_df = current_df.withColumn("flag_active", F.when(~F.col("user_id").isin([row.user_id for row in new_df.select("user_id").collect()]), "False").otherwise(F.col("flag_active")))

            current_df = current_df.withColumn("end_date", F.when((F.col("advertising_id").isin([row.advertising_id for row in new_df.select("advertising_id").collect()])) &
                                          (~F.col("user_id").isin([row.user_id for row in new_df.select("user_id").collect()])), 
                                          current_date).otherwise(F.col("end_date")))

            current_df = current_df.withColumn("flag_active", F.when((F.col("advertising_id").isin([row.advertising_id for row in new_df.select("advertising_id").collect()])) &
                                          (~F.col("user_id").isin([row.user_id for row in new_df.select("user_id").collect()])), 
                                          "False").otherwise(F.col("flag_active")))


            print(current_df.count())
            print(new_df.count())
            # Union current_df_spark and new_df_spark, drop duplicates
            final_df = current_df.union(new_df).dropDuplicates(["advertising_id", "user_id"])

            # Display the updated current_data
            print("Current DataFrame:")
            current_df.show(5)

            print("\nFinal DataFrame after Union:")
            final_df.show()
            print(final_df.count())
            final_df.write.format("delta").mode("overwrite").save("s3://dileep-stagingbucket-batch01/lookup/delta_table/")
            #final_df.writeTo("delta_table").append()
            current_df=spark.read.format("delta").load("s3://dileep-stagingbucket-batch01/lookup/delta_table/")
            print(current_df.count())
        except Exception as e:
            print("NONE")
            df=spark.read.format("parquet").load("s3://dileep-rawbucket-batch01/actives")
            df=df[['advertising_id', 'user_id']]
            print(df.count())
            df1=df1[['advertising_id', 'user_id']]
            df1 = df1.withColumnRenamed('advertising_id', 'masked_advertising_id') \
                     .withColumnRenamed('user_id', 'masked_user_id')
            print(df1.count())
            df = df.withColumn("row_index", monotonically_increasing_id())
            df1 = df1.withColumn("row_index", monotonically_increasing_id())

            df2 = df.join(df1, "row_index").drop("row_index")
            # Joining the two DataFrames based on row index
            merged_df = df2.withColumn("start_date", spark_current_date().cast("string")) \
                           .withColumn("end_date", lit("0/0/0000")) \
                           .withColumn("flag_active", lit("True"))

            spark.sql("""CREATE  TABLE IF NOT EXISTS delta_table (advertising_id string, user_id string, masked_advertising_id string, masked_user_id string, start_date string, end_date string, flag_active string )
            USING delta location
            's3://dileep-stagingbucket-batch01/lookup/delta_table' """);
            merged_df.writeTo("delta_table").append()    

    

class DatasetTransformer2:
    def __init__(self, spark, app_config):
        self.spark = spark
        self.app_config = app_config
        self.source_bucket = app_config["source_bucket"]
        self.destination_bucket = app_config["destination_bucket"]
        self.transformations = app_config.get("transformations", {}).asDict()
        self.masking = app_config.get("masking", {}).asDict()

    def read_data(self):
        raw_data = self.spark.read.parquet('s3://dileep-landingbucket-batch01/datasets/Viewership.parquet')
        raw_data.write.parquet('s3://dileep-rawbucket-batch01/viewership/', mode="overwrite")
        return self.spark.read.format("parquet").load('s3://dileep-rawbucket-batch01/viewership/')

    def apply_transformations(self, df):
        for column, transformation in self.transformations.items():
            df = df.withColumn(column, expr(transformation))
        return df

    def apply_masking(self, df):
        for column, hashing_expr in self.masking.items():
            if column == "user_id":
                df = df.withColumn(masked_user_id, sha2(expr(f"cast({column} as binary)"), 256))
            else:
                df = df.withColumn(column, sha2(column, 256))
        return df

    def show_data(self, df, num_rows=5):
        df.show(num_rows)

    def write_data(self, df):
        df.write.partitionBy("month", "date").format("parquet").mode("overwrite").save(self.destination_bucket)
        df.write.format("parquet").mode("overwrite").save(self.destination_bucket)

def main_code():

    arg1 = sys.argv[1]
    # Create a Spark session
    spark = SparkSession.builder \
    .appName("Delta Table Checker") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:1.0.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.executor.memory", "4G") \
    .config("spark.executor.instances", "5") \
    .getOrCreate()
    
    if 'Actives.parquet' in arg1:
        # Read configuration from JSON file in S3 for actives data
        app_config_path = 's3://dileep-landingbucket-batch01/config/appconfig.json'
        app_config_df = spark.read.option("multiline", "true").json(app_config_path)
        app_config = app_config_df.first().asDict()

        # Create an instance of DatasetTransformer
        transformer1 = DatasetTransformer1(spark, app_config)

        # Read data
        actives_df = transformer1.read_data()

        # Apply transformations
        actives_df = transformer1.apply_transformations(actives_df)
   
        # Apply masking
        actives_df = transformer1.apply_masking(actives_df)

        # Show the transformed DataFrame
        transformer1.show_data(actives_df, num_rows=5)

        # Write the transformed DataFrame back to S3
        transformer1.write_data(actives_df)

        #SCD2 LookUp Table
        transformer1.lookup_table(actives_df)

    elif 'Viewership.parquet' in arg1:
        # Read configuration from JSON file in S3 for viewership data
        Vapp_config_path = 's3://dileep-landingbucket-batch01/config/Vappconfig.json'
        Vapp_config_df = spark.read.option("multiline", "true").json(Vapp_config_path)
        Vapp_config = Vapp_config_df.first().asDict()

        # Create an instance of DatasetTransformer
        transformer2 = DatasetTransformer2(spark, Vapp_config)

        # Read data
        viewership_df = transformer2.read_data()

        # Apply transformations
        viewership_df = transformer2.apply_transformations(viewership_df)
   
        # Apply masking
        viewership_df = transformer2.apply_masking(viewership_df)

        # Show the transformed DataFrame
        transformer2.show_data(viewership_df, num_rows=5)

        # Write the transformed DataFrame back to S3
        transformer2.write_data(viewership_df)
    else:
        print("error")

    # Stop the Spark session
    spark.stop()

main_code()
