from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, date_format, concat, current_timestamp, current_date, udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from org.inceptez.hack.allmethods import remspecialchar


def writedatatotable(input_df, filetype='csv', location=None, delimiter=',', mode='overwrite', partition_col_name=None):
    if filetype == "csv":
        input_df.write.format("csv").option("header", "true").mode(mode).option("delimiter", delimiter).save(location)
    elif filetype == "json":
        input_df.write.format("json").mode(mode).save(location)
    elif filetype == "parquet":
        input_df.write.mode(mode).format("parquet").insertInto(location)
    else:
        print("Invalid file type")
        return None


schema1 = StructType([
    StructField("IssuerId", IntegerType(), True),
    StructField("IssuerId2", IntegerType(), True),
    StructField("BusinessDate", DateType(), True),
    StructField("StateCode", StringType(), True),
    StructField("SourceName", StringType(), True),
    StructField("NetworkName", StringType(), True),
    StructField("NetworkURL", StringType(), True),
    StructField("custnum", StringType(), True),
    StructField("MarketCoverage", StringType(), True),
    StructField("DentalOnlyPlan", StringType(), True)
])
schema2 = StructType([
    StructField("IssuerId", IntegerType(), True),
    StructField("IssuerId2", IntegerType(), True),
    StructField("BusinessDate", StringType(), True),
    StructField("StateCode", StringType(), True),
    StructField("SourceName", StringType(), True),
    StructField("NetworkName", StringType(), True),
    StructField("NetworkURL", StringType(), True),
    StructField("custnum", StringType(), True),
    StructField("MarketCoverage", StringType(), True),
    StructField("DentalOnlyPlan", StringType(), True)
])

schema3 = StructType([
    StructField("IssuerId", IntegerType(), True),
    StructField("IssuerId2", IntegerType(), True),
    StructField("BusinessDate", StringType(), True),
    StructField("StateCode", StringType(), True),
    StructField("SourceName", StringType(), True),
    StructField("NetworkName", StringType(), True),
    StructField("NetworkURL", StringType(), True),
    StructField("custnum", StringType(), True),
    StructField("MarketCoverage", StringType(), True),
    StructField("DentalOnlyPlan", StringType(), True),
    StructField("RejectRows", StringType(), True)
])

if __name__ == "__main__":
    print("Starting the Spark session")
    # Initialize Spark session
    spark = SparkSession.builder.appName("Hackathon").enableHiveSupport()\
        .config("hive.metastore.uris", "thrift://localhost:9083").getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    # Register UDF
    remspecialchar_UDF = udf(lambda z: remspecialchar(z), StringType())

    print("Reading the CSV files")
    # Read CSV file with schema1
    insurance1_df = spark.read.csv(
        "file:///home/hduser/Desktop/IZWE44WD33/hackathon_2024/hackathon_data_2024/insuranceinfo1.csv", header=True,
        schema=schema1, mode='DROPMALFORMED')

    # Read CSV file with schema2
    insurance2_df = spark.read.csv(
        "file:///home/hduser/Desktop/IZWE44WD33/hackathon_2024/hackathon_data_2024/insuranceinfo2.csv", header=True,
        schema=schema2, inferSchema=False, mode='DROPMALFORMED')

    insurance2_df = insurance2_df.withColumn("BusinessDate",
                                             date_format(to_date(col("BusinessDate"), "dd-MM-yyyy"), "yyyy-MM-dd"))

    insurance3_df = spark.read.csv(
        "file:///home/hduser/Desktop/IZWE44WD33/hackathon_2024/hackathon_data_2024/insuranceinfo2.csv", header=True,
        schema=schema3,
        mode='PERMISSIVE',
        inferSchema=False,
        ignoreLeadingWhiteSpace=True,
        ignoreTrailingWhiteSpace=True,
        columnNameOfCorruptRecord="RejectRows")

    rejected_records_df = insurance3_df.filter(col("RejectRows").isNotNull())

    # if rejected_records_df.count() == 0:
    #     print("No rejected records found")
    # else:
    #     print("Rejected records found")
    #     rejected_records_df.write.csv(
    #         path="file:///home/hduser/Desktop/IZWE44WD33/hackathon_2024/hackathon_data_2024/rejected_records.csv",
    #         header=True)

    merged_df = insurance1_df.union(insurance2_df)

    # Rename StateCode and SourceName as stcd and srcnm
    transformed_df = merged_df.withColumnRenamed("StateCode", "stcd").withColumnRenamed("SourceName", "srcnm")

    # Concatenate IssuerId and IssuerId2 as issueridcomposite
    transformed_df = transformed_df.withColumn("issueridcomposite",
                                               concat(col("IssuerId").cast("string"), col("IssuerId2").cast("string")))

    # Remove DentalOnlyPlan column
    transformed_df = transformed_df.drop("DentalOnlyPlan")

    # Add sysdt (current date) and systs (current timestamp) columns
    transformed_df = transformed_df.withColumn("sysdt", current_date()).withColumn("systs", current_timestamp())

    # a. Remove rows containing null in any field
    cleaned_df = transformed_df.na.drop()

    # b. Count the number of rows with all columns having some value
    count_non_null_rows = cleaned_df.count()

    # Display the count of non-null rows
    print(f"Number of rows with all columns having values: {count_non_null_rows}")

    cleaned_df = cleaned_df.withColumn("NetworkName", remspecialchar_UDF(col("NetworkName")))

    # Write the cleaned DataFrame to a CSV file
    writedatatotable(filetype='csv', location="/user/hduser/output/csv/", delimiter='~', mode='overwrite',
                     input_df=cleaned_df)

    # Write the cleaned DataFrame to a JSON file
    writedatatotable(filetype='json', location="/user/hduser/output/json/", mode='overwrite', input_df=cleaned_df)

    # creating external table in Hive
    spark.sql("CREATE EXTERNAL TABLE insurance_data.insurance(IssuerId INT,IssuerId2 INT,BusinessDate STRING,stcd STRING,srcnm STRING,NetworkName STRING,NetworkURL STRING,custnum STRING,MarketCoverage STRING,issueridcomposite STRING,systs TIMESTAMP) PARTITIONED BY (sysdt DATE) STORED AS parquet LOCATION '/user/hduser/hivestore/insurance_data'")

    # Write the cleaned DataFrame to a table
    writedatatotable(filetype='parquet', location="insurance_data.insurance", mode='append', input_df=cleaned_df, partition_col_name='sysdt')

    # Stop the Spark session
    spark.stop()
