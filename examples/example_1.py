from SparkCleaner import *
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType

data = [
    (1, "Alice", 30, "2024-07-01", "alice@example.com"),
    (2, "Bob", None, "2024-07/02", "bob@example"), # null in age col
    (3, "Charlie", 25, "invalid_date", None), # empty email and invalid date
    (4, "David", -5, "2024-07-04", "david@example.com"),
    (5, "Eve", 22, "2024-07-05", "eve@example.com"),
    (5, "Eve", 22, "2024-07-05", "eve@example.com"),
    (5, "Eve", '22', "05-07-2024", "eve@example.com"),  #string value in age column
]
schema = ["id", "name", "age", "date", "email"]

spark = SparkSession.builder \
    .appName("DataCleaningTests") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("OFF")

df = spark.createDataFrame(data, schema=schema)

# constructing cleaning pipeline

cleaning_pipeline = CleaningPipeline()
strategies_pipeline =[
    DropDuplicatesStrategy(columns=df.columns),
    ValidateRegexStrategy(columns=["email"], patterns={'email': '^[\\w.%+-]+@[\\w.-]+\\.[a-zA-Z]{2,}$'}),
    DropMissingValuesStrategy(columns=["age"]),
    FilterNegativeValuesStrategy(columns=["age"]),
    ValidateColumnTypesStrategy(columns=df.columns, 
            expected_types={
                'age': IntegerType(), 
                'email': StringType()
                # add more if needed
    }),
    ValidateDatesStrategy(columns=["date"], date_format='yyyy-MM-dd'),
]

# 
cleaning_pipeline.add_strategy(strategy=strategies_pipeline)
cleaning_pipeline.set_dataframe(df = df)

#
cleaned_df = cleaning_pipeline.run()
errors_report = cleaning_pipeline.get_report()
print("Error report", errors_report)
print("Cleaned dataframe: \n")
cleaned_df.show()
