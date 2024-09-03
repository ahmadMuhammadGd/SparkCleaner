import unittest
import json

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StringType, IntegerType, DoubleType

from SparkCleaner import CleaningPipeline
from SparkCleaner.Strategies import *

import warnings
warnings.simplefilter(action='ignore')

# Assuming the strategies and pipeline classes are defined as above

class TestCleaningStrategies(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("DataCleaningTests") \
            .master("local[*]") \
            .getOrCreate()
        cls.spark.sparkContext.setLogLevel("OFF")
        
        data = [
            (1, "Alice", 30, "2024-07-01", "alice@example.com"),
            (2, "Bob", None, "2024-07/02", "bob@example"),
            (3, "Charlie", '25z', "invalid_date", None),
            (4, "David", -5, "2024-07-04", "david@example.com"),
            (5, "Eve", 22, "2024-07-05", "eve@example.com"),
            (5, "Eve", 22, "2024-07-05", "eve@example.com"),  # Duplicate row
        ]
        schema = ["id", "name", "age", "date", "email"]
        cls.df = cls.spark.createDataFrame(data, schema=schema)

    def test_drop_duplicates(self):
        strategy = DropDuplicatesStrategy(columns=self.df.columns)
        pipeline = CleaningPipeline()
        pipeline.add_strategy(strategy)
        pipeline.set_dataframe(self.df)

        cleaned_df = pipeline.run()
        result = cleaned_df.count()
        self.assertEqual(result, 5)  # Should drop the duplicate row

    def test_drop_missing_values(self):
        strategy = DropMissingValuesStrategy(columns=["age"])
        pipeline = CleaningPipeline()
        pipeline.add_strategy(strategy)
        pipeline.set_dataframe(self.df)

        cleaned_df = pipeline.run()
        result = cleaned_df.filter(col("age").isNull()).count()
        self.assertEqual(result, 0)  # Should drop rows with null values in 'age'

    def test_validate_column_types(self):
        strategy = ValidateColumnTypesStrategy(columns=self.df.columns, expected_types={'age': IntegerType()})
        pipeline = CleaningPipeline()
        pipeline.add_strategy(strategy)
        pipeline.set_dataframe(self.df)

        cleaned_df = pipeline.run()
        errors = strategy.get_report()
        self.assertGreater(len(errors), 0)  # Should log errors due to invalid type in 'age'

    def test_validate_dates(self):
        strategy = ValidateDatesStrategy(columns=["date"], date_format='yyyy-MM-dd')
        pipeline = CleaningPipeline()
        pipeline.add_strategy(strategy)
        pipeline.set_dataframe(self.df)

        cleaned_df = pipeline.run()
        errors = strategy.get_report()
        self.assertGreater(len(errors), 0)  # Should log errors due to invalid date format

    def test_validate_regex(self):
        strategy = ValidateRegexStrategy(columns=["email"], patterns={'email': '^[\\w.%+-]+@[\\w.-]+\\.[a-zA-Z]{2,}$'})
        pipeline = CleaningPipeline()
        pipeline.add_strategy(strategy)
        pipeline.set_dataframe(self.df)

        cleaned_df = pipeline.run()
        errors = strategy.get_report()
        self.assertGreater(len(errors), 0)  # Should log errors for invalid email formats

    def test_filter_negative_values(self):
        strategy = FilterNegativeValuesStrategy(columns=["age"])
        pipeline = CleaningPipeline()
        pipeline.add_strategy(strategy)
        pipeline.set_dataframe(self.df)

        cleaned_df = pipeline.run()
        result = cleaned_df.filter(col("age") < 0).count()
        self.assertEqual(result, 0)  # Should filter out negative values in 'age'
    
    def test_filter_on_condition(self):
        strategy = FilteringStrategy(conditions=[col("age")>0])
        pipeline = CleaningPipeline()
        pipeline.add_strategy(strategy)
        pipeline.set_dataframe(self.df)

        cleaned_df = pipeline.run()
        result = cleaned_df.filter(col("age") < 0).count()
        self.assertEqual(result, 0)  # Should filter out negative values in 'age'


def test_replace_NA(self):
    strategy = FillNAStrategy(fill_values={'email': 'invalid_value', 'age': -9999})
    pipeline = CleaningPipeline()
    pipeline.add_strategy(strategy)
    pipeline.set_dataframe(self.df)
    cleaned_df = pipeline.run()

    age_null_count = cleaned_df.filter(col("age").isNull()).count()
    age_replacement_count = cleaned_df.filter(col("age") == -9999).count()

    email_null_count = cleaned_df.filter(col("email").isNull()).count()
    email_replacement_count = cleaned_df.filter(col("email") == 'invalid_value').count()

    self.assertEqual(age_null_count, 0, "There should be no NULL values in the 'age' column after filling.")
    self.assertGreater(age_replacement_count, 0, "There should be rows where 'age' was replaced with -9999.")
    
    self.assertEqual(email_null_count, 0, "There should be no NULL values in the 'email' column after filling.")
    self.assertGreater(email_replacement_count, 0, "There should be rows where 'email' was replaced with 'invalid_value'.")

def test_replace_NA_none_or_empty(self):
    strategy_none = FillNAStrategy(fill_values=None)
    pipeline_none = CleaningPipeline()
    pipeline_none.add_strategy(strategy_none)
    pipeline_none.set_dataframe(self.df)

    cleaned_df_none = pipeline_none.run()

    original_null_count_age = self.df.filter(col("age").isNull()).count()
    original_null_count_email = self.df.filter(col("email").isNull()).count()
    cleaned_null_count_age_none = cleaned_df_none.filter(col("age").isNull()).count()
    cleaned_null_count_email_none = cleaned_df_none.filter(col("email").isNull()).count()

    self.assertEqual(original_null_count_age, cleaned_null_count_age_none, "The 'age' column should remain unchanged when fill_values is None.")
    self.assertEqual(original_null_count_email, cleaned_null_count_email_none, "The 'email' column should remain unchanged when fill_values is None.")

    strategy_empty = FillNAStrategy(fill_values={})
    pipeline_empty = CleaningPipeline()
    pipeline_empty.add_strategy(strategy_empty)
    pipeline_empty.set_dataframe(self.df)

    cleaned_df_empty = pipeline_empty.run()

    cleaned_null_count_age_empty = cleaned_df_empty.filter(col("age").isNull()).count()
    cleaned_null_count_email_empty = cleaned_df_empty.filter(col("email").isNull()).count()

    self.assertEqual(original_null_count_age, cleaned_null_count_age_empty, "The 'age' column should remain unchanged when fill_values is an empty dictionary.")
    self.assertEqual(original_null_count_email, cleaned_null_count_email_empty, "The 'email' column should remain unchanged when fill_values is an empty dictionary.")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

if __name__ == '__main__':
    unittest.main()
