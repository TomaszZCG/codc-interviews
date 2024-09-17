import unittest
from pyspark.sql import SparkSession
"""import should be changed"""
from utils import filter_data  


class FilterDataTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[1]").appName("PySparkTest").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_filter_data(self):
        """
        Test if filter_data correctly filters and renames the columns.
        """
        # Create a test DataFrame
        test_data = [
            (1, "United Kingdom", "1ABC", "VISA"),
            (2, "Netherlands", "2ABC", "MasterCard"),
            (3, "United States", "3ABC", "VISA"),
            (4, "Germany", "4ABC", "AMEX")
        ]
        columns = ["id", "country", "btc_a", "cc_t"]
        df = self.spark.createDataFrame(test_data, columns)

        # Apply the filter_data function
        result_df = filter_data(df)

        # Expected data after filtering and renaming
        expected_data = [
            (1, "United Kingdom", "1ABC", "VISA"),
            (2, "Netherlands", "2ABC", "MasterCard")
        ]
        expected_columns = ["client_identifier", "country", "bitcoin_address", "credit_card_type"]
        expected_df = self.spark.createDataFrame(expected_data, expected_columns)

        # Assert DataFrame equality
        self.assertEqual(result_df.collect(), expected_df.collect())

    def test_column_renaming(self):
        """
        Test if the column renaming is done properly.
        """
        test_data = [
            (1, "United Kingdom", "1ABC", "VISA")
        ]
        columns = ["id", "country", "btc_a", "cc_t"]
        df = self.spark.createDataFrame(test_data, columns)

        # Apply the filter_data function
        result_df = filter_data(df)

        # Check if columns are renamed
        self.assertEqual(result_df.columns, ["client_identifier", "country", "bitcoin_address", "credit_card_type"])

    def test_no_data_for_non_matching_countries(self):
        """
        Test if no data is returned when countries do not match the filter condition.
        """
        test_data = [
            (1, "United States", "1ABC", "VISA"),
            (2, "Germany", "2ABC", "MasterCard")
        ]
        columns = ["id", "country", "btc_a", "cc_t"]
        df = self.spark.createDataFrame(test_data, columns)

        # Apply the filter_data function
        result_df = filter_data(df)

        # Check if the result is empty
        self.assertTrue(result_df.rdd.isEmpty())

if __name__ == "__main__":
    unittest.main()