from unittest import TestCase
import unittest

from pyspark.sql import SparkSession

class UnitTestCases(TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder\
                            .appName("Unit test")\
                            .master("local[2]")\
                            .getOrCreate()
    @classmethod
    def tearDownClass(cls):
        return cls.spark.stop()

    def test_cases_1(self):
        self.assertEqual(1,1,"Pass Case")
        self.assertEqual(2,2,"Pass Case")

    def test_cases_2(self):
        self.assertEqual(1,1,"Pass Case")
        self.assertEqual(2,22,"Fail Case")

if __name__ == "__main__":
    unittest.main()