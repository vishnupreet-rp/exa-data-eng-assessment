import unittest
from pyspark.sql.functions import *
from unittest import TestCase
import src.helper as helper
import json
from pyspark.sql import SparkSession


class test(TestCase):

    def test_child_struct(self):
        spark = SparkSession.builder \
            .master('local[1]') \
            .appName('test') \
            .getOrCreate()
        df = spark.read.json('test_json.json', multiLine=True).withColumn('filename', input_file_name())
        flat_df = helper.child_struct(df)
        self.assertEqual(len(flat_df.columns), 4)

    def test_master_array(self):
        spark = SparkSession.builder \
            .master('local[1]') \
            .appName('test') \
            .getOrCreate()
        df = spark.read.json('test_json.json', multiLine=True).withColumn('filename', input_file_name())
        flat_df = helper.master_array(df)
        self.assertEqual(len(flat_df.columns), 15)

    # def test_final_extract(self):
    #     spark = SparkSession.builder \
    #         .master('local[1]') \
    #         .appName('test') \
    #         .getOrCreate()
    #     df = spark.read.json('test_json.json', multiLine=True).withColumn('filename', input_file_name())
    #     file = open("../src/extract_config.json")
    #     cols = json.load(file)
    #     df_rt = helper.final_extract(df, cols)
    #     df_rt.show()
    #     self.assertEqual(len(df_rt.columns), 2)
