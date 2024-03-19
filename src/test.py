from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import json
import uuid
from os import listdir
import helper as helper

spark = SparkSession.builder \
    .master('local[1]') \
    .appName('ptData') \
    .getOrCreate()


try:
    # Read config file
    config_file = open('extract_config.json')
    extract_config = json.load(config_file)

    # Read input files
    df_json = spark.read.json('../data/*.json', multiLine=True).withColumn('filename', input_file_name())
    print('input files read!')

    # Flatten hierarchy
    df_json = helper.master_array(df_json)
    print('files flattened!')

    # Re-format filename to remove path
    df_json = df_json.withColumn('filename', split(df_json['filename'], '/')).withColumn('filename', col('filename')[
        size('filename') - 1])
    print('filename update complete!')

    # Fetch distinct filenames
    filenames = [filename.filename for filename in df_json.select('filename').distinct().collect()]
    print('distinct filenames fetched!')
    # Update filename dict to 'filename:uuid' format
    filename_dict = {filename: str(uuid.uuid4()) for filename in filenames}

    # Generate expression string to update dataframe
    expr_str = ''
    for i in range(len(filename_dict)):
        expr_str_temp = f' WHEN filename = "{str(list(filename_dict.keys())[i])}" THEN "{str(list(filename_dict.values())[i])}" '
        expr_str = expr_str_temp + expr_str
    expr_str = 'CASE' + expr_str[:len(expr_str) - 1] + ' END'

    # Update dataframe with generated expression
    df_json = df_json.withColumn('filename', expr(expr_str))

    # Rename 'entry_resource_id' to 'patient_id'
    df_json = df_json.withColumnRenamed(df_json.columns[2], 'patient_id')
    print('ids added!')

    # Create view to run spark sql to filter based on resourceType
    df_json.createOrReplaceTempView('data')

    # Loop through resourceType, generate final extract and push file to s3
    for rtype in extract_config.keys():
        df_type = spark.sql(f'''select * from data where entry_resource_resourceType = "{str(rtype)}"''')
        df_final = helper.final_extract(df_type, extract_config[rtype])
        helper.write_file(df_final, str(rtype) + '_table' + '.csv')
        file_name = [name for name in listdir(f"../src/{str(rtype) + '_table'}.csv/") if name.endswith('.csv')]
        helper.upload_s3(f"../src/{str(rtype) + '_table'}.csv/{file_name[0]}",
                  f"s3://emis-input-bucket/{str(rtype) + '_table' + '.csv'}/")
except Exception as error:
    raise error
