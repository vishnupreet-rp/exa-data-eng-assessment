import pandas as pd
import uuid

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import json

spark = SparkSession.builder \
    .master('local[1]') \
    .appName('ptData') \
    .getOrCreate()


def child_struct(nested_df):
    # Creating python list to store dataframe metadata
    list_schema = [((), nested_df)]
    # Creating empty python list for final flatten columns
    flat_columns = []

    while len(list_schema) > 0:
        # Removing latest or recently added item (dataframe schema) and returning into df variable
        parents, df = list_schema.pop()
        flat_cols = [col(".".join(parents + (c[0],))).alias("_".join(parents + (c[0],))) for c in df.dtypes if
                     c[1][:6] != "struct"]

        struct_cols = [c[0] for c in df.dtypes if c[1][:6] == "struct"]

        flat_columns.extend(flat_cols)
        # Reading  nested columns and appending into stack list
        for i in struct_cols:
            projected_df = df.select(i + ".*")
            list_schema.append((parents + (i,), projected_df))
    return nested_df.select(flat_columns)


def master_array(df):
    array_cols = [c[0] for c in df.dtypes if c[1][:5] == "array"]
    while len(array_cols) > 0:
        for c in array_cols:
            df = df.withColumn(c, explode_outer(c))
        df = child_struct(df)
        array_cols = [c[0] for c in df.dtypes if c[1][:5] == "array"]
    return df


def final_extract(df, cols):
    expression = [first(x, True).alias(y) for x, y in cols.items()]
    df_temp = (df.groupBy('patient_id')
               .agg(*expression))
    df_temp = df_temp.select([col(c).alias(cols.get(c, c)) for c in df_temp.columns])
    return df_temp


def write_file(df, file_name):
    df.coalesce(1).write.format('com.databricks.spark.csv').option('sep', '|').option('header', 'True').mode(
        'overwrite').save(
        file_name)
    print(f'{file_name} creation successful!')


def required_columns(extract_config):
    req_cols = []
    for key in extract_config.keys():
        for col in list(extract_config[key].keys()):
            req_cols.append(col)
    return req_cols


def update_id(filename):
    print(f'{filename} - update_id')
    return filename_dict[filename]


config_file = open('extract_config.json')
extract_config = json.load(config_file)
required_columns = required_columns(extract_config)
# emp_RDD = spark.sparkContext.emptyRDD()
# schema = StructType([])
# df_empty = spark.createDataFrame(emp_RDD, schema)

# for file in os.listdir('../data/'):
#     df_temp = spark.read.json('../data/' + file, multiLine=True)
#     df_temp1 = master_array(df_temp)
#     df_temp1.where(df_temp1.entry_resource_resourceType.between('Patient', 'Patient')).show()
#     df_cols = df_temp1.columns
#     cols_to_select = set(df_cols) & set(required_columns)
#     df_output = df_temp1.select('entry_resource_id','entry_resource_resourceType',*cols_to_select)
#     df_output.createOrReplaceTempView('data')
#     resource_id = spark.sql(
#         '''select entry_resource_id from data where entry_resource_resourceType="Patient" limit 1''').collect()
#     resource_id = str(resource_id[0].__getitem__('entry_resource_id'))
#     df_output_new = df_output.withColumn('entry_resource_id',
#                                          when(df_output['entry_resource_resourceType'] != 'Patient', resource_id))
#     df_empty = df_empty.unionByName(df_output_new, allowMissingColumns=True)
# df_empty.show()

# config_file = open('extract_config.json')
# extract_config = json.load(config_file)
# print('extract_config read!')
df_json = spark.read.json('../data/*.json', multiLine=True).withColumn('filename', input_file_name())
print('input files read!')
df_json = master_array(df_json)
print('files flattened!')
# df_json1 = df_json.withColumn('filename', split(df_json['filename'], '/')).select(element_at(col('filename'), -1).alias('filename'))
df_json = df_json.withColumn('filename', split(df_json['filename'], '/')).withColumn('filename', col('filename')[
    size('filename') - 1])
print('filename split complete!')
# df_json.show()
# pt_id = str(uuid.uuid4())
# df_json1.withColumn('entry_resource_id', when(df_json1.filename == 'Gilma310_Hahn503_0d55a582-07fe-a897-776c-3ab5e48cd457.json', '0d55a582-07fe-a897-776c-3ab5e48cd457'))
# df_json1.select('entry_resource_id').where(df_json1.filename == 'Gilma310_Hahn503_0d55a582-07fe-a897-776c-3ab5e48cd457.json').show()
# df_json1.show()
# df_json.createOrReplaceTempView('df_json')
filenames = [filename.filename for filename in df_json.select('filename').distinct().collect()]
print('distinct filenames fetched!')
filename_dict = {filename: str(uuid.uuid4()) for filename in filenames}

# for filename in filenames:
#     pt_id = filename_dict[filename]
#     print(f'{filename}: {pt_id}')
# df_pd = df_json.toPandas()
# print('converted to pandas!')
# for filename in filenames:
#     pt_id = filename_dict[filename]
#     df_pd.loc[df_pd['filename']==filename, df_pd['entry_resource_id']]=pt_id
# df_json = spark.createDataFrame(df_pd)
expr_str = ''
for i in range(len(filename_dict)):
    expr_str_temp = f' WHEN filename = "{str(list(filename_dict.keys())[i])}" THEN "{str(list(filename_dict.values())[i])}" '
    expr_str = expr_str_temp + expr_str
expr_str = 'CASE' + expr_str[:len(expr_str)-1] + ' END'
print(expr_str)
df_json = df_json.withColumn('filename', expr(expr_str))
df_json = df_json.withColumnRenamed(df_json.columns[2], 'patient_id')
# df_json.select('filename', 'entry_resource_id').where(df_json.filename == '62ac6b46-f171-4434-b166-dfa8a6ae6fe5').show()
print('ids added!')
df_json.limit(10).show()
# df_json1.select('entry_resource_id').where(df_json1.filename == 'Gilma310_Hahn503_0d55a582-07fe-a897-776c-3ab5e48cd457.json').show()
# df_json = df_json.limit(2000)
# write_file(df_json, 'id_test.csv')

# # print('files flattened!')
df_json.createOrReplaceTempView('data')
#     print(spark.sql('''select entry_resource_id from data''').collect())
for rtype in extract_config.keys():
    df_type = spark.sql(f'''select * from data where entry_resource_resourceType = "{str(rtype)}"''')
    df_final = final_extract(df_type, extract_config[rtype])
    write_file(df_final, str(rtype) + '_table' + '.csv')
