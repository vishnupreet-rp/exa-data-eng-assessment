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
    expression = [first(x, True).alias(y) for x,y in cols.items()]
    df_temp = (df.groupBy('entry_resource_id')
               .agg(*expression))
    df_temp = df_temp.select([col(c).alias(cols.get(c,c)) for c in df_temp.columns])
    return df_temp


def write_file(df, file_name):
    df.coalesce(1).write.format('com.databricks.spark.csv').option('sep', '|').option('header', 'True').mode('overwrite').save(
        file_name)
    print(f'{file_name} creation successful!')


config_file = open('extract_config.json')
extract_config = json.load(config_file)
print('extract_config read!')
df_json = spark.read.json('../data/*.json', multiLine=True)
print('input files read!')
df_output = master_array(df_json)
print('files flattened!')
df_output.createOrReplaceTempView('data')
# resourceType = spark.sql('''select distinct entry_resource_resourceType from data''').rdd.flatMap(lambda x: x).collect()
for rtype in extract_config.keys():
    df_type = spark.sql(f'''select * from data where entry_resource_resourceType = "{str(rtype)}"''')

# df_patient = spark.sql('''select * from data where entry_resource_resourceType = "Patient"''')
# patient_cols = {'entry_resource_birthDate': 'birthdate', 'entry_resource_gender': 'gender',
#                 'entry_resource_multipleBirthInteger': 'multiple_birth',
#                 'entry_resource_maritalStatus_text': 'marital_status', 'entry_resource_telecom_value': 'telephone',
#                 'entry_resource_name_prefix': 'prefix',
#                 'entry_resource_name_family': 'family_name', 'entry_resource_name_given': 'given_name',
#                 'entry_resource_name_use': 'name_usage',
#                 'entry_resource_extension_valueAddress_city': 'city',
#                 'entry_resource_extension_valueAddress_country': 'country',
#                 'entry_resource_extension_valueAddress_state': 'state'}
    df_final = final_extract(df_type, extract_config[rtype])
    write_file(df_final, str(rtype) + '_table' + '.csv')