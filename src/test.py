from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

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


def patient_extract(df, cols):
    exprs = [first(x,True).alias(y) for x,y in cols.items()]
    df_temp = (df.groupBy('entry_resource_id')
               .agg(*exprs))
    df_temp = df_temp.select([col(c).alias(cols.get(c,c)) for c in df_temp.columns])

        # df = (df.groupBy('entry_resource_id')
        #   .agg(first(df['entry_resource_birthDate'], True).alias('birthdate'),
        #        first(df['entry_resource_gender'], True).alias('gender'),
        #        first(df['entry_resource_multipleBirthInteger'], True).alias('multiple_birth'),
        #        first(df['entry_resource_maritalStatus_text'], True).alias('marital_status'),
        #        first(df['entry_resource_telecom_value'], True).alias('telephone'),
        #        first(df['entry_resource_name_prefix'], True).alias('prefix'),
        #        first(df['entry_resource_name_family'], True).alias('family_name'),
        #        first(df['entry_resource_name_given'], True).alias('given_name'),
        #        first(df['entry_resource_name_use'], True).alias('name_usage'),
        #        first(df['entry_resource_extension_valueAddress_city'], True).alias('city'),
        #        first(df['entry_resource_extension_valueAddress_country'], True).alias('country'),
        #        first(df['entry_resource_extension_valueAddress_state'], True).alias('state')
        #        ))
    return df_temp


def write_file(df, file_name):
    df.coalesce(1).write.format('com.databricks.spark.csv').option('sep', '|').option('header', 'True').save(
        file_name)
    print('File creation successful!')


df_json = spark.read.json('../data/*.json', multiLine=True)
df_output = master_array(df_json)
df_output.createOrReplaceTempView('data')
df_patient = spark.sql('''select * from data where entry_resource_resourceType = "Patient"''')
patient_cols = {'entry_resource_birthDate': 'birthdate', 'entry_resource_gender': 'gender',
                'entry_resource_multipleBirthInteger': 'multiple_birth',
                'entry_resource_maritalStatus_text': 'marital_status', 'entry_resource_telecom_value': 'telephone',
                'entry_resource_name_prefix': 'prefix',
                'entry_resource_name_family': 'family_name', 'entry_resource_name_given': 'given_name',
                'entry_resource_name_use': 'name_usage',
                'entry_resource_extension_valueAddress_city': 'city',
                'entry_resource_extension_valueAddress_country': 'country',
                'entry_resource_extension_valueAddress_state': 'state'}
df_patient = patient_extract(df_patient, patient_cols)
write_file(df_patient, 'patient.csv')