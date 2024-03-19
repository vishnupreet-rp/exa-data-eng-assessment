from pyspark.sql.functions import *
import boto3


def child_struct(nested_df):
    try:
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
    except Exception as error:
        raise error


def master_array(df):
    try:
        array_cols = [c[0] for c in df.dtypes if c[1][:5] == "array"]
        while len(array_cols) > 0:
            for c in array_cols:
                df = df.withColumn(c, explode_outer(c))
            df = child_struct(df)
            array_cols = [c[0] for c in df.dtypes if c[1][:5] == "array"]
        return df
    except Exception as error:
        raise error


def final_extract(df, cols):
    try:
        expression = [first(x, True).alias(y) for x, y in cols.items()]
        df_temp = (df.groupBy('patient_id')
                   .agg(*expression))
        df_temp = df_temp.select([col(c).alias(cols.get(c, c)) for c in df_temp.columns])
        return df_temp
    except Exception as error:
        raise error


def write_file(df, file_name):
    try:
        df.coalesce(1).write.format('com.databricks.spark.csv').option('sep', '|').option('header', 'True').mode(
            'overwrite').save(
            file_name)
        print(f'{file_name} creation successful!')
    except Exception as error:
        raise error


def upload_s3(file, path):
    try:
        # s3_bucket = s3: // emis - input - bucket / Patient_table.csv / patient_table.csv
        s3_bucket = path.split('//')[1].split('/')[0]
        s3_path = f"{path.split('//')[1].split('/')[1]}/{path.split('//')[1].split('/')[1].lower()}"
        print(f"{file.split('/')[-1]} - {s3_path}")
        s3 = boto3.resource('s3')
        s3.Bucket(s3_bucket).upload_file(file, s3_path)
    except Exception as error:
        print(f'exception occurred while s3 upload - {error}')