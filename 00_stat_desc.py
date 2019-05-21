import pandas as pd
from pyspark.sql import DataFrame
import pyspark.sql.functions as fn
from pyspark.sql.types import BooleanType, StringType

# pyspark2 --conf spark.dynamicAllocation.enabled=false --num-executors 15 --executor-cores 4 --conf spark.yarn.access.namenodes=hdfs://srv-louv-017.cb.local:8020 --conf spark.yarn.executor.memoryOverhead=4G
# pyspark2 --conf spark.dynamicAllocation.enabled=false --num-executors 60 --executor-cores 4 --executor-memory 6g --conf spark.yarn.access.namenodes=hdfs://srv-louv-017.cb.local:8020 --conf spark.yarn.executor.memoryOverhead=2g
# pyspark2 --conf spark.dynamicAllocation.enabled=false --num-executors 40 --executor-cores 4 --executor-memory 4g --conf spark.yarn.access.namenodes=hdfs://srv-louv-017.cb.local:8020 --conf spark.yarn.executor.memoryOverhead=2g

# Utils
def unionAll(*dfs):
    return reduce(DataFrame.unionAll, dfs)

# Audit des données - Fonctions
def summary_fn(df, fun, name, columns):
    res = (df.select([fun(x) for x in columns]).toPandas().T)
    res.columns = [name]
    return res

# * Nombre de NA
nb_na = lambda x: fn.sum(fn.col(x).isNull().cast('integer')).alias(x)

# * Nombre de modalités
nb_unique = lambda x: fn.countDistinct(x).alias(x)

# * Nombre approximé de modalités
nb_unique_approx = lambda x: fn.approx_count_distinct(x, rsd=0.1).alias(x)

# * Near Zero Variance
def near_zerovar(df, name):
    res = (df
    .groupBy(name)
    .agg((fn.count('*') / df.count()).alias('MostFrequent'))
    .sort(fn.col('MostFrequent').desc())
    .limit(1)
    .select(fn.concat_ws('_', 
            fn.col(name), 
            fn.round(fn.col('MostFrequent'), 2))
            .alias(name))
    .toPandas()
    .T)
    return res

def computeSummary(df, columns):
    df_summary = pd.DataFrame(df.dtypes, columns=['name', 'Type'])
    df_summary = df_summary.set_index("name")
    df_summary['Count'] = df.count()
    print("Compute CountNA")
    df_na = summary_fn(df, nb_na, "CountNA", columns)
    print("Compute CountDistinct")
    df_unique = summary_fn(df, nb_unique_approx, "CountDistinct", columns)
    print("Compute MostFrequent")
    df_nearzerovar = pd.concat([near_zerovar(df, x) for x in columns])
    df_nearzerovar.columns = ['MostFrequent']
    # Indicateurs from describe
    print("Compute more feat for num")
    df_describe_num = df.describe(columns).toPandas().T
    df_describe_num.columns = df_describe_num[df_describe_num.index == "summary"].values.tolist()[0]
    df_describe_num = df_describe_num.drop('summary', axis=0).drop('count', axis=1)
    print("Conso")
    df_summary = pd.concat([df_summary, df_na, df_unique, df_nearzerovar, df_describe_num], axis=1)
    df_summary['PctNA'] = df_summary['CountNA'] / df_summary['Count']
    return df_summary


list_tables = ["pdv.dam01", "pdv.inse01", "r_x_fraude.tfp01", 
               "x_autorisation.autovad01", "x_compensation.transactions"]

res = []
for name in list_tables:
    print("Compute for table {}".format(name))
    df = spark.table(name)
    df_summary = computeSummary(df, df.columns)
    res.append(df_summary)

# df_summary.to_csv("./statdec/dam01_stat.csv", index=True, encoding="utf8")
