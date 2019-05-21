import pandas as pd
from pyspark.sql import DataFrame
import pyspark.sql.functions as fn
from pyspark.sql.types import BooleanType, StringType, LongType
from pyspark.sql.functions import col

# Loading

fraude = spark.read.csv("/user/qyoshida/TFP_180726.txt", sep="\t", header=True)
df = spark.table('x_compensation.transactions')

# Filter
# Fraude
# Out[7]: 21238736
fraude = fraude.filter((fn.col("TFPTEC") == "1") & (fn.col("TFPNATDECL").isin(["1", "3"])))
fraude = fraude.withColumn("TFPRFDA_LONG", fraude["TFPRFDA"].cast(LongType()))
fraude = fraude.withColumn("TFPDTVT_YYDDMM", fn.concat(col("TFPDTVT").substr(9, 2), col("TFPDTVT").substr(4, 2), col("TFPDTVT").substr(1, 2)))
# count = 10532174
fraude = fraude.select("TFPRFDA_LONG", "TFPBQEAQ", "TFPDTVT_YYDDMM").dropDuplicates()
# count = 10127750
fraude = fraude.repartition(600).cache()

# Transaction
df = df.withColumn("REF_ACQ", fn.trim(fn.col("s06_identifiant_etablissement_donneur_dordre")))
df = df.filter(fn.col("b25_numero_reference_archivage_transaction").isNotNull())
# count = 14322580092
df = df.select("b25_numero_reference_archivage_transaction", "REF_ACQ", "b05_date_locale_transaction").dropDuplicates()
# count = 12893080712

# Merge
fraude_with_transaction = fraude.join(df, (fraude["TFPRFDA_LONG"] == df["b25_numero_reference_archivage_transaction"]) & (fraude["TFPBQEAQ"] == df["REF_ACQ"]) & (fraude["TFPDTVT_YYDDMM"] == df["b05_date_locale_transaction"]), "inner")
# count = 3624439

# Check
fraude.select("TFPRFDA_LONG", "TFPBQEAQ", "TFPDTVT_YYDDMM")

# fraude.select("TFPRFDA_LONG", "TFPBQEAQ", "TFPDTVT_YYDDMM").dropDuplicates().count()
#  20609594
# 20609594 / float(21238736)
# 0.9703776156923839

df.select("b25_numero_reference_archivage_transaction", "REF_ACQ", "b05_date_locale_transaction")
# df.select("b25_numero_reference_archivage_transaction", "REF_ACQ", "b05_date_locale_transaction").dropDup
# licates().count()
# 12893096239
# 12893096239 / float(14990474359)


df.groupBy("b25_numero_reference_archivage_transaction", "REF_ACQ", "b05_date_locale_transaction").count().sort(fn.col("count").desc()).show(10, False)
# +------------------------------------------+-------+---------------------------+-------+
# |b25_numero_reference_archivage_transaction|REF_ACQ|b05_date_locale_transaction|count  |
# +------------------------------------------+-------+---------------------------+-------+
# |null                                      |15589  |170322                     |1326739|
# |null                                      |15589  |161223                     |1101603|
# |null                                      |15589  |161210                     |1079860|
# |null                                      |15589  |161217                     |1071386|
# |null                                      |15589  |170506                     |1058747|
# |null                                      |15589  |161203                     |1056703|
# |null                                      |15589  |170701                     |1044082|
# |null                                      |15589  |170114                     |1013239|
# |null                                      |15589  |170415                     |1004629|
# |null                                      |15589  |171223                     |1004406|
# +------------------------------------------+-------+---------------------------+-------+

