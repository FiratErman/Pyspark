import pandas as pd
from itertools import chain
from pyspark.sql import DataFrame
import pyspark.sql.functions as fn
from pyspark.sql.types import BooleanType, StringType, ArrayType

# Utils ====
def merge_cust(df1, df2, key1, key2, type="left"):
    on = [df1[key1] == df2[key1], df1[key2] == df2[key2]]
    return df1.join(df2, on, type).drop(df2[key1]).drop(df2[key2])

def concat_set(*args):
    return list(set(chain(*args)))

udf_concat = fn.udf(concat_set, ArrayType(StringType()))

# Loading ====
# DAM
id_pdv = spark.read.parquet("./id_pdv_dam")
id_pdv = id_pdv.filter(fn.col("famille_ert").isin(["PROXI", "VAD"]))
# Only two malformed ID_PDV_BQE
id_pdv = id_pdv.filter(fn.length(fn.col("ID_PDV_BQE")) != 6)


# TRANSACTIONS - COMPENSATION
id_pdv_transact = spark.read.parquet("./id_pdv_transact")
# Cleaning
id_pdv_transact = id_pdv_transact.filter(fn.length(fn.col("ID_PDV_BQE")) != 0)
id_pdv_transact = id_pdv_transact.filter(fn.col("famille_ert").isin(["PROXI", "VAD"]))

# Cache
id_pdv.cache().count()
id_pdv_transact.cache().count()

# Get stat desc
# Naive merge
on = [id_pdv["SRT"] == id_pdv_transact["SRT"], id_pdv["ID_PDV_BQE"] == id_pdv_transact["ID_PDV_BQE"]]
dfRes = (id_pdv
         .join(id_pdv_transact
               .select(fn.col("SRT"), 
                       fn.col("ID_PDV_BQE"),
                       fn.col("MCC").alias("MCC_TRANSACT"), 
                       fn.col("RSN").alias("RSN_TRANSACT")), on, "left")
         .drop(id_pdv_transact["SRT"])
         .drop(id_pdv["ID_PDV_BQE"]))
dfRes = dfRes.na.fill("", subset=["RSN_TRANSACT"])

# +--------------+-----------+----+--------------------+----------+--------------+----------+------------+----------------+
# |           SRT|famille_ert| MCC|                 RSN|ID_PDV_BQE|           SRT|ID_PDV_BQE|MCC_TRANSACT|    RSN_TRANSACT|
# +--------------+-----------+----+--------------------+----------+--------------+----------+------------+----------------+
# |05008621400702|      PROXI|5542|                    |   9230092|05008621400702|   9230092|        5542|       STATION U|
# |05008621416054|      PROXI|5541|                    |   9230358|05008621416054|   9230358|        5541|       STATION U|
# |30340834800017|      PROXI|8641|    AEROCLUB RENAULT|   5877588|30340834800017|   5877588|        8641|       AERODROME|
# |33159363200010|        VAD|5969|SARL CLUB DE VAUG...|   0000000|          null|      null|        null|            null|
# |39916157900038|      PROXI|1771|CEMEX BETONS SUD EST|   4790608|39916157900038|   4790608|        1771|     J057-CXB SE|
# |40294180100027|      PROXI|5542| MANCIPOZ TRANSPORTS|   4283933|40294180100027|   4283933|        5542|     MAN



# Focus ERT ====

# PROXI vs VAD
# Combo SRT + ID_PDV_BQE
srt_contrat_dam = id_pdv.select("SRT", "ID_PDV_BQE", "famille_ert").dropDuplicates()

srt_contrat_transact = (id_pdv_transact
                        .select("SRT", "ID_PDV_BQE", 
                                fn.col("famille_ert").alias("ERT_TRANSACT"))
                        .dropDuplicates())


# Transactions has better completion regarding ID_PDV_BQE
# /!\ Reflechir au fait que ID_PDV_BQE 0000000 est propre à DAM et les conséquences sur la jointure
missing_idpdv_bqe = (srt_contrat_dam
                     .filter(fn.col("ID_PDV_BQE") == "0000000")
                     .select("SRT")
                     .dropDuplicates())

srt_transact_unique_ert = (id_pdv_transact
                          .groupBy("SRT")
                          .agg(fn.countDistinct("MCC").alias("nunique_mcc"), 
                               fn.countDistinct("famille_ert").alias("nunique_ert"))
                          .filter(fn.col("nunique_ert") == 1)
                          .select("SRT"))
  
missing_recoverable = (srt_transact_unique_ert
                       .join(missing_idpdv_bqe, 
                           srt_transact_unique_ert["SRT"] == missing_idpdv_bqe["SRT"], 
                           "inner")
                       .drop(missing_idpdv_bqe["SRT"])
                       .select(fn.col("SRT").alias("SRT_OK")))

srt_contrat_transact = (srt_contrat_transact
                        .join(missing_recoverable, 
                              srt_contrat_transact["SRT"] == missing_recoverable["SRT_OK"], 
                              "left"))

srt_contrat_transact = (srt_contrat_transact
                        .withColumn("ID_PDV_BQE", 
                                fn.when(fn.col("SRT_OK").isNotNull(), 
                                        fn.lit("0000000"))
                                .otherwise(fn.col("ID_PDV_BQE")))
                        .drop(fn.col("SRT_OK"))
                        .dropDuplicates())

srt_contrat_merged = merge_cust(srt_contrat_dam, srt_contrat_transact, "SRT", "ID_PDV_BQE")
srt_contrat_merged = srt_contrat_merged.dropDuplicates()
srt_contrat_merged = srt_contrat_merged.filter(fn.col("famille_ert").isNotNull() & fn.col("ERT_TRANSACT").isNotNull())

# In [7]: srt_contrat_merged.show()
# +--------------+----------+-----------+------------+
# |           SRT|ID_PDV_BQE|famille_ert|ERT_TRANSACT|
# +--------------+----------+-----------+------------+
# |00578096000117|   0358051|      PROXI|       PROXI|
# |00665009700014|   7871334|      PROXI|         VAD|
# |00665009700014|   7871334|        VAD|         VAD|
# |03718001500010|   1671221|      PROXI|       PROXI|
# |04615022300025|   1488175|      PROXI|       PROXI|
# |05008582800064|   0000000|      PROXI|       PROXI|
# |05008582800304|   0000000|      PROXI|       PROXI|
# |05008582802185|   0000000|      PROXI|       PROXI|
# |05008582803100|   0000000|      PROXI|       PROXI|

# srt_contrat_merged.select(fn.sum((fn.col("famille_ert") != fn.col("ERT_TRANSACT")).cast("integer"))).show
# 90073

# srt_contrat_merged.count()
# 1509905


srt_contrat_merged = (srt_contrat_merged
                      .groupBy("SRT", "ID_PDV_BQE")
                      .agg(fn.collect_set("famille_ert").alias("ERT_DAM_SET"), 
                          fn.collect_set("ERT_TRANSACT").alias("ERT_TRANSACT_SET")))
srt_contrat_merged = (srt_contrat_merged
                      .withColumn("ERT_CONSO", 
                                  udf_concat(fn.col("ERT_DAM_SET"), 
                                            fn.col("ERT_TRANSACT_SET"))))

srt_contrat_merged = (srt_contrat_merged.withColumn("MAX_SIZE_DAM_TRANSACT", 
                            fn.when(fn.size("ERT_DAM_SET") >= fn.size("ERT_TRANSACT_SET"),
                            fn.size("ERT_DAM_SET")).otherwise(fn.size("ERT_TRANSACT_SET"))))

srt_contrat_merged = srt_contrat_merged.withColumn("SIZE_CONSO", fn.size("ERT_CONSO"))
srt_contrat_merged.filter(fn.col("MAX_SIZE_DAM_TRANSACT") != fn.col("SIZE_CONSO")).count() / float(srt_contrat_merged.count())
# 499 / float(1419404)

srt_contrat_merged.filter(fn.size(fn.col("ERT_DAM_SET")) != fn.size(fn.col("ERT_TRANSACT_SET"))).count()
# srt_contrat_merged.filter(fn.size(fn.col("ERT_DAM_SET")) != fn.size(fn.col("ERT_TRANSACT_SET"))).count()
# 87720
# srt_contrat_merged.filter(fn.size(fn.col("ERT_DAM_SET")) > fn.size(fn.col("ERT_TRANSACT_SET"))).count()
# 87398
# srt_contrat_merged.filter(fn.size(fn.col("ERT_DAM_SET")) < fn.size(fn.col("ERT_TRANSACT_SET"))).count()
# 322
# => Plus d'infos dans la base DAM



# Focus MCC ====


# MCC must have length == 4
# id_pdv_transact.filter(fn.length(fn.col("MCC")) != 4).groupBy("MCC").count().show()
# +---+-----+
# |MCC|count|
# +---+-----+
# |  0|   24|
# +---+-----+

id_pdv_transact_mcc = (id_pdv_transact
                       .filter(fn.col("MCC") != 0)
                       .drop(*["RSN", "ERT", "famille_ert"])
                       .dropDuplicates())
id_pdv_transact_mcc = id_pdv_transact_mcc.withColumnRenamed("MCC", "MCC_TRANSACT")
# In [261]: id_pdv.groupBy("MCC").count().sort(fn.col("count").desc()).select(fn.col("mcc").cast("integer")).sort(fn
#      ...: .col("mcc")).show()
# +----+
# | mcc|
# +----+
# | 742|
# | 743|
# | 763|
# | 780|
# |1520|
# |1711|
# => No missing MCC for DAM

id_pdv_mcc = id_pdv.drop(*["RSN", "famille_ert"]).dropDuplicates()
id_pdv_mcc = id_pdv_mcc.withColumnRenamed("MCC", "MCC_DAM")

# Take care of missing ID_PDV_BQE
missing_idpdv_bqe = (id_pdv_mcc
                     .filter(fn.col("ID_PDV_BQE") == "0000000")
                     .select("SRT")
                     .dropDuplicates())

srt_transact_unique_mcc = (id_pdv_transact
                          .groupBy("SRT")
                          .agg(fn.countDistinct("MCC").alias("nunique_mcc"), 
                               fn.countDistinct("famille_ert").alias("nunique_ert"))
                          .filter(fn.col("nunique_mcc") == 1)
                          .select("SRT"))

missing_recoverable = (srt_transact_unique_mcc
                       .join(missing_idpdv_bqe, 
                           srt_transact_unique_mcc["SRT"] == missing_idpdv_bqe["SRT"], 
                           "inner")
                       .drop(missing_idpdv_bqe["SRT"])
                       .select(fn.col("SRT").alias("SRT_OK")))

id_pdv_transact_mcc = (id_pdv_transact_mcc
                        .join(missing_recoverable, 
                              id_pdv_transact_mcc["SRT"] == missing_recoverable["SRT_OK"], 
                              "left"))

id_pdv_transact_mcc = (id_pdv_transact_mcc
                        .withColumn("ID_PDV_BQE", 
                                fn.when(fn.col("SRT_OK").isNotNull(), 
                                        fn.lit("0000000"))
                                .otherwise(fn.col("ID_PDV_BQE")))
                        .drop(fn.col("SRT_OK"))
                        .dropDuplicates())

resMcc = merge_cust(id_pdv_mcc, id_pdv_transact_mcc, "SRT", "ID_PDV_BQE", "inner")

# In [152]: resMcc.count()
# Out[152]: 1498544

# In [153]: 1429429 / float(resMcc.count())
# Out[153]: 0.9538785647935596

resMcc = resMcc.groupBy("SRT", "ID_PDV_BQE").agg(fn.collect_set("MCC_DAM").alias("MCC_DAM"), fn.collect_set("MCC_TRANSACT").alias("MCC_TRANSACT"))
resMcc2 = resMcc

# resMcc.show()
# +--------------+----------+------------+------------+
# |           SRT|ID_PDV_BQE|     MCC_DAM|MCC_TRANSACT|
# +--------------+----------+------------+------------+
# |00578096000117|   0358051|      [7011]|      [7011]|
# |03718001500010|   1671221|      [0763]|      [0763]|
# |04615022300025|   1488175|      [5944]|      [5944]|
# |05008585134156|   4500394|      [5814]|      [5814]|
# |05008585198961|   4237534|      [8021]|      [8021]|
# |05008591907173|   8820613|      [7230]|      [7230]|
# |05008591912645|   8829006|[5651, 5714]|      [5714]|

# resMcc.select(fn.size("MCC_DAM").alias("size")).groupBy("size").count().show()
# +----+-------+
# |size|  count|
# +----+-------+
# |   1|1021543|
# |   6|      1|
# |   3|    289|
# |   5|      3|
# |   4|     29|
# |   7|      2|
# |   2|  16031|
# +----+-------+

# resMcc.select(fn.size("MCC_TRANSACT").alias("size")).groupBy("size").count().sort(fn.col("count").desc()
#      ...: ).show()
# +----+-------+
# |size|  count|
# +----+-------+
# |   1|1027080|
# |   2|  10650|
# |   3|     96|
# |   4|     16|
# |   5|      9|
# |   6|      7|
# |  35|      4|
# |  13|      2|
# |  49|      2|
# |  15|      2|
# |  17|      2|
# |   8|      2|
# |  25|      2|
# |  29|      2|
# |  12|      2|
# |  45|      2|
# |   7|      2|
# |  44|      1|
# |  36|      1|
# |  56|      1|
# +----+-------+

resMcc = resMcc.withColumn("MCC_CONSO", udf_concat(fn.col("MCC_DAM"), fn.col("MCC_TRANSACT")))
# resMcc.show()
# +--------------+----------+------------+------------+------------+
# |           SRT|ID_PDV_BQE|     MCC_DAM|MCC_TRANSACT|   MCC_CONSO|
# +--------------+----------+------------+------------+------------+
# |00578096000117|   0358051|      [7011]|      [7011]|      [7011]|
# |03718001500010|   1671221|      [0763]|      [0763]|      [0763]|
# |04615022300025|   1488175|      [5944]|      [5944]|      [5944]|
# |05008585134156|   4500394|      [5814]|      [5814]|      [5814]|
# |05008585198961|   4237534|      [8021]|      [8021]|      [8021]|
# |05008591907173|   8820613|      [7230]|      [7230]|      [7230]|
# |05008591912645|   8829006|[5651, 5714]|      [5714]|[5714, 5651]|



resMcc = (resMcc.withColumn("MAX_SIZE_DAM_TRANSACT", 
                            fn.when(fn.size("MCC_DAM") >= fn.size("MCC_TRANSACT"),
                            fn.size("MCC_DAM")).otherwise(fn.size("MCC_TRANSACT"))))
resMcc = resMcc.withColumn("SIZE_CONSO", fn.size("MCC_CONSO"))


# In [159]: resMcc.select(fn.sum((fn.col("MAX_SIZE_DAM_TRANSACT") == fn.col("SIZE_CONSO")).cast("integer"))).show()
# +------------------------------------------------------+
# |sum(CAST((MAX_SIZE_DAM_TRANSACT = SIZE_CONSO) AS INT))|
# +------------------------------------------------------+
# |                                               1420391|
# +------------------------------------------------------+

# resMcc.count()
# Out[367]: 1432206

# In [161]: 1420391 / float(1432206)
# Out[161]: 0.9917504884073939


# APE vs MCC ====

resMcc = resMcc2

# Ref ====

# RefNaf
refNaf = spark.read.csv("ref_mcc_naf.csv", header=True)
refNaf = refNaf.drop(refNaf["NAF2003ID"]).withColumnRenamed("NAF2008ID", "NAF")
refNaf.cache().count()
refNaf = fn.broadcast(refNaf)
# Famille MCC
refMcc = spark.table("default.fer_mcc_libelle")
refMcc = refMcc.select("codemcc", fn.col("famille").alias("famille_mcc"))
refMcc.cache().count()
refMcc = fn.broadcast(refMcc)

# Base SIRENE
insee = spark.table("pdv.inse01")
insee = insee.repartition(200)
# Si une entreprise n'a qu'un seul établissement, 
# l'APE de l'établissement (APET) est égal à l'APE de l'entreprise (APEN)
insee = (insee
        .select(fn.concat_ws("", fn.col("SIREN"), fn.col("NIC")).alias("SRT"),
                fn.col("APET700").alias("NAF"))
        .dropDuplicates())
insee.cache().count()

# Merge - Insee
insee = insee.join(refNaf, insee["NAF"] == refNaf["NAF"], "inner").drop(refNaf["NAF"])
insee = insee.join(refMcc, insee["MCCID"] == refMcc["codemcc"], "inner").drop(refMcc["codemcc"])
insee = insee.select("SRT", fn.col("famille_mcc").alias("famille_mcc_insee"))
# Merge - resMcc
resMcc = resMcc.join(refMcc.withColumnRenamed("famille_mcc", "famille_mcc_dam"), resMcc["MCC_DAM"] == refMcc["codemcc"], "left").drop(refMcc["codemcc"])
resMcc = resMcc.join(refMcc.withColumnRenamed("famille_mcc", "famille_mcc_transact"), resMcc["MCC_TRANSACT"] == refMcc["codemcc"], "left").drop(refMcc["codemcc"])
# Filter
resMccDam = resMcc.select("SRT", "famille_mcc_dam").dropDuplicates()
selSRT = (resMccDam
          .groupBy("SRT")
          .agg(fn.countDistinct("famille_mcc_dam").alias("count"))
          .filter(fn.col("count") == 1)
          .select("SRT"))
resMccDam = resMccDam.join(selSRT, selSRT.columns, "inner")

resMccTransact = resMcc.select("SRT", "famille_mcc_transact").dropDuplicates()
selSRT = (resMccTransact
          .groupBy("SRT")
          .agg(fn.countDistinct("famille_mcc_transact").alias("count"))
          .filter(fn.col("count") == 1)
          .select("SRT"))
resMccTransact = resMccTransact.join(selSRT, selSRT.columns, "inner")

# Get Res
resMccDam = resMccDam.join(insee, resMccDam["SRT"] == insee["SRT"], "inner").drop(insee["SRT"])
resMccTransact = resMccTransact.join(insee, resMccTransact["SRT"] == insee["SRT"], "inner").drop(insee["SRT"])

resMccDam.filter(fn.col("famille_mcc_dam") != fn.col("famille_mcc_insee")).count()
# 70179 / 757223
resMccTransact.filter(fn.col("famille_mcc_transact") != fn.col("famille_mcc_insee")).count()
# 68500 / 773216
