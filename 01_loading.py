import pandas as pd
from pyspark.sql import DataFrame
import pyspark.sql.functions as fn
from pyspark.sql.types import BooleanType, StringType, ArrayType

# Utils
def unionAll(*dfs):
    return reduce(DataFrame.unionAll, dfs)

# Ref
ert = spark.table("default.fer_ref_famille_ert")
ert.cache().count()
ert = fn.broadcast(ert)

# DAM ====
df = spark.table('pdv.dam01')

# Merge
dfMerged = df.join(ert, df["ERT"] == ert["ert"], "left").drop(ert["ert"])

# Intuition pour Contrat vs AAMM
dfSummary = (dfMerged
             .filter(fn.col("res_id") == 1)
             .groupBy("SRT", "famille_ert", "ID_PDV_BQE", "REF_ACQ")
             .agg(fn.countDistinct("MCC").alias("nunique_mcc"),
                  fn.countDistinct("RSN").alias("nunique_rsn")))

# Extract for R
dfSummary.toPandas().to_csv("./dfSummary_SRT_IDPDV_ERT.csv", index=False)

# START CONSO -- ID_PDV_BQE
dfTmp = dfMerged.select("SRT", "famille_ert", "MCC", "RSN", "ID_PDV_BQE")

id_pdv_ok = dfTmp.filter(fn.length(fn.col("ID_PDV_BQE")) == 7)
id_pdv_toSplit = (dfTmp
                  .filter(fn.length(fn.col("ID_PDV_BQE")) > 7)
                  .filter(fn.regexp_extract(fn.col("ID_PDV_BQE"), "\s", 0) != ""))
id_pdv_toUdf = (dfTmp
                .filter(fn.length(fn.col("ID_PDV_BQE")) > 7)
                .filter(fn.regexp_extract(fn.col("ID_PDV_BQE"), "\s", 0) == ""))
# UDF
def split_contrat(id):
    indices = range(0, len(id), 8)
    return(" ".join([id[(i+1):j] for i, j in zip(indices, indices[1:] + [None])]))

udf_contrat = fn.udf(lambda id: split_contrat(id), StringType())
id_pdv_toUdf = id_pdv_toUdf.withColumn("ID_PDV_BQE", udf_contrat(fn.col("ID_PDV_BQE")).alias("ID_PDV_BQE"))


id_pdv_toSplit = unionAll(*[id_pdv_toSplit, id_pdv_toUdf.filter(fn.length(fn.col("ID_PDV_BQE")) != 7)])
id_pdv_toSplit = id_pdv_toSplit.withColumn("ID_PDV_BQE", fn.split(fn.col("ID_PDV_BQE"), " "))
id_pdv_toSplit = (id_pdv_toSplit
                  .select("SRT", "famille_ert", "MCC", "RSN", 
                          fn.explode(fn.col("ID_PDV_BQE")).alias("ID_PDV_BQE")))

id_pdv = (unionAll(*[id_pdv_ok, 
                     id_pdv_toUdf.filter(fn.length(fn.col("ID_PDV_BQE")) == 7),
                     id_pdv_toSplit]))
id_pdv = id_pdv.dropDuplicates()

# Export
id_pdv.write.parquet("./id_pdv_dam")

# Transactions ====
df = spark.table('x_compensation.transactions')

df = (df
      .filter(fn.col("b21_code_pays_du_systeme_dacceptation") == 250)
      .filter(fn.col("s04_code_operation") == 100)
      .select(fn.col("b14_siret").alias("SRT"),
              fn.col("b08_environnement_reglementaire__technique_de_la_transaction").alias("ERT"),
              fn.col("b15_code_activite_de_laccepteur___code_mcc").alias("MCC"),
              fn.trim(fn.col("b17_libelle_enseigne_commerciale")).alias("RSN"),
              fn.trim(fn.col("b16_numero_de_contrat_accepteur")).alias("ID_PDV_BQE")))
# fn.col("s06_identifiant_etablissement_donneur_dordre").alias("REF_ACQ")))

# Merged
dfMerged = df.join(ert, df["ERT"] == ert["ert"], "left").drop(ert["ert"])

# Cleaning
# Contrat must have length == 7 
dfMerged = (dfMerged
            .withColumn("ID_PDV_BQE",
                        fn.when(fn.length(fn.col("ID_PDV_BQE")) == 10,
                        fn.substring(fn.col("ID_PDV_BQE"), 3, 7))
                        .otherwise(fn.col("ID_PDV_BQE"))))
# MCC must have length == 4
dfMerged = (dfMerged.withColumn("MCC",
                                fn.when(fn.length(fn.col("MCC")) == 3, 
                                fn.concat_ws("", fn.lit("0"), fn.col("MCC")))
                                .otherwise(fn.col("MCC"))))
dfTransact = dfMerged.dropDuplicates()

# Extract for R
dfSummary = (dfMerged
             .groupBy("SRT", "famille_ert", "ID_PDV_BQE")
             .agg(fn.countDistinct("MCC").alias("nunique_mcc"),
                  fn.countDistinct("RSN").alias("nunique_rsn")))

# Export extract for R
dfSummary.toPandas().to_csv("./dfSummaryTransact_SRT_ERT_CONTRAT.csv", index=False)


# Export
dfTransact.write.parquet("./id_pdv_transact")


# Breakdown MCC
# Transact
df = spark.table('x_compensation.transactions')

df = (df
      .filter(fn.col("b21_code_pays_du_systeme_dacceptation") == 250)
      .filter(fn.col("s04_code_operation") == 100)
      .select(fn.col("b14_siret").alias("SRT"),
              fn.col("b08_environnement_reglementaire__technique_de_la_transaction").alias("ERT"),
              fn.col("b15_code_activite_de_laccepteur___code_mcc").alias("MCC"),
              fn.col("b05_date_locale_transaction").alias("Date"),
              fn.substring("b05_date_locale_transaction", 1, 2).alias("Year"),
              fn.col("b09_montant_brut_de_la_transaction").alias("Montant"),
              fn.trim(fn.col("b16_numero_de_contrat_accepteur")).alias("ID_PDV_BQE")))
# fn.col("s06_identifiant_etablissement_donneur_dordre").alias("REF_ACQ")))
dfRes = (df
         .filter(fn.col("Year") == "17")
         .groupBy("MCC")
         .agg(fn.count("*").alias("count"), 
              fn.sum(fn.col("Montant")).alias("sumMontant")))

# DAM
df = spark.table('pdv.dam01')
dfRes = (dfMerged
         .filter((fn.col("res_id") == 1) & 
                 (fn.substring(fn.col("AAMM_ACTIVITE"), 1, 2) == "17"))
         .groupBy("MCC")
         .agg(fn.sum(fn.col("NB_FACTURE")).alias("Count"), 
              fn.sum(fn.col("MT_BRUT_FACT")).alias("Montant")))