import pandas as pd
from pyspark.sql import DataFrame
import pyspark.sql.functions as fn
from pyspark.sql.types import BooleanType, StringType, ArrayType


# Ref
ert = spark.table("default.fer_ref_famille_ert")
ert.cache().count()
ert = fn.broadcast(ert)

# DAM ====
dfDam = spark.table('pdv.dam01')
dfDam = dfDam.filter(fn.col("res_id") == 1)
dfDam = dfDam.join(ert, dfDam["ERT"] == ert["ert"], "left").drop(ert["ert"])

# Key: SRT + ERT + Code banque
dfDamKeys = (dfDam
             .select(fn.col("AAMM_ACTIVITE").alias("YearMon"), 
                     "SRT", "REF_ACQ", 
                     fn.col("famille_ert").alias("Famille"))
             .dropDuplicates())
dfDamKeys.cache().count()

# Transactions ====
dfTransact = spark.table('x_compensation.transactions')
dfTransact = (dfTransact
              .filter(fn.col("b21_code_pays_du_systeme_dacceptation") == 250)
              .filter(fn.col("s04_code_operation") == 100)
              .withColumn("SRT", fn.col("b14_siret"))
              .withColumn("ERT", fn.col("b08_environnement_reglementaire__technique_de_la_transaction"))
              .withColumn("REF_ACQ", fn.trim(fn.col("s06_identifiant_etablissement_donneur_dordre")))
              .withColumn("YearMon", fn.substring("b05_date_locale_transaction", 1, 4)))

# Merged
dfTransact = dfTransact.join(ert, dfTransact["ERT"] == ert["ert"], "left").drop(ert["ert"])

dfTransactKeys = dfTransact.select("YearMon", "SRT", "REF_ACQ", fn.col("famille_ert").alias("Famille")).dropDuplicates()
dfTransactKeys.cache().count()

# Autorisations ====
dfAutoVad = spark.table("x_autorisation.autovad01")
dfAutoVadKeys = (dfAutoVad
                .filter(fn.col("c19_code_pays_de_lacquereur") == "FR")
                .filter(fn.col("e_siret_siret_de_laccepteur").isNotNull())
                .filter(fn.col("e_bqe_acq_banque_acquereur_si_contexte_cb") != "     ")
                .select(
                    fn.regexp_replace(
                        fn.substring("e_dt_dem_auto_date_gmt_de_demande_dautorisation", 3, 5), 
                        "-", "").alias("YearMon"),
                    fn.col("e_siret_siret_de_laccepteur").alias("SRT"),
                    fn.col("e_bqe_acq_banque_acquereur_si_contexte_cb").alias("REF_ACQ"),
                    fn.lit("VAD").alias("Famille"))
                .dropDuplicates())

dfAutoVadKeys.cache().count()
dfAutoVadKeys.join(dfTransactKeys, on=dfAutoVadKeys.columns, how="outer")

# Indicateurs
def indicateurs(dfTarget, dfCompare, name):
    left = dfTarget.join(dfCompare.withColumn("Flag", fn.lit(1)), on=dfTarget.columns, how="left")
    left = left.withColumn("Flag", fn.when(fn.col("Flag").isNull(), fn.lit(0)).otherwise(fn.col("Flag")))
    resLeft = left.groupBy("YearMon").agg(fn.sum(fn.col("Flag")).alias("sum"), fn.count("*").alias("count"))
    resLeft = resLeft.withColumn("pct", fn.col("sum") / fn.col("count"))
    dfPandas = resLeft.toPandas()
    dfPandas["Type"] = name
    return dfPandas

# Export res
res = [indicateurs(dfAutoVadKeys, dfTransactKeys, "Auto_Transact"),
       indicateurs(dfAutoVadKeys, dfDamKeys, "Auto_DAM"),
       indicateurs(dfTransactKeys, dfDamKeys, "Transact_DAM"),
       indicateurs(dfTransactKeys, dfAutoVadKeys, "Transact_Auto"),
       indicateurs(dfDamKeys, dfTransactKeys, "DAM_Transact"),
       indicateurs(dfDamKeys, dfAutoVadKeys, "DAM_Auto")]
res = pd.concat(res, axis=0)
res.to_csv("./test_jointures.csv", index=False)
