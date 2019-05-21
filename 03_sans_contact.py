import pandas as pd
from pyspark.sql import DataFrame
import pyspark.sql.functions as fn
from pyspark.sql.types import BooleanType, StringType, ArrayType

# Year 2017 ====

# Ref ====
ert = spark.table("default.fer_ref_famille_ert")
ert.cache().count()
ert = fn.broadcast(ert)
# ContactId
contactid = spark.table("default.fer_type_contact_corrige")
contactid.cache().count()
contactid = fn.broadcast(contactid)
# Famille MCC
refMcc = spark.table("default.fer_mcc_libelle")
refMcc = refMcc.select("codemcc", fn.col("famille").alias("famille_mcc"))
refMcc.cache().count()
refMcc = fn.broadcast(refMcc)


# DAM ====
dfDam = spark.table('pdv.dam01')
dfDam = dfDam.filter((fn.col("res_id") == 1) & (fn.substring(fn.col("AAMM_ACTIVITE"), 1, 2) == "17"))
dfDam = dfDam.join(ert, dfDam["ERT"] == ert["ert"], "left").drop(ert["ert"])
dfDam = dfDam.join(refMcc, dfDam["MCC"] == refMcc["codemcc"], "left").drop(refMcc["codemcc"])

# Flag Contact - DAM
cond = fn.col("CONTACTID") == "1"
dfDam = dfDam.withColumn("Flag_Contact", fn.when(cond, fn.lit(1)).otherwise(0))


# Transactions ====
dfTransact = spark.table('x_compensation.transactions')
cond = (~fn.col("b58_application_interchange_profile").isin(["0040", "2040", "1B80", "1B00"])) & \
       (fn.col("b28_mode_de_lecture_de_la_carte") == "3")

dfTransact = (dfTransact
              .filter(fn.col("b21_code_pays_du_systeme_dacceptation") == 250)
              .filter(fn.col("s04_code_operation") == 100)
              .filter(fn.substring("b05_date_locale_transaction", 1, 2) == "17")
              .withColumn("MCC",
                                fn.when(fn.length(fn.col("b15_code_activite_de_laccepteur___code_mcc")) == 3, 
                                fn.concat_ws("", fn.lit("0"), fn.col("b15_code_activite_de_laccepteur___code_mcc")))
                                .otherwise(fn.col("b15_code_activite_de_laccepteur___code_mcc")))
              .withColumn("SRT", fn.col("b14_siret"))
              .withColumn("ERT", fn.col("b08_environnement_reglementaire__technique_de_la_transaction"))
              # Flag Contact - Transactions
              .withColumn("Flag_Contact", 
              (~fn.col("b58_application_interchange_profile").isin(["0040", "2040", "1B80", "1B00"]) & 
              (fn.col("b28_mode_de_lecture_de_la_carte") == "3")))
              .withColumn("Flag_Contact", fn.when(cond, fn.lit(1)).otherwise(fn.lit(0))))

# Merged
dfTransact = dfTransact.join(ert, dfTransact["ERT"] == ert["ert"], "left").drop(ert["ert"])
dfTransact = dfTransact.join(refMcc, dfTransact["MCC"] == refMcc["codemcc"], "left").drop(refMcc["codemcc"])

# Check with table itself
sanscontact_transact = dfTransact.groupBy("famille_ert", "Flag_Contact").count().withColumn("Type", fn.lit("Transact")).toPandas()
sanscontact_dam = dfDam.groupBy("famille_ert", "Flag_Contact").agg(fn.sum(fn.col("NB_FACTURE")).alias("count")).withColumn("Type", fn.lit("DAM")).toPandas()

# Export
pd.concat([sanscontact_dam, sanscontact_transact], axis=0).to_csv("sanscontact_analysis_2017.csv", index=False)

# MCC
sanscontact_dam_mcc = (dfDam
                       .groupBy("famille_ert", "Flag_Contact", "famille_mcc")
                       .agg(fn.sum(fn.col("NB_FACTURE")).alias("count"))
                       .withColumn("Type", fn.lit("DAM"))
                       .toPandas())
sanscontact_transact_mcc = (dfTransact
                            .groupBy("famille_ert", "Flag_Contact", "famille_mcc")
                            .count()
                            .withColumn("Type", fn.lit("Transact"))
                            .toPandas())
# Export
pd.concat([sanscontact_dam_mcc, sanscontact_transact_mcc], axis=0).to_csv("sanscontact_analysis_mcc_2017.csv", index=False, encoding="utf8")
