# Databricks notebook source
import traceback
import json
import sys
import logging
import yaml
from datetime import datetime,date,timedelta
import os
from pyspark.sql.functions import *
from pyspark.sql.functions import split,lpad,concat_ws,desc
from pyspark.sql.types import *
from pyspark.sql.types import StringType,BooleanType,DateType
import calendar
import dateutil.relativedelta
from pyspark.sql.window import Window
import pandas as pd
import numpy as np
from pyspark.sql.functions import regexp_replace
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from dateutil.relativedelta import relativedelta
spark = SparkSession.builder.appName('pyspark - example join').getOrCreate()
sc = spark.sparkContext

# Custom Library
sys.path.insert(0,'/dbfs/dataintegration/process-files-common/')
import utilsShared
from metadataManager import metadataManager
# Logger function
logger = utilsShared.getFormattedLogger(__name__)
# Read the env Config Properties
envConfig = utilsShared.getEnvConf(logger,dbutils)
spark.conf.set('spark.sql.caseSensitive', True)

'''
This notebook will generate extract for number of months prior to batchDate based on Accounting Period.
For example, if wanted to generate quarterly data - noOfMonths = 3 and batchDate = 20230101, then extract will contain data from 202210 to 202212.
If wanted to generate yearly data - noOfMonths = 12 and batchDate = 20230101, then extract will contain data from 202201 to 202212
'''

#Creation Of widgets to accept Parameters in the notebook
dbutils.widgets.text("adv_schema", "","")
dbutils.widgets.text("loaddate", "","")
dbutils.widgets.text("noOfMonths", "","")
dbutils.widgets.dropdown("source","All",["All","Majesco","Gain","Prwins","Arrowhead","DYESL","XM","ClaimCenter","Juris","DYESL Claim"])
dbutils.widgets.text("batchDate", "","")
dbutils.widgets.text("batchEndDate", "","")
dbutils.widgets.text("batchId", "","")
dbutils.widgets.text("parameterObject", "","")

#Getting the values from widgets and storing it in variables
adv_schema_name  = dbutils.widgets.get("adv_schema").strip().upper() #optional
loadDate = dbutils.widgets.get("loaddate").strip()
noOfMonths = dbutils.widgets.get("noOfMonths").strip()
source = dbutils.widgets.get("source")
batchDate = dbutils.widgets.get("batchDate").strip() 
batchEndDate = dbutils.widgets.get("batchEndDate").strip() #optional
batchId = dbutils.widgets.get("batchId").strip()
parameterObject = dbutils.widgets.get("parameterObject").strip()

if parameterObject != "":
  try:
    paramstr = parameterObject
    paramObject = json.loads(paramstr)
    if 'logging-level' in paramObject:
      if paramObject['logging-level'] == 'DEBUG':
        debugMode = 'TRUE'
      else:
        debugMode = 'FALSE'
    else:
      debugMode = 'FALSE'
  except:
      raise ValueError("ERROR in  Processing parameterJSON " + paramstr)
else:
  debugMode = 'FALSE'

if debugMode == 'TRUE':
  logger.setLevel(logging.DEBUG)
else:
  logger.setLevel(logging.INFO)

# Check batchDate format
if batchDate == "" :
  batchDate = str(datetime.now().strftime("%Y%m%d"))
  logger.info("Since batch date is not provided, considering load datetime as current/Execution timestamp")
elif len(batchDate) != 8:
  logger.info("batchDate is incorrect. Please pass the date in YYYYMMDD format")
  sys.exit()

# if batchEndDate is passed, check format
if batchEndDate != "":
    if len(batchEndDate) != 8:
      logger.info("batchEndDate format is incorrect. Please pass the date in YYYYMMDD format")
      sys.exit()

# getting schema name
if adv_schema_name == '':
  adv_schema_name = "ADV_DB"
logger.info("Fetching table from schema: " + adv_schema_name)

# Check noOfMonths format
try:
  noOfMonths = int(noOfMonths)
except:
  raise ValueError("ERROR in converting noOfMonths to integer")
  

#Reading tables from ADV_DB
atmoicTableList = ["SAT_UNIDNTFD_PRTY","XFRMTN","REF_LKUP"]
if len(atmoicTableList) > 0:
  for atomicTableName in atmoicTableList:
    delta_path = "/mnt/mountdeltalake/" + adv_schema_name + "/" + atomicTableName.lower()
    Atomic_DF = spark.read \
    .format("delta") \
    .load(delta_path)
    if "LD_END_DT" in Atomic_DF.columns:
      Atomic_DF = Atomic_DF.where(col("LD_END_DT").isNull())
    Atomic_DF.registerTempTable(str(atomicTableName))
    logger.info("Table is being added to sql : " + atomicTableName)

# COMMAND ----------

# DBTITLE 1,Calculation for Accounting Start & End Date
#Generating multiple batch date formats and adding it to parameter object
# batchDate = "20240301"
# noOfMonths = 4
logger.info("Proccessing Batch Date: " + batchDate + " with noOfMonths: " + str(noOfMonths))
batchDate_dt = datetime.strptime(batchDate, "%Y%m%d")
rangeEndDate_dt = batchDate_dt.replace(day=1) - timedelta(days=1)
rangeStartDate_dt = (rangeEndDate_dt - relativedelta(months=noOfMonths-1)).replace(day=1)
rangeEndDate = rangeEndDate_dt.strftime("%Y-%m-%d")
rangeStartDate = rangeStartDate_dt.strftime("%Y-%m-%d")

logger.info("Extract Range Start Date: " + rangeStartDate + "; Extract Range End Date: " + rangeEndDate)

# COMMAND ----------

#source filtered policy
atmoicTableListPolicy= ["LNK_PREM_TRAN","SAT_PREM","LNK_PLCY_CVRG","SAT_PLCY_DTL","SAT_CMSN","SAT_PLCY_CVRG_DTL","HUB_PLCY","LNK_PLCY_PRTY_ROLE","SAT_ORG","SAT_SELL_AGCY","SAT_TRTY_DTL","LNK_PLCY_CNTCT_PNT","SAT_PHYS_ADDR","SAT_PLCY_LMT","SAT_PLCY_CVRG_DED","SAT_INS_OBJ_PROP","LNK_PLCY_LOB","SAT_PLCY_LOB_DTL","SAT_SCHED_MDFR","SAT_PLCY_CVRG_LMT","LNK_PLCY_LOB","SAT_PLCY_LOB_DTL"]
if len(atmoicTableListPolicy) > 0:
  for atomicTableName in atmoicTableListPolicy:
    delta_path = "/mnt/mountdeltalake/" + adv_schema_name + "/" + atomicTableName.lower()
    Atomic_DF = spark.read \
    .format("delta") \
    .load(delta_path)
    #.where(col("REC_SRC_NM")=="109")
    if "LD_END_DT" in Atomic_DF.columns:
      Atomic_DF = Atomic_DF.where(col("LD_END_DT").isNull())
    Atomic_DF.registerTempTable(str(atomicTableName))
    logger.info("Table is being added to sql for Policy: " + atomicTableName)

# COMMAND ----------

#source filtered claim
atmoicTableListClaim = ["SAT_CLM_DTL","SAT_CVRG_DTL","LNK_CLM_PLCY", "LNK_CLM_TRAN","SAT_CLM_TRAN_DTL","SAT_EXPSR_DTL","SAT_LOB_DTL","SAT_PLCY_CVRG_SAR_DTL","SAT_AGCY_ACCT_DTL","LNK_AGCY_ACCT_LGL_AGCY","SAT_LGL_AGCY_DTL","SAT_AGCY_ACCT_LGL_AGCY_DTL","SAT_PLCY_LOB_EXPSR"]
if len(atmoicTableListClaim) > 0:
  for atomicTableName in atmoicTableListClaim:
    delta_path = "/mnt/mountdeltalake/" + adv_schema_name + "/" + atomicTableName.lower()
    Atomic_DF = spark.read \
    .format("delta") \
    .load(delta_path)
    #.where(col("REC_SRC_NM")=="105")
    if "LD_END_DT" in Atomic_DF.columns:
      Atomic_DF = Atomic_DF.where(col("LD_END_DT").isNull())
    Atomic_DF.registerTempTable(str(atomicTableName))
    logger.info("Table is being added to sql for claim: " + atomicTableName)

# COMMAND ----------

# Define pivot function
def pivotDF(inputDF,pkCol,pivotCol,aggCol,pivotPrefix):
  pivotList = sorted(list(inputDF.select(pivotCol).distinct().toPandas()[pivotCol]))
  pivotDF = inputDF.groupBy(pkCol).pivot(pivotCol,pivotList).agg(first(aggCol,True))
  pivotDF = pivotDF.select([pkCol]+[col("`" + c + "`").alias(pivotPrefix + " - " + c) for c in pivotDF.columns if c != pkCol])
  return pivotDF

# COMMAND ----------

try:  
  logger.info("Start of reading atomic tables...")
  #read table LNK_PREM_TRAN
  LPT = spark.table("LNK_PREM_TRAN") #.fillna('')

  # read table SAT_PREM
  SP = spark.table("SAT_PREM") \
          .withColumn("ACCTG_YR",expr("int(left(ACCTG_YR_MO_TXT,4))")) \
          .withColumn("ACCTG_MO",expr("lpad(int(replace(replace(RIGHT(left(ACCTG_YR_MO_TXT,7), length(left(ACCTG_YR_MO_TXT,7)) - 4),'-',''),'_','')),2,'0')")) \
          .withColumn("ACCTG_YR_MO_DATE",expr("cast(ACCTG_YR_MO_TXT as date)")) \
          .withColumn("ACCTG_YR_MO_FORMAT",expr("concat(ACCTG_YR,'-',ACCTG_MO,'-01')")) \
          .withColumn("ACCTG_YR_MO_FINAL",expr("CASE WHEN length(ACCTG_YR_MO_TXT) >= 10 THEN ACCTG_YR_MO_DATE ELSE ACCTG_YR_MO_FORMAT END"))
#           .withColumn("ACCTG_YR_MO_FINAL_ADD1MO",expr(""))
  
  # read table SAT_CMSN
  SC = spark.table("SAT_CMSN")

  # reading table SAT_PLCY_DTL
  SPD = spark.table("SAT_PLCY_DTL")

  #read table LNK_PLCY_CVRG
  LPC = spark.table("LNK_PLCY_CVRG")#.fillna('')


  #read table SAT_PLCY_CVRG_DTL
  SPCD = spark.table("SAT_PLCY_CVRG_DTL")
  
  #read table SAT_PLCY_CVRG_LMT
  SPCL = spark.table("SAT_PLCY_CVRG_LMT")
    

  #read table HUB_PLCY
  HPY = spark.table("HUB_PLCY")
  
  # read table LNK_PLCY_PRTY_ROLE
  LPPR = spark.table("LNK_PLCY_PRTY_ROLE")
  
  # read table SAT_ORG
  SO = spark.table("SAT_ORG")
  
  # read table SAT_CLM_DTL
  SCD = spark.table("SAT_CLM_DTL")
  
  # read table SAT_CVRG_DTL
  SCVD = spark.table("SAT_CVRG_DTL")
  
  # read table LNK_CLM_PLCY
  LCP = spark.table("LNK_CLM_PLCY")
  
  # read table LNK_CLM_TRAN
  LCT = spark.table("LNK_CLM_TRAN")
  
  # read table SAT_CLM_TRAN_DTL
  SCTD = spark.table("SAT_CLM_TRAN_DTL")
  
  # read table SAT_EXPSR_DTL
  SED = spark.table("SAT_EXPSR_DTL")
  
  # read table SAT_LOB_DTL
  SLD = spark.table("SAT_LOB_DTL")
  
  # read table SAT_PLCY_CVRG_SAR_DTL
  SPCSD = spark.table("SAT_PLCY_CVRG_SAR_DTL")
  
  # read table SAT_AGCY_ACCT_DTL
  SAAD_AAS = spark.table("SAT_AGCY_ACCT_DTL").where((col("REC_SRC_NM")=='0036')).withColumn("ACCT_NBR1", trim(col("ACCT_NBR"))).withColumn("PROD_NBR_JOIN",col("ACCT_NBR1").cast(IntegerType())).withColumn("rwnm", eval("row_number().over(Window.partitionBy('PROD_NBR_JOIN').orderBy(desc('BTCH_DT'),asc('PROD_NBR_JOIN')))")).where("rwnm == '1'")
  
  # read table LNK_AGCY_ACCT_LGL_AGCY
  LAALA_AAS = spark.table("LNK_AGCY_ACCT_LGL_AGCY").where((col("REC_SRC_NM")=='0036'))
  
  # read table SAT_LGL_AGCY_DTL
  SLAD_AAS = spark.table("SAT_LGL_AGCY_DTL").where((col("REC_SRC_NM")=='0036'))
  
  # read table SAT_AGCY_ACCT_LGL_AGCY_DTL
  SAALAD_AAS = spark.table("SAT_AGCY_ACCT_LGL_AGCY_DTL").where((col("REC_SRC_NM")=='0036'))
  
  # read table SAT_SELL_AGCY
  SSA = spark.table("SAT_SELL_AGCY")
  
  # read table SAT_TRTY_DTL
  STD = spark.table("SAT_TRTY_DTL")
  
  # read table LNK_PLCY_CNTCT_PNT
  LPCP_SPA = spark.table("LNK_PLCY_CNTCT_PNT") \
                .where("CNTCT_PNT_TP_CD = 'Primary_Address'") \
                .withColumn("rn_lpcp",eval("row_number().over(Window.partitionBy('QBE_HASH_PLCY_ID').orderBy(desc('BTCH_DT'),asc('QBE_HASH_CNTCT_PNT_ID')))")) \
                .where("rn_lpcp == 1") \
                .join(spark.table("SAT_PHYS_ADDR"),["QBE_HASH_CNTCT_PNT_ID"], "left")
  
  # read table SAT_PHYS_ADDR
  SPA = spark.table("SAT_PHYS_ADDR")
  
  # read table SAT_PLCY_LMT
  SPL = spark.table("SAT_PLCY_LMT").where("REC_SRC_NM = '109'")

  SPL_pivot = pivotDF(SPL,"QBE_HASH_PLCY_ID","LMT_TP_CD","LMT_AMT","Policy Limit Amount")
  
  # read table SAT_PLCY_CVRG_DED
  SPCDED_FILTER = spark.table("SAT_PLCY_CVRG_DED").where("REC_SRC_NM = '109'")
  SPCDED = SPCDED_FILTER.join(LPC,["QBE_HASH_PLCY_CVRG_ID"], "inner").where("QBE_HASH_PLCY_ID is not null").select(SPCDED_FILTER["*"])
  SPCDED_pivot = pivotDF(SPCDED,"QBE_HASH_PLCY_CVRG_ID","DED_TP_CD","DED_AMT","Coverage Deductible Amount")
  
  # read table SAT_INS_OBJ_PROP
  SIOP = spark.table("SAT_INS_OBJ_PROP")
  
  #New Tables added 
  
  # read table SAT_PLCY_CVRG_LMT
  SPCL_FILTER = spark.table("SAT_PLCY_CVRG_LMT").filter(SPCL.REC_SRC_NM == '109')
  SPCL = SPCL_FILTER.join(LPC,["QBE_HASH_PLCY_CVRG_ID"], "inner").where("QBE_HASH_PLCY_ID is not null").select(SPCL_FILTER["*"])
  SPCL_Pivot = pivotDF(SPCL,"QBE_HASH_PLCY_CVRG_ID","LMT_TP_CD","LMT_AMT","Coverage Limit Amount")

  
  LPL = spark.table("LNK_PLCY_LOB")
  SPLD = spark.table("SAT_PLCY_LOB_DTL")
  #.where((col("REC_SRC_NM")=='109'))
  SPLE = spark.table("SAT_PLCY_LOB_EXPSR")
  #.where((col("REC_SRC_NM")=='109'))
 
  # read table SAT_SCHED_MDFR
  SSM = spark.table("SAT_SCHED_MDFR").where("REC_SRC_NM = '109'")
  #.where("REC_SRC_NM == '109'")
  SSM = SSM.na.fill('NA',["SCHED_MDFR_TP_CD"]) 
  SSM_Pivot1 = pivotDF(SSM,"QBE_HASH_PREM_TRAN_ID","SCHED_MDFR_TP_CD","SCHED_MDFR_RT_PCT","Coverage Level Schedule Modifier Rate Percent")
  SSM_Pivot = LPT.join(SSM_Pivot1,SSM_Pivot1.QBE_HASH_PREM_TRAN_ID == LPT.QBE_HASH_PREM_TRAN_ID,"left") \
              .select([LPT.REF_ID] +                      
                      [col for col in SSM_Pivot1.columns if col.startswith("Coverage Level Schedule Modifier Rate Percent")])
  
  #read XFRMTN - 
  data_value_json = spark.table("ADV_DB.XFRMTN").where("DATA_KEY_ID='DM_GAIN_UW_COMP'").select("DATA_VAL").collect()
  ref_lookup_val_list = [str(row["DATA_VAL"]).strip() for row in data_value_json]
  LKUP_DF_UW = spark.read.option("multiline", "true").json(spark.sparkContext.parallelize(ref_lookup_val_list))
  
  data_value_json = spark.table("ADV_DB.XFRMTN").where("DATA_KEY_ID='DIM_STATE_CODE_WINS'").select("DATA_VAL").collect()
  ref_lookup_val_list = [str(row["DATA_VAL"]).strip() for row in data_value_json]
  LKUP_DF_ST_WINS = spark.read.option("multiline", "true").json(spark.sparkContext.parallelize(ref_lookup_val_list))
  
  data_value_json = spark.table("ADV_DB.XFRMTN").where("DATA_KEY_ID='ISO_STATE_CODE'").select("DATA_VAL").collect()
  ref_lookup_val_list = [str(row["DATA_VAL"]).strip() for row in data_value_json]
  LKUP_DF_ST = spark.read.option("multiline", "true").json(spark.sparkContext.parallelize(ref_lookup_val_list))
  
  REF_LKUP = spark.table("REF_LKUP").where("CD_LST = 'IT Application'")
  
   
#try
except:
  # Logging failure to audit table
  logger.error(str(traceback.print_exc()))
  auditDict = {'tableName':'extract_csv_generation_process',
               'batchDate':batchDate}
  obj = auditDict
  obj.update(envConfig)
  metadataObject = metadataManager(obj)
  audit_doc = {}
  audit_doc['STATUS'] = "Failure"
  error_msg = "ERROR in reading atomic tables..."
  if "prcs-name" not in obj:
      obj["prcs-name"] = "extract_csv_generation_process"
  metadataObject.insert_auditRecord_dataflow(obj,audit_doc,error_msg)
  logger.debug("Inserted Audit Entry - Job Failure ")
  traceback.print_exc()
  raise ValueError("ERROR in reading atomic tables...")
#except
finally:
  logger.info("End in reading atomic tables...")
#finally

# COMMAND ----------

# DBTITLE 1,Insured table
insdDF = LPPR.join(SO,["QBE_HASH_PRTY_ID"], "left") \
             .withColumn("PRTY_ROLE_TP_CODE", lower(LPPR.PRTY_ROLE_TP_CD)) \
             .where(col("PRTY_ROLE_TP_CODE") == 'insured') \
             .withColumn("insd", eval("row_number().over(Window.partitionBy(LPPR.QBE_HASH_PLCY_ID).orderBy(desc(LPPR.BTCH_DT),desc(LPPR.QBE_HASH_PLCY_PRTY_ROLE_ID)))")) \
             .where(col("insd") == '1') \
             .select(LPPR.QBE_HASH_PLCY_ID,SO.SIC_CD.alias("SIC_CD_Insured"),SO.NBR_OF_EMP_CNT.alias("NBR_OF_EMP_COUNT"))

# COMMAND ----------

# DBTITLE 1,GAIN
gainDF = HPY.join(SPD, HPY.QBE_HASH_PLCY_ID.eqNullSafe(SPD.QBE_HASH_PLCY_ID), "inner") \
            .join(LPT, HPY.QBE_HASH_PLCY_ID.eqNullSafe(LPT.QBE_HASH_PLCY_ID), "left") \
            .join(LPC, (LPC.QBE_HASH_PLCY_ID.eqNullSafe(LPT.QBE_HASH_PLCY_ID)) & (LPC.QBE_HASH_LOB_ID.eqNullSafe(LPT.QBE_HASH_LOB_ID)) & (LPC.QBE_HASH_INSD_OBJ_ID.eqNullSafe(LPT.QBE_HASH_INSD_OBJ_ID)) & (LPC.QBE_HASH_CVRG_ID.eqNullSafe(LPT.QBE_HASH_CVRG_ID)) & (LPC.QBE_HASH_JOB_CLS_CD_ID.eqNullSafe(LPT.QBE_HASH_JOB_CLS_CD_ID)) & (LPC.ST_PRVNC_CD.eqNullSafe(LPT.ST_PRVNC_CD)) & (LPT.REF_ID.eqNullSafe(LPC.REF_ID)) & (LPT.QBE_HASH_CNTCT_PNT_ID.eqNullSafe(LPC.QBE_HASH_CNTCT_PNT_ID)), "left") \
            .join(SPCD, ["QBE_HASH_PLCY_CVRG_ID"], "left") \
            .join(SP, ["QBE_HASH_PREM_TRAN_ID"], "left") \
            .join(SC, ["QBE_HASH_PREM_TRAN_ID"], "left") \
            .join(LKUP_DF_UW.withColumnRenamed("Company_Code","CO_NBR"), ["CO_NBR"], "left") \
            .join(LKUP_DF_ST_WINS.withColumnRenamed("CDKEY3","ST_PRVNC_CD"), ["ST_PRVNC_CD"], "left") \
               .join(insdDF, ["QBE_HASH_PLCY_ID"], "left") \
            .join(REF_LKUP, HPY.REC_SRC_NM == col("CD_VAL"), "left") \
            .withColumn("PROD_NBR_JOIN",col("PRODR_NBR").cast(IntegerType())) \
            .join(SCVD, ["QBE_HASH_CVRG_ID"], "left") \
               .join(LPCP_SPA, ["QBE_HASH_PLCY_ID"], "left") \
               .join(SPL_pivot, ["QBE_HASH_PLCY_ID"], "left") \
               .join(SPCDED_pivot, ["QBE_HASH_PLCY_CVRG_ID"], "left") \
               .join(SIOP, ["QBE_HASH_INSD_OBJ_ID"], "left") \
            .join(SLD, ["QBE_HASH_LOB_ID"], "left") \
            .join(SPCSD, ["QBE_HASH_PLCY_CVRG_ID"], "left") \
            .join(SAAD_AAS, ["PROD_NBR_JOIN"], "left") \
            .join(LAALA_AAS, ["QBE_HASH_AGCY_ACCT_ID"], "left") \
            .join(SLAD_AAS, ["QBE_HASH_LGL_AGCY_ID"], "left") \
            .join(SAALAD_AAS, ["QBE_HASH_AGCY_ACCT_LGL_AGCY_ID"], "left") \
            .withColumnRenamed("CD_SHRT_DESC", "Source System Name") \
            .where(HPY.REC_SRC_NM == '92') \
            .where("ACCTG_YR_MO_FORMAT >= " + rangeStartDate + " and ACCTG_YR_MO_FORMAT <=" + "rangeEndDate") \
            .where(LPT.TRAN_TP_CD.isin(["Coverage_Level_Premium","Manual_Rated_Coverage_Premium"])) \
            .withColumn("Underwriting Year", year(SPD.EFF_DT)) \
            .withColumn("Commission Amount (Calculated)",(SP.CHGD_PREM_AMT * (SC.CMSN_PCT/100)).cast(FloatType())) \
            .withColumn("Product Code",lit("")) \
            .withColumn("Policy Symbol Status", expr("case when SAT_PLCY_DTL.PLCY_SYM_CD in ('CU','CCU','UCU','VCU','WCU','XCU','YCU','ZCU') THEN 'InList' when (SAT_PLCY_DTL.PLCY_SYM_CD not in ('CU','CCU','UCU','VCU','WCU','XCU','YCU','ZCU') or SAT_PLCY_DTL.PLCY_SYM_CD is not null or SAT_PLCY_DTL.PLCY_SYM_CD <> '' ) THEN 'NotInList' ELSE '' end")) \
            .withColumn("Annual Statement Line of Business1", expr("lpad(SAT_CVRG_DTL.ASLOB_CD,3,'0')")) \
            .withColumn("Annual Statement Line of Business", SCVD.ASLOB_CD) \
.withColumn("Coverage TP Code", SCVD.CVRG_TP_CD) \
            .withColumn("Major Peril New1", expr("trim(SAT_CVRG_DTL.CVRG_CD)")) \
            .withColumn("Coverage Code", SCVD.CVRG_CD) \
            .withColumn("Major Peril Status", expr("case when trim(SAT_CVRG_DTL.CVRG_CD) in ('088','089','090','099','100','101','102','103','110','112','114','115','116','117','118','119','120','122','123','130','133','145','146','147','148','149','151','152','153','163','165','166','170','171','172','173','801','810','845','870','124','164','700','701','710','711','712','713','720','721','99','050','210','211','501','502','503','504','506','507','508','521','532','533','505','509','511','531','540','542','543','544','550','552','560') THEN 'InList' when (trim(SAT_CVRG_DTL.CVRG_CD) not in ('088','089','090','099','100','101','102','103','110','112','114','115','116','117','118','119','120','122','123','130','133','145','146','147','148','149','151','152','153','163','165','166','170','171','172','173','801','810','845','870','124','164','700','701','710','711','712','713','720','721','99','050','210','211','501','502','503','504','506','507','508','521','532','533','505','509','511','531','540','542','543','544','550','552','560') or trim(SAT_CVRG_DTL.CVRG_CD) is not null or trim(SAT_CVRG_DTL.CVRG_CD) <> '' ) THEN 'NotInList' ELSE '' end")) \
            .withColumn("Subline Code New1", expr("lpad(SAT_CVRG_DTL.SUBLINE_CD,3,'0')")) \
            .withColumn("Subline Code", SCVD.SUBLINE_CD) \
            .withColumn("Subline Code Status", expr("case when lpad(SAT_CVRG_DTL.SUBLINE_CD,3,'0') in ('010','011','012','013','014','015','016','017','018','019','091','060','061','210','215','220','225','230','235','240','245','250','255','260','265','270','275','280','285','325','332','334','335','336','342','343','345','346','347','350','360','317','344','365') THEN 'InList' when (lpad(SAT_CVRG_DTL.SUBLINE_CD,3,'0') not in ('010','011','012','013','014','015','016','017','018','019','091','060','061','210','215','220','225','230','235','240','245','250','255','260','265','270','275','280','285','325','332','334','335','336','342','343','345','346','347','350','360','317','344','365') or lpad(SAT_CVRG_DTL.SUBLINE_CD,3,'0') is not null or lpad(SAT_CVRG_DTL.SUBLINE_CD,3,'0') <> '' ) THEN 'NotInList' ELSE '' end")) \
            .withColumn("DSTN_CHNNL_CD_DRV",SLAD_AAS.DSTN_CHNNL_CD) \
            .withColumn("EPLI", expr("CASE WHEN DSTN_CHNNL_CD_DRV > '01' and SAT_LOB_DTL.LOB_CD = 'GL' and SAT_PLCY_CVRG_SAR_DTL.TP_BUR_CD = 'GL' and trim(SAT_CVRG_DTL.CVRG_CD) = '560' THEN 'E' WHEN DSTN_CHNNL_CD_DRV > '01' and SAT_LOB_DTL.LOB_CD = 'CPP' and SAT_PLCY_CVRG_SAR_DTL.TP_BUR_CD = 'GL' and trim(SAT_CVRG_DTL.CVRG_CD) = '560' THEN 'E' WHEN DSTN_CHNNL_CD_DRV > '01' and SAT_LOB_DTL.LOB_CD = 'BOP' and SAT_PLCY_CVRG_SAR_DTL.TP_BUR_CD = 'BC' and trim(SAT_CVRG_DTL.CVRG_CD) = '385' THEN 'E' WHEN DSTN_CHNNL_CD_DRV > '01' and SAT_LOB_DTL.LOB_CD = 'BOP' and SAT_PLCY_CVRG_SAR_DTL.TP_BUR_CD = 'BC' and trim(SAT_CVRG_DTL.CVRG_CD) = '662' THEN 'E' WHEN DSTN_CHNNL_CD_DRV > '01' and SAT_LOB_DTL.LOB_CD = 'ML' and SAT_PLCY_CVRG_SAR_DTL.TP_BUR_CD = 'BC' and trim(SAT_CVRG_DTL.CVRG_CD) = '362' THEN 'E' WHEN trim(SAT_CVRG_DTL.CVRG_CD) = '172' THEN 'E' ELSE '' END")) \
            .withColumn("Reinsurance_Company_Code_Status", expr("case when SAT_PLCY_DTL.REINS_CO_NBR = '' then '' when SAT_PLCY_DTL.REINS_CO_NBR not in ('0060') THEN 'NotInList' ELSE '' end")) \
            .withColumn("Reins Flag", expr("case when Reinsurance_Company_Code_Status in ('NotInList') then 'FALSE' ELSE '' end")) \
            .withColumn("Service_Office_New", SAALAD_AAS.SERV_OFC_CD) \
            .withColumn("Service Office Status", expr("case when Service_Office_New in ('70') THEN 'InList' when (Service_Office_New not in ('70')) THEN 'NotInList' ELSE 'NotInList' end")) \
            .withColumn("MSTR_ACCT_NBR_DRV", SAAD_AAS.MSTR_ACCT_NBR) \
            .withColumn("Agency_State_New1", expr("cast(CAST(left(MSTR_ACCT_NBR_DRV,3) as INT) as string)")) \
            .withColumn("Agency_State_Status", expr("case when Agency_State_New1 in ('57','66') THEN 'InList' when (Agency_State_New1 not in ('57','66')) THEN 'NotInList' ELSE '' end")) \
            .withColumn("Kind Code Status", expr("case when PLCY_KIND_CD in ('M') THEN 'InList' when PLCY_KIND_CD not in ('M') THEN 'NotInList' ELSE '' end")) \
            .withColumn("Program Code",lit("")) \
            .withColumn("Organization Code",lit("")) \
            .withColumn("Sub Program Code",lit("")) \
            .withColumn("Business Type Code",lit("")) \
            .withColumn("Policy Type Code",lit("")) \
            .withColumn("Coverage Part Code",lit("")) \
            .withColumn("Market Segment Code",lit("")) \
            .withColumn("Policy Structure",lit("")) \
            .withColumn("Insurer SIC Code",lit("")) \
            .withColumn("Ownership Type Code",lit("")) \
            .withColumn("Treaty Code",lit("")) \
            .select(['Source System Name', \
                     HPY.REC_SRC_NM.alias('Source System Code'), \
                     LKUP_DF_UW.Company_Name.alias("Writing Company"), \
                     SPD.PLCY_SYM_CD.alias("Policy Symbol"), \
                     SPD.PLCY_NBR.alias("Policy Number"), \
                     SPD.ORGNL_PLCY_NBR.alias("Original Policy Number"), \
                     "Product Code", \
                     SP.TRAN_DT.alias("Transaction Date"), \
                     SP.TRAN_EFF_DT.alias("Transaction Effective Date"), \
                     LPT.TRAN_TP_CD.alias("Transaction Type"), \
                     SP.TRAN_CD.alias("Transaction Code"), \
                     SP.TRAN_DESCR.alias("Transaction Desc"), \
                     SPD.EFF_DT.alias("Policy Effective Date"), \
                     SPD.EXPRN_DT.alias("Policy Expiration Date"), \
                     'Underwriting Year', \
                     SP.ACCTG_YR_MO_TXT.alias("Accounting Year Month"), \
                     LKUP_DF_ST_WINS.CDDESC.alias("Risk State"), \
                     SPCD.QBE_CELL_CD.alias("Cell"), \
                     SPCD.QBE_SGMT_CD.alias("Segment Code"), \
                     SPCD.QBE_FINCL_LN_OF_BUS_CD.alias("Financial Line of Business Code"), \
                     SP.CHGD_PREM_AMT.alias("Changed Premium Amount"), \
                     SP.WRTN_PREM_AMT.alias("Written Premium Amount"), \
                     SC.CMSN_PCT.alias("Commission Percentage"), \
                     SC.CMSN_AMT.alias("Commission Amount"), \
                     "Commission Amount (Calculated)", \
                     SPD.CO_NBR.alias("Company Number"), \
                     "Annual Statement Line of Business", \
                     "Coverage Code", \
                     "Coverage TP Code", \
                     "Subline Code", \
                     SPD.CLM_MADE_FLG.alias("Claim_Made Indicator"), \
                     SPD.REINS_CO_NBR.alias("Reinsurance Code"), \
                     SLD.LOB_CD.alias("Line of Business"), \
                     SPCSD.TP_BUR_CD.alias("Bureau Type"), \
                     col("DSTN_CHNNL_CD_DRV").alias("Distribution Channel"), \
                     SLAD_AAS.DEST_GRP_CD.alias("Group Destination Code"), \
                     col("Service_Office_New").alias("Service Office"), \
                     col("MSTR_ACCT_NBR_DRV").alias("Master Account Number"), \
                     col("PROD_NBR_JOIN").alias("Agency Number"), \
                     SPD.PLCY_KIND_CD.alias("Policy Kind Code"), \
                     "Program Code", \
                     "Organization Code", \
                     "Sub Program Code", \
                     "Business Type Code", \
                     "Policy Type Code", \
                     "Coverage Part Code", \
                     "Market Segment Code", \
                     "Policy Structure", \
                     "Insurer SIC Code", \
                     "Ownership Type Code", \
                     "Treaty Code", \
                     SP.TRAN_EXPRN_DT.alias("Transaction Expiration Date"), \
                     LPCP_SPA.PSTL_CD.alias("Primary Address Zip Code"), \
                     LPCP_SPA.CITY_NM.alias("Primary Address City"), \
                     LPCP_SPA.CNTY_NM.alias("Primary Address County"), \
                     SPCD.CLS_CD.alias("Class Code"), \
                     SPD.RETRO_DT.alias("Retroactive Date"), \
                     SPD.TAIL_DT.alias("Tail Date"), \
                     SPCD.COINS_VAL.alias("Coinsurance Value"), \
                     SP.PLCY_EVNT_TP_CD.alias("Policy Event Type Code"), \
                       SPD.RNWL_CNT.alias("Renewal Count"), \
                       SPD.CANCTN_DESCR.alias("Cancellation Description"), \
                       SPCD.EXPSR_AMT.alias("Exposure Amount"), \
                       SPCD.EXPSR_BASE.alias("Exposure Base"), \
                       insdDF.SIC_CD_Insured.alias("Insured SIC Code"), \
                       SPD.REVSN_NBR.alias("Revision Number"), \
                       SIOP.PRPTY_LOC_NBR.alias("Insured Property Location Number")] \
                       + [col for col in SPL_pivot.columns if col.startswith("Policy Limit Amount")] \
                       + [col for col in SPCDED_pivot.columns if col.startswith("Coverage Deductible Amount")])
#gainDF.display()

# COMMAND ----------

# DBTITLE 1,arrowheadinsr
arrowheadinsrDF = LPPR.join(SO,["QBE_HASH_PRTY_ID"], "left") \
             .where(LPPR.REC_SRC_NM == 'Arrowhead') \
             .where(SO.REC_SRC_NM == 'Arrowhead') \
             .withColumn("PRTY_ROLE_TP_CODE", lower(LPPR.PRTY_ROLE_TP_CD)) \
             .where(col("PRTY_ROLE_TP_CODE") == 'insurer') \
             .withColumn("Insurerarw", eval("first(SO.ORG_NM).over(Window.partitionBy(LPPR.QBE_HASH_PLCY_ID,LPPR.PRTY_ROLE_TP_CD).orderBy(desc(LPPR.BTCH_DT)))")) \
             .select(LPPR.QBE_HASH_PLCY_ID,SO.ORG_CD,'Insurerarw').distinct()

# COMMAND ----------

# DBTITLE 1,ARROWHEAD  
LPT_AH = LPT.fillna("")

arrowheadDF = HPY.join(SPD, ["QBE_HASH_PLCY_ID"], "inner") \
            .join(LPT_AH, ["QBE_HASH_PLCY_ID"], "left") \
            .join(LPC.fillna(""),["QBE_HASH_PLCY_ID","QBE_HASH_CVRG_ID","QBE_HASH_INSD_OBJ_ID","QBE_HASH_LOB_ID","QBE_HASH_JOB_CLS_CD_ID","QBE_HASH_CNTCT_PNT_ID"],"left") \
            .join(SPCD, ["QBE_HASH_PLCY_CVRG_ID"], "left") \
            .join(SP, ["QBE_HASH_PREM_TRAN_ID"], "left") \
            .join(SC, ["QBE_HASH_PREM_TRAN_ID"], "left") \
            .join(arrowheadinsrDF, ["QBE_HASH_PLCY_ID"], "left") \
               .join(insdDF, ["QBE_HASH_PLCY_ID"], "left") \
            .join(LKUP_DF_ST.withColumnRenamed("Code","ST_PRVNC_CD"), ["ST_PRVNC_CD"], "left") \
            .join(SCVD, ["QBE_HASH_CVRG_ID"], "left") \
               .join(LPCP_SPA, ["QBE_HASH_PLCY_ID"], "left") \
               .join(SPL_pivot, ["QBE_HASH_PLCY_ID"], "left") \
               .join(SPCDED_pivot, ["QBE_HASH_PLCY_CVRG_ID"], "left") \
               .join(SIOP, ["QBE_HASH_INSD_OBJ_ID"], "left") \
            .withColumn("Source System Name", lit('Arrowhead')) \
            .where(HPY.REC_SRC_NM == 'Arrowhead') \
            .where(col("TRAN_TP_CD").isin(["Coverage_Level_Premium"])) \
            .withColumn("Underwriting Year", year(SPD.EFF_DT)) \
            .withColumn("Commission Amount (Calculated)",(SP.CHGD_PREM_AMT * (SC.CMSN_PCT/100)).cast(FloatType())) \
            .withColumn("Product Code",lit("")) \
            .withColumn("ASLOB1", expr("lpad(SAT_CVRG_DTL.ASLOB_CD,3,'0')")) \
            .withColumn("ASLOB", SCVD.ASLOB_CD) \
            .withColumn("Company Number",lit("")) \
.withColumn("Coverage TP Code",lit("")) \
            .withColumn("Coverage Code",lit("")) \
            .withColumn("Subline Code",lit("")) \
            .withColumn("Claim_Made Indicator",lit("")) \
            .withColumn("Reinsurance Code", lit("")) \
            .withColumn("Line of Business",lit("")) \
            .withColumn("Bureau Type",lit("")) \
            .withColumn("Distribution Channel",lit("")) \
            .withColumn("Group Destination Code",lit("")) \
            .withColumn("Service Office",lit("")) \
            .withColumn("Master Account Number",lit("")) \
            .withColumn("Agency Number",lit("")) \
            .withColumn("Policy Kind Code",lit("")) \
            .withColumn("Sub Program Code",lit("")) \
            .withColumn("Business Type Code",lit("")) \
            .withColumn("Policy Type Code",lit("")) \
            .withColumn("Coverage Part Code",lit("")) \
            .withColumn("Market Segment Code",lit("")) \
            .withColumn("Policy Structure",lit("")) \
            .withColumn("Insurer SIC Code",lit("")) \
            .withColumn("Ownership Type Code",lit("")) \
            .withColumn("Treaty Code",lit("")) \
			.select(['Source System Name', \
                     HPY.REC_SRC_NM.alias('Source System Code'), \
					 arrowheadinsrDF.Insurerarw.alias("Writing Company"), \
                     SPD.PLCY_SYM_CD.alias("Policy Symbol"), \
                     SPD.PLCY_NBR.alias("Policy Number"), \
                     SPD.ORGNL_PLCY_NBR.alias("Original Policy Number"), \
                     "Product Code", \
                     SP.TRAN_DT.alias("Transaction Date"), \
                     SP.TRAN_EFF_DT.alias("Transaction Effective Date"), \
                     col("TRAN_TP_CD").alias("Transaction Type"), \
                     SP.TRAN_CD.alias("Transaction Code"), \
                     SP.TRAN_DESCR.alias("Transaction Desc"), \
                     SPD.EFF_DT.alias("Policy Effective Date"), \
                     SPD.EXPRN_DT.alias("Policy Expiration Date"), \
                     'Underwriting Year', \
                     SP.ACCTG_YR_MO_TXT.alias("Accounting Year Month"), \
                     LPT_AH.ST_PRVNC_CD.alias("Risk State"), \
                     SPCD.QBE_CELL_CD.alias("Cell"), \
                     SPCD.QBE_SGMT_CD.alias("Segment Code"), \
                     SPCD.QBE_FINCL_LN_OF_BUS_CD.alias("Financial Line of Business Code"), \
                     SP.CHGD_PREM_AMT.alias("Changed Premium Amount"), \
                     SP.WRTN_PREM_AMT.alias("Written Premium Amount"), \
                     SC.CMSN_PCT.alias("Commission Percentage"), \
                     SC.CMSN_AMT.alias("Commission Amount"), \
                     "Commission Amount (Calculated)", \
                     "Company Number", \
                     col("ASLOB").alias("Annual Statement Line of Business"), \
                     "Coverage Code", \
                     "Coverage TP Code", \
                     "Subline Code", \
                     "Claim_Made Indicator", \
                     "Reinsurance Code", \
                     "Line of Business", \
                     "Bureau Type", \
                     "Distribution Channel", \
                     "Group Destination Code", \
                     "Service Office", \
                     "Master Account Number", \
                     "Agency Number", \
                     "Policy Kind Code", \
                     SPD.PGM_CD.alias("Program Code"), \
                     arrowheadinsrDF.ORG_CD.alias("Organization Code"), \
                     "Sub Program Code", \
                     "Business Type Code", \
                     "Policy Type Code", \
                     "Coverage Part Code", \
                     "Market Segment Code", \
                     "Policy Structure", \
                     "Insurer SIC Code", \
                     "Ownership Type Code", \
                     "Treaty Code", \
                       SP.TRAN_EXPRN_DT.alias("Transaction Expiration Date"), \
                       LPCP_SPA.PSTL_CD.alias("Primary Address Zip Code"), \
                       LPCP_SPA.CITY_NM.alias("Primary Address City"), \
                       LPCP_SPA.CNTY_NM.alias("Primary Address County"), \
                       SPCD.CLS_CD.alias("Class Code"), \
                       SPD.RETRO_DT.alias("Retroactive Date"), \
                       SPD.TAIL_DT.alias("Tail Date"), \
                       SPCD.COINS_VAL.alias("Coinsurance Value"), \
                       SP.PLCY_EVNT_TP_CD.alias("Policy Event Type Code"), \
                       SPD.RNWL_CNT.alias("Renewal Count"), \
                       SPD.CANCTN_DESCR.alias("Cancellation Description"), \
                       SPCD.EXPSR_AMT.alias("Exposure Amount"), \
                       SPCD.EXPSR_BASE.alias("Exposure Base"), \
                       insdDF.SIC_CD_Insured.alias("Insured SIC Code"), \
                       SPD.REVSN_NBR.alias("Revision Number"), \
                       SIOP.PRPTY_LOC_NBR.alias("Insured Property Location Number")] \
                       + [col for col in SPL_pivot.columns if col.startswith("Policy Limit Amount")] \
                       + [col for col in SPCDED_pivot.columns if col.startswith("Coverage Deductible Amount")])
# arrowheadDF.display()
#             .where("ACCTG_YR_MO_FORMAT >= '2020-12-01' and ACCTG_YR_MO_FORMAT < '2022-01-01'") \
#             .where("year(sat_plcy_dtl.EFF_DT) = '2020'") \


# COMMAND ----------

# DBTITLE 1,prwinsinsr
prwinsinsrDF = LPPR.join(SO,["QBE_HASH_PRTY_ID"], "left") \
             .where(LPPR.REC_SRC_NM == '217') \
             .where(SO.REC_SRC_NM == '217') \
             .withColumn("PRTY_ROLE_TP_CODE", lower(LPPR.PRTY_ROLE_TP_CD)) \
             .where(col("PRTY_ROLE_TP_CODE") == 'insurer') \
             .withColumn("Insurerprwins", eval("first(SO.ORG_NM).over(Window.partitionBy(LPPR.QBE_HASH_PLCY_ID,LPPR.PRTY_ROLE_TP_CD).orderBy(desc(LPPR.BTCH_DT)))")) \
             .select(LPPR.QBE_HASH_PLCY_ID,'Insurerprwins').distinct()

# COMMAND ----------

# DBTITLE 1,prwinscmsn
prwinscmsnDF = LPT.join(SC,["QBE_HASH_PREM_TRAN_ID"], "left") \
                .where(LPT.REC_SRC_NM == '217') \
                .where(LPT.TRAN_TP_CD.isin(["Policy_Level_Commission"])) \
                .select(col("QBE_HASH_PLCY_ID").alias("QBE_HASH_PLCY_ID_cmsn"),"ENDRSMT_REVSN_NBR","CMSN_PCT","CMSN_AMT") \
                .distinct()

# COMMAND ----------

# DBTITLE 1,producer number from sat selling agency
SSA_PDNBR_DF = LPPR.join(SSA, ["QBE_HASH_PLCY_PRTY_ROLE_ID"], "inner") \
              .where(LPPR.PRTY_ROLE_TP_CD == 'Selling_Agency') \
              .where(LPPR.REC_SRC_NM == '217') \
              .where(SSA.REC_SRC_NM == '217') \
              .withColumn("rw", eval("row_number().over(Window.partitionBy(LPPR.QBE_HASH_PLCY_ID,SSA.PRODR_NBR).orderBy(desc(LPPR.QBE_HASH_PLCY_ID)))")) \
              .select(LPPR.QBE_HASH_PLCY_ID,SSA.PRODR_NBR.alias("PRODR_NBR_SSA"))

# COMMAND ----------

# DBTITLE 1,PRWINS
prwinsDF = HPY.join(SPD, HPY.QBE_HASH_PLCY_ID.eqNullSafe(SPD.QBE_HASH_PLCY_ID), "inner") \
            .join(LPT, HPY.QBE_HASH_PLCY_ID.eqNullSafe(LPT.QBE_HASH_PLCY_ID), "left") \
            .join(LPC, (LPC.QBE_HASH_PLCY_ID.eqNullSafe(LPT.QBE_HASH_PLCY_ID)) & (LPC.QBE_HASH_LOB_ID.eqNullSafe(LPT.QBE_HASH_LOB_ID)) & (LPC.QBE_HASH_INSD_OBJ_ID.eqNullSafe(LPT.QBE_HASH_INSD_OBJ_ID)) & (LPC.QBE_HASH_CVRG_ID.eqNullSafe(LPT.QBE_HASH_CVRG_ID)) & (LPC.QBE_HASH_JOB_CLS_CD_ID.eqNullSafe(LPT.QBE_HASH_JOB_CLS_CD_ID)) & (LPC.QBE_HASH_CNTCT_PNT_ID.eqNullSafe(LPT.QBE_HASH_CNTCT_PNT_ID)) & (LPT.REF_ID.eqNullSafe(LPC.REF_ID)), "left") \
            .join(SPCD, ["QBE_HASH_PLCY_CVRG_ID"], "left") \
            .join(SP, ["QBE_HASH_PREM_TRAN_ID"], "left") \
            .join(prwinscmsnDF, (LPT.QBE_HASH_PLCY_ID.eqNullSafe(prwinscmsnDF.QBE_HASH_PLCY_ID_cmsn)) & (SP.ENDRSMT_REVSN_NBR.eqNullSafe(prwinscmsnDF.ENDRSMT_REVSN_NBR)), "left") \
            .join(prwinsinsrDF, HPY.QBE_HASH_PLCY_ID.eqNullSafe(prwinsinsrDF.QBE_HASH_PLCY_ID), "left") \
               .join(insdDF, ["QBE_HASH_PLCY_ID"], "left") \
            .join(LKUP_DF_ST_WINS.withColumnRenamed("CDKEY3","ST_PRVNC_CD"), ["ST_PRVNC_CD"], "left") \
            .join(REF_LKUP, HPY.REC_SRC_NM == col("CD_VAL"), "left") \
            .join(SCVD, ["QBE_HASH_CVRG_ID"], "left") \
               .join(LPCP_SPA, ["QBE_HASH_PLCY_ID"], "left") \
               .join(SPL_pivot, ["QBE_HASH_PLCY_ID"], "left") \
               .join(SPCDED_pivot, ["QBE_HASH_PLCY_CVRG_ID"], "left") \
               .join(SIOP, ["QBE_HASH_INSD_OBJ_ID"], "left") \
            .join(SSA_PDNBR_DF, ["QBE_HASH_PLCY_ID"], "left") \
            .withColumnRenamed("CD_SHRT_DESC", "Source System Name") \
            .where(HPY.REC_SRC_NM == '217') \
            .where(LPT.TRAN_TP_CD.isin(["Coverage_Level_Premium"])) \
            .withColumn("Underwriting Year", year(SPD.EFF_DT)) \
            .withColumn("Commission Amount (Calculated)",(SP.CHGD_PREM_AMT * (prwinscmsnDF.CMSN_PCT/100)).cast(FloatType())) \
            .withColumn("Product Code",lit("")) \
            .withColumn("Annual Statement Line of Business1", expr("lpad(SAT_CVRG_DTL.ASLOB_CD,3,'0')")) \
            .withColumn("Annual Statement Line of Business", SCVD.ASLOB_CD) \
            .withColumn("Subline Code New", expr("lpad(SAT_CVRG_DTL.SUBLINE_CD,3,'0')")) \
            .withColumn("Subline Code", SCVD.SUBLINE_CD) \
            .withColumn("Company Number",lit("")) \
.withColumn("Coverage TP Code",lit("")) \
            .withColumn("Coverage Code",lit("")) \
            .withColumn("Claim_Made Indicator",lit("")) \
            .withColumn("Reinsurance Code",lit("")) \
            .withColumn("Line of Business",lit("")) \
            .withColumn("Bureau Type",lit("")) \
            .withColumn("Distribution Channel",lit("")) \
            .withColumn("Group Destination Code",lit("")) \
            .withColumn("Service Office",lit("")) \
            .withColumn("Master Account Number",lit("")) \
            .withColumn("Agency Number", SSA_PDNBR_DF.PRODR_NBR_SSA) \
            .withColumn("Policy Kind Code",lit("")) \
            .withColumn("Organization Code",lit("")) \
            .withColumn("Policy Type Code",lit("")) \
            .withColumn("Coverage Part Code",lit("")) \
            .withColumn("Market Segment Code",lit("")) \
            .withColumn("Policy Structure",lit("")) \
            .withColumn("Insurer SIC Code",lit("")) \
            .withColumn("Ownership Type Code",lit("")) \
            .withColumn("Treaty Code",lit("")) \
            .select(['Source System Name', \
                     HPY.REC_SRC_NM.alias('Source System Code'), \
                     prwinsinsrDF.Insurerprwins.alias("Writing Company"), \
                     SPD.PLCY_SYM_CD.alias("Policy Symbol"), \
                     SPD.PLCY_NBR.alias("Policy Number"), \
                     SPD.ORGNL_PLCY_NBR.alias("Original Policy Number"), \
                     "Product Code", \
                     SP.TRAN_DT.alias("Transaction Date"), \
                     SP.TRAN_EFF_DT.alias("Transaction Effective Date"), \
                     LPT.TRAN_TP_CD.alias("Transaction Type"), \
                     SP.TRAN_CD.alias("Transaction Code"), \
                     SP.TRAN_DESCR.alias("Transaction Desc"), \
                     SPD.EFF_DT.alias("Policy Effective Date"), \
                     SPD.EXPRN_DT.alias("Policy Expiration Date"), \
                     'Underwriting Year', \
                     SP.ACCTG_YR_MO_TXT.alias("Accounting Year Month"), \
                     LKUP_DF_ST_WINS.CDDESC.alias("Risk State"), \
                     SPCD.QBE_CELL_CD.alias("Cell"), \
                     SPCD.QBE_SGMT_CD.alias("Segment Code"), \
                     SPCD.QBE_FINCL_LN_OF_BUS_CD.alias("Financial Line of Business Code"), \
                     SP.CHGD_PREM_AMT.alias("Changed Premium Amount"), \
                     SP.WRTN_PREM_AMT.alias("Written Premium Amount"), \
                     prwinscmsnDF.CMSN_PCT.alias("Commission Percentage"), \
                     prwinscmsnDF.CMSN_AMT.alias("Commission Amount"), \
                     "Commission Amount (Calculated)", \
                     "Company Number", \
                     "Annual Statement Line of Business", \
                     "Coverage TP Code", \
                     "Coverage Code", \
                     "Subline Code", \
                     "Claim_Made Indicator", \
                     "Reinsurance Code", \
                     "Line of Business", \
                     "Bureau Type", \
                     "Distribution Channel", \
                     "Group Destination Code", \
                     "Service Office", \
                     "Master Account Number", \
                     "Agency Number", \
                     "Policy Kind Code", \
                     SPD.PGM_CD.alias("Program Code"), \
                     "Organization Code", \
                     SPD.SUB_PGM_CD.alias("Sub Program Code"), \
                     SPD.BUS_TP_CD.alias("Business Type Code"), \
                     "Policy Type Code", \
                     "Coverage Part Code", \
                     "Market Segment Code", \
                     "Policy Structure", \
                     "Insurer SIC Code", \
                     "Ownership Type Code", \
                     "Treaty Code", \
                       SP.TRAN_EXPRN_DT.alias("Transaction Expiration Date"), \
                       LPCP_SPA.PSTL_CD.alias("Primary Address Zip Code"), \
                       LPCP_SPA.CITY_NM.alias("Primary Address City"), \
                       LPCP_SPA.CNTY_NM.alias("Primary Address County"), \
                       SPCD.CLS_CD.alias("Class Code"), \
                       SPD.RETRO_DT.alias("Retroactive Date"), \
                       SPD.TAIL_DT.alias("Tail Date"), \
                       SPCD.COINS_VAL.alias("Coinsurance Value"), \
                       SP.PLCY_EVNT_TP_CD.alias("Policy Event Type Code"), \
                       SPD.RNWL_CNT.alias("Renewal Count"), \
                       SPD.CANCTN_DESCR.alias("Cancellation Description"), \
                       SPCD.EXPSR_AMT.alias("Exposure Amount"), \
                       SPCD.EXPSR_BASE.alias("Exposure Base"), \
                       insdDF.SIC_CD_Insured.alias("Insured SIC Code"), \
                       SPD.REVSN_NBR.alias("Revision Number"), \
                       SIOP.PRPTY_LOC_NBR.alias("Insured Property Location Number")] \
                       + [col for col in SPL_pivot.columns if col.startswith("Policy Limit Amount")] \
                       + [col for col in SPCDED_pivot.columns if col.startswith("Coverage Deductible Amount")])
# prwinsDF.display()

# COMMAND ----------

# DBTITLE 1,eslinsr
eslinsrDF = LPPR.join(SO,["QBE_HASH_PRTY_ID"], "left") \
             .where(LPPR.REC_SRC_NM == '61') \
             .where(SO.REC_SRC_NM == '61') \
             .withColumn("PRTY_ROLE_TP_CODE", lower(LPPR.PRTY_ROLE_TP_CD)) \
             .where(col("PRTY_ROLE_TP_CODE") == 'insurer') \
             .withColumn("Insureresl", eval("first(SO.ORG_NM).over(Window.partitionBy(LPPR.QBE_HASH_PLCY_ID,LPPR.PRTY_ROLE_TP_CD).orderBy(desc(LPPR.BTCH_DT)))")) \
             .select(LPPR.QBE_HASH_PLCY_ID,'Insureresl').distinct()

# COMMAND ----------

# DBTITLE 1,ESL
# LPT_ESL = LPT.fillna("")

# eslDF = HPY.join(SPD, ["QBE_HASH_PLCY_ID"], "inner") \
#             .join(LPT_ESL, ["QBE_HASH_PLCY_ID"], "left") \
#             .join(LPC.fillna(""),["QBE_HASH_PLCY_ID","QBE_HASH_CVRG_ID","QBE_HASH_INSD_OBJ_ID","QBE_HASH_LOB_ID","QBE_HASH_JOB_CLS_CD_ID","QBE_HASH_CNTCT_PNT_ID"],"left") \
#             .join(SPCD, ["QBE_HASH_PLCY_CVRG_ID"], "left") \
#             .join(SP, ["QBE_HASH_PREM_TRAN_ID"], "left") \
#             .join(SC, ["QBE_HASH_PREM_TRAN_ID"], "left") \
#             .join(eslinsrDF, ["QBE_HASH_PLCY_ID"], "left") \
#                .join(insdDF, ["QBE_HASH_PLCY_ID"], "left") \
#             .join(LKUP_DF_ST_WINS.withColumnRenamed("CDKEY3","ST_PRVNC_CD"), ["ST_PRVNC_CD"], "left") \
#             .join(REF_LKUP, HPY.REC_SRC_NM == col("CD_VAL"), "left") \
#             .join(SCVD, ["QBE_HASH_CVRG_ID"], "left") \
#                .join(LPCP_SPA, ["QBE_HASH_PLCY_ID"], "left") \
#                .join(SPL_pivot, ["QBE_HASH_PLCY_ID"], "left") \
#                .join(SPCDED_pivot, ["QBE_HASH_PLCY_CVRG_ID"], "left") \ 
#                .join(SIOP, ["QBE_HASH_INSD_OBJ_ID"], "left") \
#             .join(STD, ["QBE_HASH_TRTY_ID"], "left") \
#             .withColumnRenamed("CD_SHRT_DESC", "Source System Name") \
#             .where(HPY.REC_SRC_NM == '61') \
#             .where(LPT_ESL.TRAN_TP_CD.isin(["Coverage_Level_Premium","Coverage_Level_Contract_Premium"])) \
#             .withColumn("Underwriting Year", year(SPD.EFF_DT)) \
#             .withColumn("Commission Amount (Calculated)",(SP.CHGD_PREM_AMT * (SC.CMSN_PCT/100)).cast(FloatType())) \
#             .withColumn("Product Code",lit("")) \
#             .withColumn("Company Number",lit("")) \
#             .withColumn("Coverage TP Code",lit("")) \
#             .withColumn("Coverage Code",lit("")) \
#             .withColumn("Subline Code",lit("")) \
#             .withColumn("Claim_Made Indicator",lit("")) \
#             .withColumn("Reinsurance Code",lit("")) \
#             .withColumn("Line of Business",lit("")) \
#             .withColumn("Bureau Type",lit("")) \
#             .withColumn("Distribution Channel",lit("")) \
#             .withColumn("Group Destination Code",lit("")) \
#             .withColumn("Service Office",lit("")) \
#             .withColumn("Master Account Number",lit("")) \
#             .withColumn("Agency Number",lit("")) \
#             .withColumn("Policy Kind Code",lit("")) \
#             .withColumn("Program Code",lit("")) \
#             .withColumn("Organization Code",lit("")) \
#             .withColumn("Sub Program Code",lit("")) \
#             .withColumn("Business Type Code",lit("")) \
#             .withColumn("Policy Type Code",lit("")) \
#             .withColumn("Coverage Part Code",lit("")) \
#             .withColumn("Market Segment Code",lit("")) \
#             .withColumn("Policy Structure",lit("")) \
#             .withColumn("Insurer SIC Code",lit("")) \
#             .withColumn("Ownership Type Code",lit("")) \
#             .withColumn("REF_ID_TRTY", STD.REF_ID) \
#             .select(['Source System Name', \
#                      HPY.REC_SRC_NM.alias('Source System Code'), \
#                      eslinsrDF.Insureresl.alias("Writing Company"), \
#                      SPD.PLCY_SYM_CD.alias("Policy Symbol"), \
#                      SPD.PLCY_NBR.alias("Policy Number"), \
#                      SPD.ORGNL_PLCY_NBR.alias("Original Policy Number"), \
#                      "Product Code", \
#                      SP.TRAN_DT.alias("Transaction Date"), \
#                      SP.TRAN_EFF_DT.alias("Transaction Effective Date"), \
#                      LPT_ESL.TRAN_TP_CD.alias("Transaction Type"), \
#                      SP.TRAN_CD.alias("Transaction Code"), \
#                      SP.TRAN_DESCR.alias("Transaction Desc"), \
#                      SPD.EFF_DT.alias("Policy Effective Date"), \
#                      SPD.EXPRN_DT.alias("Policy Expiration Date"), \
#                      'Underwriting Year', \
#                      SP.ACCTG_YR_MO_TXT.alias("Accounting Year Month"), \
#                      SPD.CNTRL_ST_CD.alias("Risk State"), \
#                      SPCD.QBE_CELL_CD.alias("Cell"), \
#                      SPCD.QBE_SGMT_CD.alias("Segment Code"), \
#                      SPCD.QBE_FINCL_LN_OF_BUS_CD.alias("Financial Line of Business Code"), \
#                      SP.CHGD_PREM_AMT.alias("Changed Premium Amount"), \
#                      SP.WRTN_PREM_AMT.alias("Written Premium Amount"), \
#                      SC.CMSN_PCT.alias("Commission Percentage"), \
#                      SC.CMSN_AMT.alias("Commission Amount"), \
#                      "Commission Amount (Calculated)", \
#                      "Company Number", \
#                      SCVD.ASLOB_CD.alias("Annual Statement Line of Business"), \
#                      "Coverage TP Code", \
#                      "Coverage Code", \
#                      "Subline Code", \
#                      "Claim_Made Indicator", \
#                      "Reinsurance Code", \
#                      "Line of Business", \
#                      "Bureau Type", \
#                      "Distribution Channel", \
#                      "Group Destination Code", \
#                      "Service Office", \
#                      "Master Account Number", \
#                      "Agency Number", \
#                      "Policy Kind Code", \
#                      "Program Code", \
#                      "Organization Code", \
#                      "Sub Program Code", \
#                      "Business Type Code", \
#                      "Policy Type Code", \
#                      "Coverage Part Code", \
#                      "Market Segment Code", \
#                      "Policy Structure", \
#                      "Insurer SIC Code", \
#                      "Ownership Type Code", \
#                      col("REF_ID_TRTY").alias("Treaty Code"), \
#                        SP.TRAN_EXPRN_DT.alias("Transaction Expiration Date"), \
#                        LPCP_SPA.PSTL_CD.alias("Primary Address Zip Code"), \
#                        LPCP_SPA.CITY_NM.alias("Primary Address City"), \
#                        LPCP_SPA.CNTY_NM.alias("Primary Address County"), \
#                        SPCD.CLS_CD.alias("Class Code"), \
#                        SPD.RETRO_DT.alias("Retroactive Date"), \
#                        SPD.TAIL_DT.alias("Tail Date"), \
#                        SPCD.COINS_VAL.alias("Coinsurance Value"), \
#                        SP.PLCY_EVNT_TP_CD.alias("Policy Event Type Code"), \
#                        SPD.RNWL_CNT.alias("Renewal Count"), \
#                        SPD.CANCTN_DESCR.alias("Cancellation Description"), \
#                        SPCD.EXPSR_AMT.alias("Exposure Amount"), \
#                        SPCD.EXPSR_BASE.alias("Exposure Base"), \
#                        insdDF.SIC_CD_Insured.alias("Insured SIC Code"), \
#                        SPD.REVSN_NBR.alias("Revision Number"), \
#                        SIOP.PRPTY_LOC_NBR.alias("Insured Property Location Number")] \
#                        + [col for col in SPL_pivot.columns if col.startswith("Policy Limit Amount")] \
#                        + [col for col in SPCDED_pivot.columns if col.startswith("Coverage Deductible Amount")])
# # eslDF.display()				 

# COMMAND ----------

# DBTITLE 1,masjecoinsr
mjscinsrDF = LPPR.join(SO,["QBE_HASH_PRTY_ID"], "left") \
             .where(LPPR.REC_SRC_NM == '109') \
             .where(SO.REC_SRC_NM == '109') \
             .withColumn("PRTY_ROLE_TP_CODE", lower(LPPR.PRTY_ROLE_TP_CD)) \
             .where(col("PRTY_ROLE_TP_CODE") == 'insurer') \
             .withColumn("insr", eval("row_number().over(Window.partitionBy(LPPR.QBE_HASH_PLCY_ID).orderBy(desc(LPPR.BTCH_DT),desc(LPPR.QBE_HASH_PLCY_PRTY_ROLE_ID)))")) \
             .where(col("insr") == '1') \
             .select(LPPR.QBE_HASH_PLCY_ID,SO.ORG_NM,SO.SIC_CD,SO.OWNRSP_TP_CD)

# COMMAND ----------

# DBTITLE 1,majescocmsn
mjsccmsnDF = LPT.withColumnRenamed('BTCH_DT','BTCH_DT_LPT') \
                .withColumnRenamed('REF_ID','REF_ID_LPT') \
                .join(SC,["QBE_HASH_PREM_TRAN_ID"], "left") \
                .where(LPT.REC_SRC_NM == '109') \
                .where(LPT.TRAN_TP_CD.isin(["Policy_Level_Commission"])) \
                .withColumn("ENDRSMT_REVSN_NBR", when(SC.ENDRSMT_REVSN_NBR.isNull(),substring(SC.REF_ID,16,3).cast(IntegerType())).otherwise(SC.ENDRSMT_REVSN_NBR)) \
                .withColumn('RF',substring(SC.REF_ID,1,15)) \
                .withColumn("rn",expr("row_number() over (partition by QBE_HASH_PLCY_ID, RF, ENDRSMT_REVSN_NBR order by BTCH_DT_LPT desc, SRC_EFF_DT desc, REF_ID_LPT desc)"))\
                .where("rn = 1") \
                .groupBy(LPT.QBE_HASH_PLCY_ID,col('ENDRSMT_REVSN_NBR'),SC.CMSN_PCT,SC.ACCTG_YR_MO_TXT,col('RF')).agg(sum(SC.NET_CMSN_AMT).alias('NET_CMSN_AMT')) \
                .select("QBE_HASH_PLCY_ID","RF","ACCTG_YR_MO_TXT","ENDRSMT_REVSN_NBR","CMSN_PCT","NET_CMSN_AMT") \
                .distinct()

# COMMAND ----------

# DBTITLE 1,majescocmsnpct
mjsccmsnpctDF = LPT.join(SP,["QBE_HASH_PREM_TRAN_ID"], "inner") \
                   .where(LPT.REC_SRC_NM == '109') \
                   .withColumn('RF',substring(SP.REF_ID,1,15)) \
                   .groupBy(LPT.QBE_HASH_PLCY_ID,SP.ENDRSMT_REVSN_NBR,col('RF'),SP.ACCTG_YR_MO_TXT).agg(sum(SP.CHGD_PREM_AMT).alias('CHGD_PREM_AMT')) \
                   .join(mjsccmsnDF, ["QBE_HASH_PLCY_ID","ENDRSMT_REVSN_NBR","RF","ACCTG_YR_MO_TXT"], "left") \
                   .withColumn("CMSN_PCT_calculate", expr("cast((NET_CMSN_AMT/CHGD_PREM_AMT) * 100 as decimal(18,2))")) \
                   .select('QBE_HASH_PLCY_ID','RF','ENDRSMT_REVSN_NBR','ACCTG_YR_MO_TXT','CMSN_PCT_calculate').fillna(0)

# COMMAND ----------

Iso_cd = LPL.join(SPLD,["QBE_HASH_PLCY_LOB_ID"], "left") \
            .select(LPL.QBE_HASH_PLCY_ID,LPL.QBE_HASH_LOB_ID,SPLD.ISO_CD)

# COMMAND ----------

-- 100038065 exposure all balnk (No of Locations)
-- 100037354 duplicate premium
-- schedule modifier missing for MP

# COMMAND ----------

# Exposure Amount and Exposure Base
EXPSR_AMT = pivotDF(SPLE,"QBE_HASH_PLCY_LOB_ID","EXPSR_TP_CD","EXPSR_AMT","Line of Business Exposure Amount")
EXPSR_BASE = pivotDF(SPLE,"QBE_HASH_PLCY_LOB_ID","EXPSR_TP_CD","EXPSR_BASE","Line of Business Exposure Base")
EXPSR_V1 = EXPSR_AMT.join(EXPSR_BASE,EXPSR_AMT.QBE_HASH_PLCY_LOB_ID == EXPSR_BASE.QBE_HASH_PLCY_LOB_ID,"left") \
           .select([EXPSR_AMT.QBE_HASH_PLCY_LOB_ID]
                + [col for col in EXPSR_AMT.columns if col.startswith("Line of Business Exposure Amount")]
                + [col for col in EXPSR_BASE.columns if col.startswith("Line of Business Exposure Base")])
EXPSR = LPL.join(EXPSR_V1,LPL.QBE_HASH_PLCY_LOB_ID == EXPSR_V1.QBE_HASH_PLCY_LOB_ID, "left") \
        .select([LPL.QBE_HASH_PLCY_ID,LPL.QBE_HASH_LOB_ID] \
        + [col for col in EXPSR_V1.columns if col.startswith("Line of Business Exposure Amount")]
        + [col for col in EXPSR_V1.columns if col.startswith("Line of Business Exposure Base")])

# Schedule Modifier
SSM = spark.table("SAT_SCHED_MDFR").where("REC_SRC_NM == '109'")
SSM = SSM.na.fill('NA',["SCHED_MDFR_TP_CD"]) 
SSM_Pivot = pivotDF(SSM,"REF_ID","SCHED_MDFR_TP_CD","SCHED_MDFR_RT_PCT","Coverage Level Schedule Modifier Rate Percent").withColumnRenamed("REF_ID","SSM_REF_ID")

# COMMAND ----------

# DBTITLE 1,MAJESCO
# LPT_MJSC = LPT.fillna("")
majescoDF1 =HPY.join(SPD, HPY.QBE_HASH_PLCY_ID.eqNullSafe(SPD.QBE_HASH_PLCY_ID), "inner") \
            .join(LPT, HPY.QBE_HASH_PLCY_ID.eqNullSafe(LPT.QBE_HASH_PLCY_ID), "left") \
            .join(LPC, (LPC.QBE_HASH_PLCY_ID.eqNullSafe(LPT.QBE_HASH_PLCY_ID)) & (LPC.QBE_HASH_LOB_ID.eqNullSafe(LPT.QBE_HASH_LOB_ID)) & (LPC.QBE_HASH_INSD_OBJ_ID.eqNullSafe(LPT.QBE_HASH_INSD_OBJ_ID)) & (LPC.QBE_HASH_CVRG_ID.eqNullSafe(LPT.QBE_HASH_CVRG_ID)) &                                     (LPC.QBE_HASH_CNTCT_PNT_ID.eqNullSafe(LPT.QBE_HASH_CNTCT_PNT_ID)) & (LPC.QBE_HASH_JOB_CLS_CD_ID.eqNullSafe(LPT.QBE_HASH_JOB_CLS_CD_ID)) & (LPC.REF_ID.eqNullSafe(LPT.REF_ID)), "left") \
.join(SPCD, LPC.QBE_HASH_PLCY_CVRG_ID.eqNullSafe(SPCD.QBE_HASH_PLCY_CVRG_ID), "inner") \
               .join(SP, ["QBE_HASH_PREM_TRAN_ID"], "left").where(col("ACCTG_YR_MO_FINAL").between(rangeStartDate,rangeEndDate)) \
               .withColumn('RF',substring(SP.REF_ID,1,15)) \
               .join(mjsccmsnDF, ["QBE_HASH_PLCY_ID","ENDRSMT_REVSN_NBR","RF","ACCTG_YR_MO_TXT"], "left") \
               .join(mjsccmsnpctDF, ["QBE_HASH_PLCY_ID","ENDRSMT_REVSN_NBR","RF","ACCTG_YR_MO_TXT"], "left") \
               .join(mjscinsrDF, ["QBE_HASH_PLCY_ID"], "left") \
               .join(insdDF, ["QBE_HASH_PLCY_ID"], "left") \
               .join(REF_LKUP, HPY.REC_SRC_NM == col("CD_VAL"), "left") \
               .join(SCVD, ["QBE_HASH_CVRG_ID"], "left") \
               .join(LPCP_SPA, ["QBE_HASH_PLCY_ID"], "left") \
               .join(SPL_pivot, ["QBE_HASH_PLCY_ID"], "left") \
               .join(SPCDED_pivot, ["QBE_HASH_PLCY_CVRG_ID"], "left") \
               .join(SPCL_Pivot, ["QBE_HASH_PLCY_CVRG_ID"], "left") \
               .join(SIOP, ["QBE_HASH_INSD_OBJ_ID"], "left") \
               .join(Iso_cd, (LPT.QBE_HASH_PLCY_ID.eqNullSafe(Iso_cd.QBE_HASH_PLCY_ID)) & (LPT.QBE_HASH_LOB_ID.eqNullSafe(Iso_cd.QBE_HASH_LOB_ID)) , "left") \
               .join(EXPSR, (LPT.QBE_HASH_PLCY_ID.eqNullSafe(EXPSR.QBE_HASH_PLCY_ID)) & (LPT.QBE_HASH_LOB_ID.eqNullSafe(EXPSR.QBE_HASH_LOB_ID)) , "left") \
               .where(coalesce(SP.BNDR_FLG,lit("N")) == "N") \
               .where(SPD.PROD_CD .isin('BO','MP')) \
               .where(HPY.REC_SRC_NM == '109') \
               .where(LPT.TRAN_TP_CD.isin(["Coverage_Level_Premium"])) \
               .withColumn("Commission Amount (Calculated)",(SP.CHGD_PREM_AMT * (mjsccmsnpctDF.CMSN_PCT_calculate/100)).cast(FloatType())) \
               .withColumn("Underwriting Year", year(SPD.EFF_DT)) \
               .withColumnRenamed("CD_SHRT_DESC", "Source System Name") \
               .withColumn("Company Number",lit("")) \
               .withColumn("Reinsurance Code",lit("")) \
               .withColumn("Bureau Type",lit("")) \
               .withColumn("Distribution Channel",lit("")) \
               .withColumn("Group Destination Code",lit("")) \
               .withColumn("Service Office",lit("")) \
               .withColumn("Master Account Number",lit("")) \
               .withColumn("Agency Number",lit("")) \
               .withColumn("Policy Kind Code",lit("")) \
               .withColumn("Organization Code",lit("")) \
               .withColumn("Sub Program Code",lit("")) \
               .withColumn("Business Type Code",lit("")) \
               .withColumn("Treaty Code",lit("")) \
               .select(['Source System Name', \
                       HPY.REC_SRC_NM.alias('Source System Code'), \
                       LPT.REF_ID, \
                       mjscinsrDF.ORG_NM.alias('Writing Company'), \
                       SPD.PLCY_SYM_CD.alias("Policy Symbol"), \
                       SPD.PLCY_NBR.alias("Policy Number"), \
                       SPD.ORGNL_PLCY_NBR.alias("Original Policy Number"), \
                       SPD.PROD_CD.alias('Product Code'), \
                       SP.TRAN_DT.alias("Transaction Date"), \
                       SP.TRAN_EFF_DT.alias("Transaction Effective Date"), \
                       LPT.TRAN_TP_CD.alias("Transaction Type"), \
                       SP.TRAN_CD.alias("Transaction Code"), \
                       SP.TRAN_DESCR.alias("Transaction Desc"), \
                       SPD.EFF_DT.alias("Policy Effective Date"), \
                       SPD.EXPRN_DT.alias("Policy Expiration Date"), \
                       'Underwriting Year', \
                       SP.ACCTG_YR_MO_TXT.alias("Accounting Year Month"), \
                       LPT.ST_PRVNC_CD.alias("Risk State"), \
                       SPCD.QBE_CELL_CD.alias("Cell"), \
                       SPCD.QBE_SGMT_CD.alias("Segment Code"), \
                       SPCD.QBE_FINCL_LN_OF_BUS_CD.alias("Financial Line of Business Code"), \
                       SP.CHGD_PREM_AMT.alias("Changed Premium Amount"), \
                       SP.WRTN_PREM_AMT.alias("Written Premium Amount"), \
                       mjsccmsnDF.CMSN_PCT.alias("Commission Percentage"), \
                       mjsccmsnDF.NET_CMSN_AMT.alias("Commission Amount"), \
                       "Commission Amount (Calculated)", \
                       "Company Number", \
                       SCVD.CVRG_TP_CD.alias("Coverage Type Code"), \
                       SCVD.ASLOB_CD.alias("Annual Statement Line of Business"), \
                       SCVD.CVRG_CD.alias("Coverage Code"), \
                       SCVD.CVRG_DESCR.alias("Coverage Code Description"), \
                       SCVD.SUBLINE_CD.alias("Subline Code"), \
                       SPCD.CLM_MADE_FLG.alias("Claim_Made Indicator"), \
                       "Reinsurance Code", \
                       SCVD.LOB_CD.alias("Line of Business"), \
                       "Bureau Type", \
                       "Distribution Channel", \
                       "Group Destination Code", \
                       "Service Office", \
                       "Master Account Number", \
                       "Agency Number", \
                       "Policy Kind Code", \
                       SPD.PGM_CD.alias("Program Code"), \
                       "Organization Code", \
                       "Sub Program Code", \
                       "Business Type Code", \
                       SPD.PLCY_TP_CD.alias("Policy Type Code"), \
                       SCVD.CVRG_PART_CD.alias("Coverage Part Code"), \
                       SPD.MKT_SGMT_CD.alias("Market Segment Code"), \
                       SPD.PLCY_STRC_CD.alias("Policy Structure"), \
                       mjscinsrDF.SIC_CD.alias("Insurer SIC Code"), \
                       mjscinsrDF.OWNRSP_TP_CD.alias("Ownership Type Code"), \
                       "Treaty Code", \
                       SP.TRAN_EXPRN_DT.alias("Transaction Expiration Date"), \
                       LPCP_SPA.PSTL_CD.alias("Primary Address Zip Code"), \
                       LPCP_SPA.CITY_NM.alias("Primary Address City"), \
                       LPCP_SPA.CNTY_NM.alias("Primary Address County"), \
                       SPCD.CLS_CD.alias("Class Code"), \
                       SPD.RETRO_DT.alias("Retroactive Date"), \
                       SPD.TAIL_DT.alias("Tail Date"), \
                       SPD.EXTNDD_RPT_DT_NBR.alias("Extended RPT Number"), \
                       SPD.EXCS_ATTCHMT_PNT_AMT.alias("Extended Attachment PNT Number"), \
                       SPCD.COINS_VAL.alias("Coinsurance Value"), \
                       SP.PLCY_EVNT_TP_CD.alias("Policy Event Type Code"), \
                       SPD.RNWL_CNT.alias("Renewal Count"), \
                       SPD.CANCTN_DESCR.alias("Cancellation Description"), \
                       SPCD.EXPSR_AMT.alias("Exposure Amount"), \
                       SPCD.EXPSR_BASE.alias("Exposure Base"), \
                       insdDF.SIC_CD_Insured.alias("Insured SIC Code"), \
                       insdDF.NBR_OF_EMP_COUNT.alias("Number Of Employee Count"), \
                       SPD.REVSN_NBR.alias("Revision Number"), \
                       Iso_cd.ISO_CD.alias("ISO Code"), \
                       SIOP.PRPTY_LOC_NBR.alias("Insured Property Location Number")] \
                       + [col for col in SPL_pivot.columns if col.startswith("Policy Limit Amount")] \
                       + [col for col in SPCDED_pivot.columns if col.startswith("Coverage Deductible Amount")] \
                       + [col for col in SPCL_Pivot.columns if col.startswith("Coverage Limit Amount")] \
                       + [col for col in EXPSR.columns if col.startswith("Line of Business Exposure")]) 
# SCVD.QBE_HASH_CVRG_ID.alias("SCVD_QBE_HASH_CVRG_ID")

#.where(SPD.QBE_HASH_PLCY_ID.isin('52a10786ec790110500a864db2745764','54775228488e4e5cfd41144f1dab574d'))
#.where("ACCTG_YR_MO_TXT in ('2022_4','2022_5','2022_6')")

# COMMAND ----------

majescoDF = majescoDF1.join(SSM_Pivot,SSM_Pivot.SSM_REF_ID == majescoDF1.REF_ID,"left").drop("REF_ID","SSM_REF_ID")

# COMMAND ----------

majescoDF.createOrReplaceTempView("majescoDF")

# COMMAND ----------

majescoDF.count()

# COMMAND ----------

HPY.join(SPD, HPY.QBE_HASH_PLCY_ID.eqNullSafe(SPD.QBE_HASH_PLCY_ID), "inner") \
            .join(LPT, HPY.QBE_HASH_PLCY_ID.eqNullSafe(LPT.QBE_HASH_PLCY_ID), "left") \
            .join(LPC, (LPC.QBE_HASH_PLCY_ID.eqNullSafe(LPT.QBE_HASH_PLCY_ID)) & (LPC.QBE_HASH_LOB_ID.eqNullSafe(LPT.QBE_HASH_LOB_ID)) & (LPC.QBE_HASH_INSD_OBJ_ID.eqNullSafe(LPT.QBE_HASH_INSD_OBJ_ID)) & (LPC.QBE_HASH_CVRG_ID.eqNullSafe(LPT.QBE_HASH_CVRG_ID)) &                                     (LPC.QBE_HASH_CNTCT_PNT_ID.eqNullSafe(LPT.QBE_HASH_CNTCT_PNT_ID)) & (LPC.QBE_HASH_JOB_CLS_CD_ID.eqNullSafe(LPT.QBE_HASH_JOB_CLS_CD_ID)) & (LPC.REF_ID.eqNullSafe(LPT.REF_ID)), "left") \
.join(SPCD, LPC.QBE_HASH_PLCY_CVRG_ID.eqNullSafe(SPCD.QBE_HASH_PLCY_CVRG_ID), "inner") \
               .join(SP, ["QBE_HASH_PREM_TRAN_ID"], "left").where(col("ACCTG_YR_MO_FINAL").between(rangeStartDate,rangeEndDate)) \
              .where(coalesce(SP.BNDR_FLG,lit("N")) == "N") \
               .where("PROD_CD in ('BO','MP')") \
               .where(HPY.REC_SRC_NM == '109') \
               .where(LPT.TRAN_TP_CD.isin(["Coverage_Level_Premium"])).count()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from 
# MAGIC (select *, count(*) over (partition by QBE_HASH_PREM_TRAN_ID) as cnt from testDF)
# MAGIC where cnt > 1

# COMMAND ----------

majescoDF.where("`Original Policy Number` in ('100038065', '130000102')").select("Original Policy Number","Risk State","Changed Premium Amount","Accounting Year Month","Line of Business","Line of Business Exposure Amount - EmpCountTotal").display()

# COMMAND ----------

# DBTITLE 1,Format Column Name for Inrule & Export CSV/JSON -- 2022 Q2
import re
from pyspark.sql import functions as F
import pandas as pd
import json

exportDBFSPathCSV = "/dbfs/mnt/extractMount/inrule/" + source + "_BO_MP_Policy_Extract_" + "2021Q2" + ".csv"
exportCntrPathCSV = "/mnt/extractMount/inrule/" + source + "_BO_MP_Policy_Extract_" + "2021Q2" + ".csv"
exportDBFSPathJSON = "/dbfs/mnt/extractMount/inrule/" + source + "_BO_MP_Policy_Extract_" + "2021Q2" + ".json"

majescoFormattedDF = majescoDF.select([F.col(col).alias(col.replace(' ', '')) for col in majescoDF.columns]).withColumn("TransactionDate",expr("date(TransactionDate)"))
majescoFormattedDF = majescoFormattedDF.toDF(*[re.sub('[^0-9a-zA-Z$]+','_',x) for x in majescoFormattedDF.columns])
# majescoFormattedDF.display()
logger.info("Column Formatted Done...")

exportDF = majescoFormattedDF
exportDF.toPandas().to_csv(exportDBFSPathCSV,index=False,header=True,sep = ',',encoding="utf-8")
logger.info("Extract CSV Done...")

import pandas as pd
import json
import re
inputDF = spark.read.format("csv").option("header","true").load(exportCntrPathCSV)
df = inputDF.toPandas()
# regex = re.compile(r'[^\w]')
df_as_json_string = df.to_json(orient = 'records', index = 'true', date_format='iso').replace(" ","")

json_data = {}
json_data["ISO"] = json.loads(df_as_json_string)

with open(exportDBFSPathJSON, "w") as outfile:
  outfile.write(json.dumps(json_data))
logger.info("Extract JSON Done...")

# COMMAND ----------

from pyspark.sql.functions import *

df = spark.read.format("csv").option("header","true").load("/mnt/extractMount/inrule/Majesco_BO_Policy_Extract_" + "2021Q2Q3" + ".csv")
df.withColumn("TransactionDate",expr("date(TransactionDate)")).where("AnnualStatementLineofBusiness = '110'").display()

# COMMAND ----------

# DBTITLE 1,xminsr
xminsrDF = LPPR.join(SO,["QBE_HASH_PRTY_ID"], "left") \
             .where(LPPR.REC_SRC_NM == '220') \
             .where(SO.REC_SRC_NM == '220') \
             .withColumn("PRTY_ROLE_TP_CODE", lower(LPPR.PRTY_ROLE_TP_CD)) \
             .where(col("PRTY_ROLE_TP_CODE") == 'insurer') \
             .withColumn("insr", eval("row_number().over(Window.partitionBy(LPPR.QBE_HASH_PLCY_ID).orderBy(desc(LPPR.BTCH_DT),desc(LPPR.QBE_HASH_PLCY_PRTY_ROLE_ID)))")) \
             .where(col("insr") == '1') \
             .select(LPPR.QBE_HASH_PLCY_ID,SO.ORG_NM).distinct()

# COMMAND ----------

# DBTITLE 1,xmcmsn
xmcmsnDF = LPT.join(SC,["QBE_HASH_PREM_TRAN_ID"], "left") \
                .where(LPT.REC_SRC_NM == '220') \
                .where(LPT.TRAN_TP_CD.isin(["Coverage_Level_Commission","Endorsement_Level_Commission"])) \
                .select("QBE_HASH_PLCY_ID","QBE_HASH_LOB_ID","QBE_HASH_CVRG_ID",LPT.REF_ID.alias("REF_ID_LPT_XM"),"CMSN_PCT","CMSN_AMT").distinct()

# COMMAND ----------

# DBTITLE 1,XM
LPT_XM = LPT.fillna("").withColumnRenamed("REF_ID","REF_ID_LPT_XM")
LPC_XM = LPC.withColumn("rn",eval("row_number().over(Window.partitionBy(['QBE_HASH_PLCY_ID','QBE_HASH_CVRG_ID','QBE_HASH_INSD_OBJ_ID','QBE_HASH_LOB_ID','QBE_HASH_JOB_CLS_CD_ID','QBE_HASH_CNTCT_PNT_ID','REF_ID']).orderBy(desc('BTCH_DT')))")).where("rn = 1 ").fillna("").withColumnRenamed("REF_ID","REF_ID_LPT_XM")
SPCD_XM = SPCD.withColumn("rn",eval("row_number().over(Window.partitionBy(['QBE_HASH_PLCY_CVRG_ID']).orderBy(desc('BTCH_DT')))")).where("rn = 1 ")

xmDF = HPY.join(SPD, ["QBE_HASH_PLCY_ID"], "inner") \
          .join(LPT_XM,["QBE_HASH_PLCY_ID"], "left") \
          .join(LPC_XM, ["QBE_HASH_PLCY_ID","QBE_HASH_CVRG_ID","QBE_HASH_INSD_OBJ_ID","QBE_HASH_LOB_ID","QBE_HASH_JOB_CLS_CD_ID","QBE_HASH_CNTCT_PNT_ID","REF_ID_LPT_XM"], "left") \
          .join(SPCD_XM, ["QBE_HASH_PLCY_CVRG_ID"], "left") \
          .join(SP, ["QBE_HASH_PREM_TRAN_ID"], "left") \
          .join(xminsrDF, ["QBE_HASH_PLCY_ID"], "left") \
               .join(insdDF, ["QBE_HASH_PLCY_ID"], "left") \
               .join(LPCP_SPA, ["QBE_HASH_PLCY_ID"], "left") \
               .join(SPL_pivot, ["QBE_HASH_PLCY_ID"], "left") \
               .join(SPCDED_pivot, ["QBE_HASH_PLCY_CVRG_ID"], "left") \
               .join(SIOP, ["QBE_HASH_INSD_OBJ_ID"], "left") \
          .join(xmcmsnDF, ["QBE_HASH_PLCY_ID","QBE_HASH_LOB_ID","QBE_HASH_CVRG_ID","REF_ID_LPT_XM"], "left") \
          .where(HPY.REC_SRC_NM == '220') \
          .where(LPT_XM.TRAN_TP_CD.isin(["Coverage_Level_Premium","Endorsment_Level_Premium"])) \
          .withColumn("Underwriting Year", year(SPD.EFF_DT)) \
          .join(REF_LKUP, HPY.REC_SRC_NM == col("CD_VAL"), "left") \
          .withColumnRenamed("CD_SHRT_DESC", "Source System Name") \
          .withColumn("Commission Amount (Calculated)",(SP.CHGD_PREM_AMT * (xmcmsnDF.CMSN_PCT)).cast(FloatType())) \
          .withColumn("Product Code",lit("")) \
          .dropDuplicates(["QBE_HASH_PLCY_ID","REF_ID_LPT_XM"]) \
          .withColumn("Company Number",lit("")) \
          .withColumn("Annual Statement Line of Business",lit("")) \
            .withColumn("Coverage TP Code",lit("")) \
          .withColumn("Coverage Code",lit("")) \
          .withColumn("Subline Code",lit("")) \
          .withColumn("Claim_Made Indicator",lit("")) \
          .withColumn("Reinsurance Code",lit("")) \
          .withColumn("Line of Business",lit("")) \
          .withColumn("Bureau Type",lit("")) \
          .withColumn("Distribution Channel",lit("")) \
          .withColumn("Group Destination Code",lit("")) \
          .withColumn("Service Office",lit("")) \
          .withColumn("Master Account Number",lit("")) \
          .withColumn("Agency Number",lit("")) \
          .withColumn("Policy Kind Code",lit("")) \
          .withColumn("Program Code",lit("")) \
          .withColumn("Organization Code",lit("")) \
          .withColumn("Sub Program Code",lit("")) \
          .withColumn("Business Type Code",lit("")) \
          .withColumn("Policy Type Code",lit("")) \
          .withColumn("Coverage Part Code",lit("")) \
          .withColumn("Market Segment Code",lit("")) \
          .withColumn("Policy Structure",lit("")) \
          .withColumn("Insurer SIC Code",lit("")) \
          .withColumn("Ownership Type Code",lit("")) \
          .withColumn("Treaty Code",lit("")) \
          .select(['Source System Name', \
                  HPY.REC_SRC_NM.alias('Source System Code'), \
                  xminsrDF.ORG_NM.alias('Writing Company'), \
                  SPD.PLCY_SYM_CD.alias("Policy Symbol"), \
                  SPD.PLCY_NBR.alias("Policy Number"), \
                  SPD.ORGNL_PLCY_NBR.alias("Original Policy Number"), \
                  'Product Code', \
                  SP.TRAN_DT.alias("Transaction Date"), \
                  SP.TRAN_EFF_DT.alias("Transaction Effective Date"), \
                  LPT_XM.TRAN_TP_CD.alias("Transaction Type"), \
                  SP.TRAN_CD.alias("Transaction Code"), \
                  SP.TRAN_DESCR.alias("Transaction Desc"), \
                  SPD.EFF_DT.alias("Policy Effective Date"), \
                  SPD.EXPRN_DT.alias("Policy Expiration Date"), \
                  "Underwriting Year", \
                  SP.ACCTG_YR_MO_TXT.alias("Accounting Year Month"), \
                  LPT_XM.ST_PRVNC_CD.alias("Risk State"), \
                  SPCD.QBE_CELL_CD.alias("Cell"), \
                  SPCD.QBE_SGMT_CD.alias("Segment Code"), \
                  SPCD.QBE_FINCL_LN_OF_BUS_CD.alias("Financial Line of Business Code"), \
                  SP.CHGD_PREM_AMT.alias("Changed Premium Amount"), \
                  SP.WRTN_PREM_AMT.alias("Written Premium Amount"), \
                  xmcmsnDF.CMSN_PCT.alias("Commission Percentage"), \
                  xmcmsnDF.CMSN_AMT.alias("Commission Amount"), \
                  "Commission Amount (Calculated)",
                  "Company Number", \
                  "Annual Statement Line of Business", \
                 "Coverage TP Code", \
                  "Coverage Code", \
                  "Subline Code", \
                  "Claim_Made Indicator", \
                  "Reinsurance Code", \
                  "Line of Business", \
                  "Bureau Type", \
                  "Distribution Channel", \
                  "Group Destination Code", \
                  "Service Office", \
                  "Master Account Number", \
                  "Agency Number", \
                  "Policy Kind Code", \
                  "Program Code", \
                  "Organization Code", \
                  "Sub Program Code", \
                  "Business Type Code", \
                  "Policy Type Code", \
                  "Coverage Part Code", \
                  "Market Segment Code", \
                  "Policy Structure", \
                  "Insurer SIC Code", \
                  "Ownership Type Code", \
                  "Treaty Code", \
                       SP.TRAN_EXPRN_DT.alias("Transaction Expiration Date"), \
                       LPCP_SPA.PSTL_CD.alias("Primary Address Zip Code"), \
                       LPCP_SPA.CITY_NM.alias("Primary Address City"), \
                       LPCP_SPA.CNTY_NM.alias("Primary Address County"), \
                       SPCD.CLS_CD.alias("Class Code"), \
                       SPD.RETRO_DT.alias("Retroactive Date"), \
                       SPD.TAIL_DT.alias("Tail Date"), \
                       SPCD.COINS_VAL.alias("Coinsurance Value"), \
                       SP.PLCY_EVNT_TP_CD.alias("Policy Event Type Code"), \
                       SPD.RNWL_CNT.alias("Renewal Count"), \
                       SPD.CANCTN_DESCR.alias("Cancellation Description"), \
                       SPCD.EXPSR_AMT.alias("Exposure Amount"), \
                       SPCD.EXPSR_BASE.alias("Exposure Base"), \
                       insdDF.SIC_CD_Insured.alias("Insured SIC Code"), \
                       SPD.REVSN_NBR.alias("Revision Number"), \
                       SIOP.PRPTY_LOC_NBR.alias("Insured Property Location Number")] \
                       + [col for col in SPL_pivot.columns if col.startswith("Policy Limit Amount")] \
                       + [col for col in SPCDED_pivot.columns if col.startswith("Coverage Deductible Amount")])
#          .where(SP.ACCTG_YR_MO_TXT == '2021_01') \

# COMMAND ----------

# DBTITLE 1,OUTPUT CSV - POLICIES - COMBINE ALL Sources
combinedPlcyDF = gainDF.union(arrowheadDF).union(prwinsDF).union(eslDF).union(majescoDF).union(xmDF)
# print("Policy DF count: " + str(combinedPlcyDF.count()))
# combinedPlcyDF.toPandas().to_csv("/dbfs/mnt/extractMount/inrule/Policy_Extract_" + "20220606" + ".csv",index=False,header=True,sep = ',',encoding="utf-8")

# COMMAND ----------

# DBTITLE 1,OUTPUT CSV - Majesco
# print("Policy DF count: " + str(majescoDF.count()))
# filepath = "/dbfs/FileStore/extract/majesco_data.csv"
majescoDF.toPandas().to_csv("/dbfs/mnt/extractMount/inrule/Policy_Extract_" + "Majesco" + "_20220606" + ".csv",index=False,header=True,sep = ',',encoding="utf-8")

# COMMAND ----------

# DBTITLE 1,LPC CC
LCP_CC = LCP.withColumn("rowNum", eval("row_number().over(Window.partitionBy(col('QBE_HASH_CLM_ID')).orderBy(desc(col('BTCH_DT'))))")) \
                     .where(col('REC_SRC_NM').isin('45')) \
                     .where(col("rowNum") == '1')

# COMMAND ----------

# DBTITLE 1,CLAIM CENTER
claimcenterDF = SCD.join(LCP_CC, ["QBE_HASH_CLM_ID"], "left") \
                   .join(SPD, ["QBE_HASH_PLCY_ID"], "left") \
                   .join(LCT, ["QBE_HASH_CLM_ID"], "left") \
                   .join(SCTD, ["QBE_HASH_CLM_FINCL_TRAN_ID"], "left") \
                   .join(SED, ["QBE_HASH_EXPSR_ID"], "left") \
                   .where(SCD.REC_SRC_NM == '45') \
                   .join(REF_LKUP, SCD.REC_SRC_NM == col("CD_VAL"), "left") \
                   .withColumnRenamed("CD_SHRT_DESC", "Claim Source Name") \
                   .withColumn("Accident Year", year(SCD.LOSS_DT)) \
                   .where("(ACCTG_YR_NBR = '2020' and ACCTG_MO_NBR = '12') or (ACCTG_YR_NBR = '2021')") \
                   .select('Claim Source Name', \
                           SCD.REC_SRC_NM.alias("Claim Source Code"), \
                           SCD.SRC_SYS_REC_NM.alias("UW Source Name"), \
                           SCTD.ACCTG_YR_NBR.alias("Accounting Year"), \
                           SCTD.ACCTG_MO_NBR.alias("Accounting Month"), \
                           SCD.WRTG_CO_NM.alias("Writing Company"), \
                           SCD.CLM_NBR.alias("Claim Number"), \
                           SCD.CLM_PUB_ID.alias("Claim Pubilc Id"), \
                           SCD.PLCY_NBR.alias("Policy Number"), \
                           SCD.PLCY_EFF_DT.cast('date').alias("Policy Effective Date"), \
                           SCTD.TRAN_DT.cast('date').alias("Transaction Date"), \
                           SCTD.TRAN_TP_DESCR.alias("Transaction Type"), \
                           SCTD.COST_TP_DESCR.alias("Cost Type"), \
                           SCTD.COST_CTGRY_CD.alias("Cost Category Code"), \
                           SCTD.COST_CTGRY_DESCR.alias("Cost Category Type"), \
                           SCD.CLM_STS_DESCR.alias("Claim Status"), \
                           SCD.LOSS_DT.cast('date').alias("Date of Loss"), \
                           'Accident Year', \
                           SCD.LOSS_LOC_ST_NM.alias("Loss State"), \
                           SCTD.ERD_RSRV_FLG.alias("Erode Reserve Flag"), \
                           SED.QBE_CELL_CD.alias("Cell"), \
                           SED.QBE_SGMT_CD.alias("Segment Code"), \
                           SED.QBE_FINCL_LN_OF_BUS_CD.alias("Financial Line of Business"), \
                           SCTD.TRAN_AMT.alias("Transaction Amount"), \
                           SCTD.RSRV_LN_ID.alias("Reserve Line Public Id"), \
                           SCTD.BTCH_DT.alias("Batch Date"), \
                           SCTD.TRAN_STS_DESCR.alias("Transaction Status"), \
                           SCTD.RETRD_FLG.alias("Retired Flag"))
# claimcenterDF.display()

# COMMAND ----------

# DBTITLE 1,DY ESL - CLAIMS
eslclaimsDF = SCD.join(LCP, ["QBE_HASH_CLM_ID"], "left") \
                   .join(SPD, ["QBE_HASH_PLCY_ID"], "left") \
                   .join(eslinsrDF, ["QBE_HASH_PLCY_ID"], "left") \
                   .join(LCT, ["QBE_HASH_CLM_ID"], "left") \
                   .join(SCTD, ["QBE_HASH_CLM_FINCL_TRAN_ID"], "left") \
                   .where(SCD.REC_SRC_NM == '61') \
                   .join(REF_LKUP, SCD.REC_SRC_NM == col("CD_VAL"), "left") \
                   .withColumnRenamed("CD_SHRT_DESC", "Claim Source Name") \
                   .withColumn("Accident Year", year(SCD.LOSS_DT)) \
                   .where("(ACCTG_YR_NBR = '2020' and ACCTG_MO_NBR = '12') or (ACCTG_YR_NBR = '2021')") \
                   .select('Claim Source Name', \
                           SPD.REC_SRC_NM.alias("Claim Source Code"), \
                           SCD.SRC_SYS_REC_NM.alias("UW Source Name"), \
                           SCTD.ACCTG_YR_NBR.alias("Accounting Year"), \
                           SCTD.ACCTG_MO_NBR.alias("Accounting Month"), \
                           eslinsrDF.Insureresl.alias("Writing Company"), \
                           SCD.CLM_NBR.alias("Claim Number"), \
                           SCD.CLM_PUB_ID.alias("Claim Pubilc Id"), \
                           SCD.PLCY_NBR.alias("Policy Number"), \
                           SPD.EFF_DT.alias("Policy Effective Date"), \
                           SCTD.TRAN_DT.cast('date').alias("Transaction Date"), \
                           SCTD.TRAN_TP_CD.alias("Transaction Type"), \
                           SCTD.COST_TP_DESCR.alias("Cost Type"), \
                           SCTD.COST_CTGRY_CD.alias("Cost Category Code"), \
                           SCTD.COST_CTGRY_DESCR.alias("Cost Category Type"), \
                           SCD.CLM_STS_DESCR.alias("Claim Status"), \
                           SCD.LOSS_DT.cast('date').alias("Date of Loss"), \
                           'Accident Year', \
                           SCD.LOSS_LOC_ST_CD.alias("Loss State"), \
                           SCTD.ERD_RSRV_FLG.alias("Erode Reserve Flag"), \
                           SCD.QBE_CELL_CD.alias("Cell"), \
                           SCD.QBE_SGMT_CD.alias("Segment Code"), \
                           SCD.QBE_FINCL_LN_OF_BUS_CD.alias("Financial Line of Business"), \
                           SCTD.TRAN_AMT.alias("Transaction Amount"), \
                           SCTD.RSRV_LN_ID.alias("Reserve Line Public Id"), \
                           SCTD.BTCH_DT.alias("Batch Date"), \
                           SCTD.TRAN_STS_DESCR.alias("Transaction Status"), \
                           SCTD.RETRD_FLG.alias("Retired Flag"))

# COMMAND ----------

# DBTITLE 1,JURIS - CLAIMS
jurisDF = SCD.join(LCP, ["QBE_HASH_CLM_ID"], "left") \
             .join(SPD, ["QBE_HASH_PLCY_ID"], "left") \
             .join(LCT, ["QBE_HASH_CLM_ID"], "left") \
             .join(SCTD, ["QBE_HASH_CLM_FINCL_TRAN_ID"], "left") \
             .where(SCD.REC_SRC_NM == '105') \
             .join(REF_LKUP, SCD.REC_SRC_NM == col("CD_VAL"), "left") \
             .withColumn("TRAN_AMT_LEAD",eval("lead(SCTD.TRAN_AMT).over(Window.partitionBy('QBE_HASH_CLM_ID',SCTD.COST_CTGRY_CD,SCTD.COST_TP_DESCR,SCTD.RSRV_LN_ID,SCTD.TRAN_TP_CD).orderBy(desc(SCTD.TRAN_DT)))")) \
             .withColumnRenamed("CD_SHRT_DESC", "Claim Source Name") \
             .withColumn("Accident Year", year(SCD.LOSS_DT)) \
                   .where("(ACCTG_YR_NBR = '2020' and ACCTG_MO_NBR = '12') or (ACCTG_YR_NBR = '2021')") \
             .select('Claim Source Name', \
                     SCD.REC_SRC_NM.alias("Claim Source Code"), \
                     SCD.SRC_SYS_REC_NM.alias("UW Source Name"), \
                     SCTD.ACCTG_YR_NBR.alias("Accounting Year"), \
                     SCTD.ACCTG_MO_NBR.alias("Accounting Month"), \
                     SCD.WRTG_CO_CD.alias("Writing Company"), \
                     SCD.CLM_NBR.alias("Claim Number"), \
                     SCD.CLM_PUB_ID.alias("Claim Pubilc Id"), \
                     SCD.PLCY_NBR.alias("Policy Number"), \
                     SCD.PLCY_EFF_DT.cast('date').alias("Policy Effective Date"), \
                     SCTD.TRAN_DT.cast('date').alias("Transaction Date"), \
                     SCTD.TRAN_TP_DESCR.alias("Transaction Type"), \
                     SCTD.COST_TP_DESCR.alias("Cost Type"), \
                     SCTD.COST_CTGRY_CD.alias("Cost Category Code"), \
                     SCTD.COST_CTGRY_DESCR.alias("Cost Category Type"), \
                     SCD.CLM_STS_CD.alias("Claim Status"), \
                     SCD.LOSS_DT.cast('date').alias("Date of Loss"), \
                     'Accident Year', \
                     SCD.LOSS_LOC_ST_CD.alias("Loss State"), \
                     SCTD.ERD_RSRV_FLG.alias("Erode Reserve Flag"), \
                     SCD.QBE_CELL_CD.alias("Cell"), \
                     SCD.QBE_SGMT_CD.alias("Segment Code"), \
                     SCD.QBE_FINCL_LN_OF_BUS_CD.alias("Financial Line of Business"), \
                     SCTD.TRAN_AMT.alias("Transaction Amount"), \
                     SCTD.RSRV_LN_ID.alias("Reserve Line Public Id"), \
                     SCTD.BTCH_DT.alias("Batch Date"), \
                     SCTD.TRAN_STS_DESCR.alias("Transaction Status"), \
                     SCTD.RETRD_FLG.alias("Retired Flag"))

# COMMAND ----------

# DBTITLE 1,DYESL - CLAIMS - RESERVE
eslclaimsreserveDF = SCD.join(LCP, ["QBE_HASH_CLM_ID"], "left") \
                   .join(SPD, ["QBE_HASH_PLCY_ID"], "left") \
                   .join(eslinsrDF, ["QBE_HASH_PLCY_ID"], "left") \
                   .join(LCT, ["QBE_HASH_CLM_ID"], "left") \
                   .join(SCTD, ["QBE_HASH_CLM_FINCL_TRAN_ID"], "left") \
                   .where(SCD.REC_SRC_NM == '61') \
                   .join(REF_LKUP, SCD.REC_SRC_NM == col("CD_VAL"), "left") \
                   .withColumnRenamed("CD_SHRT_DESC", "Claim Source Name") \
                   .withColumn("Accident Year", year(SCD.LOSS_DT)) \
                   .where(SCTD.TRAN_TP_CD == 'Reserve') \
                   .select('Claim Source Name', \
                           SPD.REC_SRC_NM.alias("Claim Source Code"), \
                           SCD.SRC_SYS_REC_NM.alias("UW Source Name"), \
                           SCTD.ACCTG_YR_NBR.alias("Accounting Year"), \
                           SCTD.ACCTG_MO_NBR.alias("Accounting Month"), \
                           eslinsrDF.Insureresl.alias("Writing Company"), \
                           SCD.CLM_NBR.alias("Claim Number"), \
                           SCD.CLM_PUB_ID.alias("Claim Pubilc Id"), \
                           SCD.PLCY_NBR.alias("Policy Number"), \
                           SPD.EFF_DT.alias("Policy Effective Date"), \
                           SCTD.TRAN_DT.cast('date').alias("Transaction Date"), \
                           SCTD.TRAN_TP_CD.alias("Transaction Type"), \
                           SCTD.COST_TP_DESCR.alias("Cost Type"), \
                           SCTD.COST_CTGRY_CD.alias("Cost Category Code"), \
                           SCTD.COST_CTGRY_DESCR.alias("Cost Category Type"), \
                           SCD.CLM_STS_DESCR.alias("Claim Status"), \
                           SCD.LOSS_DT.cast('date').alias("Date of Loss"), \
                           'Accident Year', \
                           SCD.LOSS_LOC_ST_CD.alias("Loss State"), \
                           SCTD.ERD_RSRV_FLG.alias("Erode Reserve Flag"), \
                           SCD.QBE_CELL_CD.alias("Cell"), \
                           SCD.QBE_SGMT_CD.alias("Segment Code"), \
                           SCD.QBE_FINCL_LN_OF_BUS_CD.alias("Financial Line of Business"), \
                           SCTD.TRAN_AMT.alias("Transaction Amount"), \
                           SCTD.RSRV_LN_ID.alias("Reserve Line Public Id"), \
                           SCTD.BTCH_DT.alias("Batch Date"), \
                           SCTD.TRAN_STS_DESCR.alias("Transaction Status"), \
                           SCTD.RETRD_FLG.alias("Retired Flag"))

# COMMAND ----------

# DBTITLE 1,OUTPUT CSV - CLAIMS- COMBINE ALL Sources
combinedClmDF = claimcenterDF.union(eslclaimsDF).union(eslclaimsreserveDF).union(jurisDF)
print("Claim DF count: " + str(combinedClmDF.count()))
combinedClmDF.toPandas().to_csv("/dbfs/mnt/extractMount/FTP_Extract/FTP_Claim_Extract_" + "202012_202112" + ".csv",index=False,header=True,sep = ',',encoding="utf-8")

# COMMAND ----------

# DBTITLE 1,Export for CC only
claimcenterDF = claimcenterDF.withColumn("Accounting Year",col("Accounting Year").cast("int").cast("string")).withColumn("Accounting Month",col("Accounting Month").cast("int").cast("string")).withColumn("Accident Year",col("Accident Year").cast("int").cast("string"))
claimcenterDF.toPandas().to_csv("/dbfs/mnt/extractMount/FTP_Extract/FTP_Claim_Extract_" + "CC_202012_202112" + ".csv",index=False,header=True,sep = ',',encoding="utf-8")

# COMMAND ----------

combinedClmDF = claimcenterDF.union(eslclaimsDF).union(eslclaimsreserveDF).union(jurisDF).withColumn("Accounting Year",col("Accounting Year").cast("int").cast("string")).withColumn("Accounting Month",col("Accounting Month").cast("int").cast("string")).withColumn("Accident Year",col("Accident Year").cast("int").cast("string"))
print("Claim DF count: " + str(combinedClmDF.count()))

combinedClmDF1 = combinedClmDF.where("(`Accounting Year` is null) or (`Accounting Year` = '2021')")
# print("Claim DF1 count: " + str(combinedClmDF1.count()))
combinedClmDF1.toPandas().to_csv("/dbfs/mnt/extractMount/FTP_Extract/FTP_Claim_Extract_" + "202101_202112" + ".csv",index=False,header=True,sep = ',',encoding="utf-8")

combinedClmDF2 = combinedClmDF.where("(`Accounting Year` is null) or (`Accounting Year` = '2020' and `Accounting Month` = '12') or (`Accounting Year` = '2021' and int(`Accounting Month`) < 12)")
# print("Claim DF2 count: " + str(combinedClmDF2.count()))
combinedClmDF2.toPandas().to_csv("/dbfs/mnt/extractMount/FTP_Extract/FTP_Claim_Extract_" + "202012_202111" + ".csv",index=False,header=True,sep = ',',encoding="utf-8")

# COMMAND ----------

combinedClmDF3 = combinedClmDF.withColumn("Accounting Year",col("Accounting Year").cast("int").cast("string")).withColumn("Accounting Month",col("Accounting Month").cast("int").cast("string")).withColumn("Accident Year",col("Accident Year").cast("int").cast("string"))
# print("Claim DF2 count: " + str(combinedClmDF2.count()))
combinedClmDF3.toPandas().to_csv("/dbfs/mnt/extractMount/FTP_Extract/FTP_Claim_Extract_" + "202012_202106_new" + ".csv",index=False,header=True,sep = ',',encoding="utf-8")
