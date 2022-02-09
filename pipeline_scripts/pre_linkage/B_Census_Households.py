# ----------------------- #
# -------- SETUP -------- #
# ----------------------- #

# Import PySpark, Parameters, File Paths, Functions & Packages
import pyspark
from CCSLink import Parameters
from CCSLink.Parameters import FILE_PATH
from CCSLink import Person_Functions as PF
from CCSLink import Household_Functions as HF
from CCSLink import Cluster_Function as CF
exec(open("/home/cdsw/collaborative_method_matching/CCSLink/Packages.py").read()) 

# Changes to SparkSession 
sparkSession.conf.set('spark.sql.codegen.wholeStage', 'false')

# ----------------------- #
# -------- DATA --------- #
# ----------------------- #            

# Set year, month, date for file path
YEAR, MONTH, DAY =  '2021', '11', '16'

# Read in CEN HH data
cen = sparkSession.read.csv('/data/dap/c21_processing_zone/c21_cmatch_hdfs_h/file/cms/census/{}/{}/{}/raw/census_households/census_households.csv'.format(YEAR, MONTH, DAY), header = True).persist()
cen.count()

# Select columns
cen = cen.selectExpr('qid as qid_cen', 'response_id as response_id_cen', 'household_id as hh_id_cen', 'ownership_type as tenure_cen', 'accommodation_type as typaccom_cen', 'number_of_residents as no_resi_cen', 'From_Dummy')

# ------------------------------------------ #
# ------ ADD VARIABLES FROM QID TABLE ------ #
# ------------------------------------------ #  

# CEN Questionnaire data
cen_q = sparkSession.read.csv('/data/dap/c21_processing_zone/c21_cmatch_hdfs_h/file/cms/census/{}/{}/{}/raw/census_questionnaires/census_questionnaires.csv'.format(YEAR, MONTH, DAY), header = True)
cen_q = cen_q.selectExpr('response_id as response_id_cen', 'display_address', 'address_raw', 'address', 'address_postcode as pc_cen', 'uprn as uprn_cen').drop_duplicates()

# Replace -9 & -8 with None
for variable in ['display_address', 'address', 'address_raw', 'pc_cen']:
  cen_q = cen_q.withColumn(variable, when(col(variable).isin(['-9', '-8']), lit(None)).otherwise(col(variable)))

# Add comma to address_raw (helps with house number function later on)
cen_q = cen_q.withColumn('address_raw', concat(col('address_raw'), lit(',')))
    
# Replace missing address with raw address then display address (equivalent to CMS variable 'address_combined')
cen_q = cen_q.withColumn('address_cen', when(col('address').isNull() == True, col('address_raw')).otherwise(col('address'))).drop('address_raw', 'address')
cen_q = cen_q.withColumn('address_cen', when(col('address_cen').isNull() == True, col('display_address')).otherwise(col('address_cen'))).drop('display_address')

# Clean Postcode
cen_q = cen_q.withColumn('pc_cen', upper(regexp_replace(col('pc_cen'), "[^0-9A-Za-z]+", "")))
cen_q = cen_q.withColumn('pc_cen', when(col('pc_cen') == '', None).otherwise(col('pc_cen')))

# Join variables on via response_id
cen = cen.join(cen_q, on = 'response_id_cen', how = 'left')

# Create sect/dist/area 
cen = cen.withColumn('pc_sect_cen', F.expr("substring({0},1, length({0}) - 2)".format("pc_cen")))\
             .withColumn('pc_dist_cen', F.expr("""IF(length({0}) = 5, substring({0},1,4), IF(length({0}) =4, substring({0},1,3), substring({0},1,2)))""".format("pc_sect_cen")))\
             .withColumn('pc_area_cen', F.expr("""IF(substring({0},2,1) in('1','2','3','4','5','6','7','8','9'), substr({0},1,1), substring({0},1,2))""".format("pc_sect_cen")))

# --------------------------------- #
# -------- PERSON VARIABLES ------- #
# --------------------------------- #  

# Read in Census person data
cen_ppl = sparkSession.read.parquet(FILE_PATH('Stage_1_clean_census')).selectExpr('id_cen', 'hh_id_cen', 'fn1_cen as FN', 'sn1_cen as SN', 'dob_cen as DOB', 'age_cen as AGE')

# Remove records with no household ID
cen_ppl = cen_ppl.filter(col('hh_id_cen') != '-9')

# ----------------------------- #
# -------- MISSINGNESS -------- #
# ----------------------------- #   

# FN / SN
cen_ppl = cen_ppl.withColumn('FN', when(cen_ppl.FN.isNull(), 'XXXXX').otherwise(cen_ppl.FN))
cen_ppl = cen_ppl.withColumn('SN', when(cen_ppl.SN.isNull(), 'XXXXX').otherwise(cen_ppl.SN))

# DOB
cen_ppl = cen_ppl.withColumn('DOB', when(cen_ppl.DOB.isNull(), '1800-08-08').otherwise(cen_ppl.DOB))
cen_ppl = cen_ppl.withColumn('DOB', to_date('DOB', 'yyyy-MM-dd'))

# AGE
cen_ppl = cen_ppl.withColumn('AGE', when(cen_ppl.AGE.isNull(), '888').otherwise(cen_ppl.AGE))

# ------------------------- #
# -------- HH SIZE -------- #
# ------------------------- #  

# Count number of IDs for each HH - different to number of usual residents
cen_ppl = cen_ppl.withColumn('hh_size_cen', size(collect_set('id_cen').over(Window.partitionBy('hh_id_cen')))).drop('id_cen')

# ------------------------------ #
# --------- PERSON SETS -------- #
# ------------------------------ #  

# Create 4 columns which contains list of all unique forenames / surnames / dobs / ages from that household
cen_ppl = cen_ppl.withColumn('fn_set_cen',   collect_set('FN').over(Window.partitionBy('hh_id_cen')))\
                 .withColumn('sn_set_cen',   collect_set('SN').over(Window.partitionBy('hh_id_cen')))\
                 .withColumn('dob_set_cen',  collect_set('DOB').over(Window.partitionBy('hh_id_cen')))\
                 .withColumn('age_set_cen',  collect_set('AGE').over(Window.partitionBy('hh_id_cen')))\
                 .drop('FN', 'SN', 'DOB', 'AGE')\
                 .drop_duplicates(['hh_id_cen'])\

# Array missing values
cen_ppl = cen_ppl.withColumn('fn_set_cen',  when(cen_ppl.fn_set_cen.isNull(),  array(lit('XXXXX'))).otherwise(cen_ppl.fn_set_cen))
cen_ppl = cen_ppl.withColumn('sn_set_cen',  when(cen_ppl.sn_set_cen.isNull(),  array(lit('XXXXX'))).otherwise(cen_ppl.sn_set_cen))
cen_ppl = cen_ppl.withColumn('dob_set_cen', when(cen_ppl.dob_set_cen.isNull(), array(lit('1800-08-08'))).otherwise(cen_ppl.dob_set_cen))
cen_ppl = cen_ppl.withColumn('age_set_cen', when(cen_ppl.age_set_cen.isNull(), array(lit('888'))).otherwise(cen_ppl.age_set_cen))

# Sort arrays
for var in ['fn_set_cen', 'sn_set_cen', 'dob_set_cen', 'age_set_cen']:
  cen_ppl = cen_ppl.withColumn(var, sort_array(var))

# -------------------------------------- #
# ---- COMBINE PERSON & HH VARIABLES --- #
# -------------------------------------- # 

# Join person variables on HH ID
cen = cen.join(cen_ppl, on = 'hh_id_cen', how = 'left')

# ------------------------------ #
# ----- HOUSE / FLAT NUMBER ---- #
# ------------------------------ # 

# Numbers
cen = cen.withColumn('house_no_cen', HF.house_number_udf(col('address_cen')))
cen = cen.withColumn('flat_no_cen',  HF.flat_number_udf(col('address_cen')))

# ------------------------- #
# ---------- SAVE --------- #
# ------------------------- # 

# Array missing values (for empty households)
cen = cen.withColumn('fn_set_cen',  when(cen.fn_set_cen.isNull(),  array(lit('XXXXX'))).otherwise(cen.fn_set_cen))
cen = cen.withColumn('sn_set_cen',  when(cen.sn_set_cen.isNull(),  array(lit('XXXXX'))).otherwise(cen.sn_set_cen))
cen = cen.withColumn('dob_set_cen', when(cen.dob_set_cen.isNull(), array(lit('1800-08-08'))).otherwise(cen.dob_set_cen))
cen = cen.withColumn('age_set_cen', when(cen.age_set_cen.isNull(), array(lit('888'))).otherwise(cen.age_set_cen))

# Replace missing values with NULL
cen = cen.withColumn('typaccom_cen', when(col('typaccom_cen').isin(['-9', '-7', '-3']), lit(None)).otherwise(col('typaccom_cen')))
cen = cen.withColumn('tenure_cen', when(col('tenure_cen').isin(['-9', '-7']), lit(None)).otherwise(col('tenure_cen')))
cen = cen.withColumn('no_resi_cen', when(col('no_resi_cen').isin(['-9', '-5']), lit(None)).otherwise(col('no_resi_cen')))

# Column Types
cen = cen.withColumn('typaccom_cen', cen['typaccom_cen'].cast('int'))
cen = cen.withColumn('tenure_cen',   cen['tenure_cen'].cast('int'))
cen = cen.withColumn('no_resi_cen',  cen['no_resi_cen'].cast('int'))

# Save clean households
cen.write.mode('overwrite').parquet(FILE_PATH('Stage_1_clean_HHs_census'))

sparkSession.stop()