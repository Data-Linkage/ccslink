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

# Read in CCS HH data
ccs = sparkSession.read.csv('some_path' + 'ccs_households/ccs_households.csv'.format(YEAR, MONTH, DAY), header = True)

# Select columns
ccs = ccs.selectExpr('qid as qid_ccs', 'household_id as hh_id_ccs', 'ownership_type as tenure_ccs', 'accommodation_type as typaccom_ccs', 'resident_count as no_resi_ccs',
                     'census_address_indicator', 'census_address as address_cenday_ccs', 'census_address_postcode as pc_cenday_ccs', 'census_address_uprn as uprn_cenday_ccs',
                     'census_address_country as country_cenday_ccs').persist()
ccs.count()

# ---------------------------------------------------------------------------------- #
# ---------------- Current Address / Postcode / UPRN / House Number ---------------- #
# ---------------------------------------------------------------------------------- #  

# CCS Questionnaire data
ccs_q = sparkSession.read.csv('some_path' + 'ccs_questionnaires/ccs_questionnaires.csv'.format(YEAR, MONTH, DAY), header = True)
ccs_q = ccs_q.selectExpr('qid as qid_ccs', 'display_address', 'address', 'address_postcode as pc_ccs', 'uprn as uprn_ccs').drop_duplicates()

# Replace -9 & -8 with None
for variable in ['display_address', 'address', 'pc_ccs']:
  ccs_q = ccs_q.withColumn(variable, when(col(variable).isin(['-9', '-8']), lit(None)).otherwise(col(variable)))
  
# Add comma to display_address (helps with house number function later on)
ccs_q = ccs_q.withColumn('display_address', concat(col('display_address'), lit(',')))

# Replace missing address with display address (equivalent to CMS variable 'address_combined')
ccs_q = ccs_q.withColumn('address_ccs', when(col('address').isNull() == True, col('display_address')).otherwise(col('address'))).drop('display_address')

# Clean Postcode
ccs_q = ccs_q.withColumn('pc_ccs', upper(regexp_replace(col('pc_ccs'), "[^0-9A-Za-z]+", "")))
ccs_q = ccs_q.withColumn('pc_ccs', when(col('pc_ccs') == '', None).otherwise(col('pc_ccs')))

# Join variables on via qid
ccs = ccs.join(ccs_q.dropDuplicates(['qid_ccs']), on = 'qid_ccs', how = 'left')

# House/Flat Number
ccs = ccs.withColumn('house_no_ccs', HF.house_number_udf(col('address_ccs')))
ccs = ccs.withColumn('flat_no_ccs',  HF.flat_number_udf(col('address_ccs')))

# ----------------------------------------------------------------------------------------------- #
# ---------------- Census Day Address / Postcode / UPRN / House Number / Country ---------------- #
# ----------------------------------------------------------------------------------------------- #  

# Indicator update: If census day postcode exists (and census day address is not -8), set to 1, otherwise set to 0
ccs = ccs.withColumn('census_address_indicator', when(col('pc_cenday_ccs').isin(['-9', '-8']), lit(0)).otherwise(lit(1)))
ccs = ccs.withColumn('census_address_indicator', when(col('address_cenday_ccs') == '-8', lit(0)).otherwise(col('census_address_indicator')))

# Replace -9 & -8 with None
for variable in ['census_address_indicator', 'address_cenday_ccs', 'pc_cenday_ccs', 'uprn_cenday_ccs', 'country_cenday_ccs']:
  ccs = ccs.withColumn(variable, when(col(variable).isin(['-9', '-8']), lit(None)).otherwise(col(variable)))

# Clean Census Day Postcode
ccs = ccs.withColumn('pc_cenday_ccs', upper(regexp_replace(col('pc_cenday_ccs'), "[^0-9A-Za-z]+", "")))
ccs = ccs.withColumn('pc_cenday_ccs', when(col('pc_cenday_ccs') == '', None).otherwise(col('pc_cenday_ccs')))

# Create House/Flat Number using UDF
ccs = ccs.withColumn('house_no_cenday_ccs', HF.house_number_udf(col('address_cenday_ccs')))
ccs = ccs.withColumn('flat_no_cenday_ccs',  HF.flat_number_udf(col('address_cenday_ccs')))

# Update mover indicator to 0 if pc_cenday = pc (ccsday)
ccs = ccs.withColumn('census_address_indicator', when(col('pc_cenday_ccs') == col('pc_ccs'), lit(0)).otherwise(col('census_address_indicator')))

# -------------------------------------------------------------------------- #
# ----------------- Update Geographic Variables for Movers ----------------- #
# -------------------------------------------------------------------------- #

# Firstly, save geographic variables on CCS day in new columns
ccs = ccs.withColumn('pc_ccsday_ccs',       col('pc_ccs'))
ccs = ccs.withColumn('uprn_ccsday_ccs',     col('uprn_ccs'))
ccs = ccs.withColumn('house_no_ccsday_ccs', col('house_no_ccs'))
ccs = ccs.withColumn('flat_no_ccsday_ccs',  col('flat_no_ccs'))
ccs = ccs.withColumn('address_ccsday_ccs',  col('address_ccs'))

# Next, if CCS person is a mover, update their main geographic columns with census_day variables
ccs = ccs.withColumn('pc_ccs',       when(col('census_address_indicator') == 1, col('pc_cenday_ccs')).otherwise(col('pc_ccs')))
ccs = ccs.withColumn('uprn_ccs',     when(col('census_address_indicator') == 1, col('uprn_cenday_ccs')).otherwise(col('uprn_ccs')))
ccs = ccs.withColumn('house_no_ccs', when(col('census_address_indicator') == 1, col('house_no_cenday_ccs')).otherwise(col('house_no_ccs')))
ccs = ccs.withColumn('flat_no_ccs',  when(col('census_address_indicator') == 1, col('flat_no_cenday_ccs')).otherwise(col('flat_no_ccs')))
ccs = ccs.withColumn('address_ccs',  when(col('census_address_indicator') == 1, col('address_cenday_ccs')).otherwise(col('address_ccs')))

# If HH has moved since Census Day, set variables relating to current address to NULL
ccs = ccs.withColumn('typaccom_ccs', when(ccs.census_address_indicator == 1, lit(None)).otherwise(ccs.typaccom_ccs))
ccs = ccs.withColumn('tenure_ccs',   when(ccs.census_address_indicator == 1, lit(None)).otherwise(ccs.tenure_ccs))
  
# Create sect/dist/area from primary postcode
ccs = ccs.withColumn('pc_sect_ccs', F.expr("substring({0},1, length({0}) - 2)".format("pc_ccs")))\
             .withColumn('pc_dist_ccs', F.expr("""IF(length({0}) = 5, substring({0},1,4), IF(length({0}) =4, substring({0},1,3), substring({0},1,2)))""".format("pc_sect_ccs")))\
             .withColumn('pc_area_ccs', F.expr("""IF(substring({0},2,1) in('1','2','3','4','5','6','7','8','9'), substr({0},1,1), substring({0},1,2))""".format("pc_sect_ccs")))

# --------------------------------- #
# -------- PERSON VARIABLES ------- #
# --------------------------------- #  

# Read in CCS People
ccs_ppl = sparkSession.read.parquet(FILE_PATH('Stage_1_clean_ccs')).selectExpr('id_ccs', 'hh_id_ccs', 'fn1_ccs as FN', 'sn1_ccs as SN', 'dob_ccs as DOB', 'age_ccs as AGE').persist()
ccs_ppl.count()

# Remove records with no household ID
ccs_ppl = ccs_ppl.filter(col('hh_id_ccs') != '-9')

# ----------------------------- #
# -------- MISSINGNESS -------- #
# ----------------------------- #   

# FN / SN
ccs_ppl = ccs_ppl.withColumn('FN', when(ccs_ppl.FN.isNull(), 'YYYYY').otherwise(ccs_ppl.FN))
ccs_ppl = ccs_ppl.withColumn('SN', when(ccs_ppl.SN.isNull(), 'YYYYY').otherwise(ccs_ppl.SN))

# DOB
ccs_ppl = ccs_ppl.withColumn('DOB', when(ccs_ppl.DOB.isNull(), '1700-07-07').otherwise(ccs_ppl.DOB))
ccs_ppl = ccs_ppl.withColumn('DOB', to_date('DOB', 'yyyy-MM-dd'))

# AGE
ccs_ppl = ccs_ppl.withColumn('AGE', when(ccs_ppl.AGE.isNull(), '777').otherwise(ccs_ppl.AGE))

# ------------------------- #
# -------- HH SIZE -------- #
# ------------------------- #  

# Count number of IDs for each HH - different to number of usual residents
ccs_ppl = ccs_ppl.withColumn('hh_size_ccs', size(collect_set('id_ccs').over(Window.partitionBy('hh_id_ccs')))).drop('id_ccs')

# ------------------------------ #
# --------- PERSON SETS -------- #
# ------------------------------ #  

# Create 4 columns which contains list of all unique forenames / surnames / dobs / ages from that household
ccs_ppl = ccs_ppl.withColumn('fn_set_ccs',   collect_set('FN').over(Window.partitionBy('hh_id_ccs')))\
                 .withColumn('sn_set_ccs',   collect_set('SN').over(Window.partitionBy('hh_id_ccs')))\
                 .withColumn('dob_set_ccs',  collect_set('DOB').over(Window.partitionBy('hh_id_ccs')))\
                 .withColumn('age_set_ccs',  collect_set('AGE').over(Window.partitionBy('hh_id_ccs')))\
                 .drop('FN', 'SN', 'DOB', 'AGE')\
                 .drop_duplicates(['hh_id_ccs'])\

# Array missing values
ccs_ppl = ccs_ppl.withColumn('fn_set_ccs',  when(ccs_ppl.fn_set_ccs.isNull(),  array(lit('YYYYY'))).otherwise(ccs_ppl.fn_set_ccs))
ccs_ppl = ccs_ppl.withColumn('sn_set_ccs',  when(ccs_ppl.sn_set_ccs.isNull(),  array(lit('YYYYY'))).otherwise(ccs_ppl.sn_set_ccs))
ccs_ppl = ccs_ppl.withColumn('dob_set_ccs', when(ccs_ppl.dob_set_ccs.isNull(), array(lit('1700-07-07'))).otherwise(ccs_ppl.dob_set_ccs))
ccs_ppl = ccs_ppl.withColumn('age_set_ccs', when(ccs_ppl.age_set_ccs.isNull(), array(lit('777'))).otherwise(ccs_ppl.age_set_ccs))

# Sort arrays
for var in ['fn_set_ccs', 'sn_set_ccs', 'dob_set_ccs', 'age_set_ccs']:
  ccs_ppl = ccs_ppl.withColumn(var, sort_array(var))
  
# -------------------------------------- #
# ---- COMBINE PERSON & HH VARIABLES --- #
# -------------------------------------- # 

# Join person variables on HH ID
ccs = ccs.join(ccs_ppl, on = 'hh_id_ccs', how = 'left')

# ------------------------- #
# ---------- SAVE --------- #
# ------------------------- # 

# Ensure all None array values have a missing value - this enables HH functions to run
ccs = ccs.withColumn('fn_set_ccs',  when(ccs.fn_set_ccs.isNull(),  array(lit('YYYYY'))).otherwise(ccs.fn_set_ccs))
ccs = ccs.withColumn('sn_set_ccs',  when(ccs.sn_set_ccs.isNull(),  array(lit('YYYYY'))).otherwise(ccs.sn_set_ccs))
ccs = ccs.withColumn('dob_set_ccs', when(ccs.dob_set_ccs.isNull(), array(lit('1700-07-07'))).otherwise(ccs.dob_set_ccs))
ccs = ccs.withColumn('age_set_ccs', when(ccs.age_set_ccs.isNull(), array(lit('777'))).otherwise(ccs.age_set_ccs))

# Replace missing values with NULL
ccs = ccs.withColumn('typaccom_ccs', when(col('typaccom_ccs').isin(['-9', '-8', '-7']), lit(None)).otherwise(col('typaccom_ccs')))
ccs = ccs.withColumn('tenure_ccs', when(col('tenure_ccs').isin(['-9', '-8', '-7']), lit(None)).otherwise(col('tenure_ccs')))
ccs = ccs.withColumn('no_resi_ccs', when(col('no_resi_ccs').isin(['-9', '-5', '-4']), lit(None)).otherwise(col('no_resi_ccs')))

# Column Types
ccs = ccs.withColumn('typaccom_ccs', ccs['typaccom_ccs'].cast('int'))
ccs = ccs.withColumn('tenure_ccs',   ccs['tenure_ccs'].cast('int'))
ccs = ccs.withColumn('no_resi_ccs',  ccs['no_resi_ccs'].cast('int'))

# Save clean households
ccs.write.mode('overwrite').parquet(FILE_PATH('Stage_1_clean_HHs_ccs'))

sparkSession.stop()
