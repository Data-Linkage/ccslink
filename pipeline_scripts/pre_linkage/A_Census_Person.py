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

# Read in CCS person data
cen = sparkSession.read.csv('/data/dap/c21_processing_zone/c21_cmatch_hdfs_h/file/cms/census/{}/{}/{}/raw/census_residents/census_residents.csv'.format(YEAR, MONTH, DAY), header = True)

# Select columns
cen = cen.selectExpr('resident_id as id', 'qid', 'response_id', 'residence_type', 'ce_id', 'household_id as hh_id', 'first_name', 'middle_name', 'last_name',
                     'resident_first_name', 'resident_last_name', 'full_dob', 'resident_age', 'born_in_uk_20 as born_in_uk',
                     'sex', 'marital_status', 'ethnic05_20 as ethnic5', 'ethnic19_20 as ethnic19',
                     'alternative_address_type', 'alternative_address_indicator', 'alternative_address as address_alt',
                     'alternative_address_postcode as postcode_alt', 'alternative_address_uprn as uprn_alt',
                     '1_Year_Ago_Address_Postcode',
                     'in_full_time_education', 'is_hh_term_time_address', 'resident_response_mode_20 as mode').persist()
cen.count()

# --------------------------------------------------------------------------------------------------------------------- #
# ------------------------------------------------------- Names ------------------------------------------------------- #
# --------------------------------------------------------------------------------------------------------------------- #  

# Replace -9 with a blank
cen = cen.withColumn('first_name',  when(cen.first_name ==  '-9',  lit('')).otherwise(cen.first_name))
cen = cen.withColumn('middle_name', when(cen.middle_name == '-9',  lit('')).otherwise(cen.middle_name))
cen = cen.withColumn('last_name',   when(cen.last_name ==   '-9',  lit('')).otherwise(cen.last_name))

# Do the same for the resident FN and SN
cen = cen.withColumn('resident_first_name', when(cen.resident_first_name.isin(['-9', '-4']), lit('')).otherwise(cen.resident_first_name))
cen = cen.withColumn('resident_last_name',  when(cen.resident_last_name.isin(['-9', '-4']),  lit('')).otherwise(cen.resident_last_name))

# Upper case
for var in ['first_name', 'middle_name', 'last_name', 'resident_first_name', 'resident_last_name']:
  cen = cen.withColumn(var, upper(cen[var]))

# Create Forenames column from FN & MN
cen = cen.withColumn('forenames', concat_ws(' ', 'first_name', 'middle_name'))
cen = cen.withColumn('forenames', when(cen.forenames == ' ',  lit('')).otherwise(cen.forenames))

# Forename / Res Forename / Surname / Res Surname variables with no spaces
cen = cen.withColumn('fn',  PF.clean_names_nospace(cen.forenames))
cen = cen.withColumn('sn',  PF.clean_names_nospace(cen.last_name))
cen = cen.withColumn('res_fn', PF.clean_names_nospace(cen.resident_first_name))
cen = cen.withColumn('res_sn', PF.clean_names_nospace(cen.resident_last_name))

# Forename / Res Forename Split 1 & 2
cen = cen.withColumn('fn1',  split(PF.clean_names_AZ(PF.single_space(PF.hyphen_to_space(cen.forenames))), ' ').getItem(0))
cen = cen.withColumn('fn2',  split(PF.clean_names_AZ(PF.single_space(PF.hyphen_to_space(cen.forenames))), ' ').getItem(1))
cen = cen.withColumn('res_fn1', split(PF.clean_names_AZ(PF.single_space(PF.hyphen_to_space(cen.resident_first_name))), ' ').getItem(0))
cen = cen.withColumn('res_fn2', split(PF.clean_names_AZ(PF.single_space(PF.hyphen_to_space(cen.resident_first_name))), ' ').getItem(1))

# Surname / Res Surname Split 1 & 2
cen = cen.withColumn('sn1',  split(PF.clean_names_AZ(PF.single_space(PF.hyphen_to_space(cen.last_name))), ' ').getItem(0))
cen = cen.withColumn('sn2',  split(PF.clean_names_AZ(PF.single_space(PF.hyphen_to_space(cen.last_name))), ' ').getItem(1))
cen = cen.withColumn('res_sn1', split(PF.clean_names_AZ(PF.single_space(PF.hyphen_to_space(cen.resident_last_name))), ' ').getItem(0))
cen = cen.withColumn('res_sn2', split(PF.clean_names_AZ(PF.single_space(PF.hyphen_to_space(cen.resident_last_name))), ' ').getItem(1))

# Full Name with spaces
cen = cen.withColumn('fullname', trim(PF.single_space(concat(PF.clean_names_AZ(PF.hyphen_to_space('forenames')), lit(' '), PF.clean_names_AZ(PF.hyphen_to_space('last_name'))))))

# Full Name NS
cen = cen.withColumn('fullname_ns', PF.clean_names_nospace(cen.fullname))

# Full Name NS sorted alphabetically
cen = cen.withColumn('alphaname', PF.alpha_udf(cen.fullname_ns))

# Nickname lookup
lookup = sparkSession.read.csv('/dap/landing_zone/ons/nickname_lookup/v1/nickname_dictionary_Oct_2019.csv', header = True).selectExpr('in_name as fn1', 'out_name as fn1_nickname')

# Create FN1 nickname column
cen = cen.join(lookup, on = 'fn1', how = 'left')

# Replace all blanks with None
for variable in ['first_name', 'middle_name', 'last_name', 'forenames', 'fn1', 'fn2', 'fn1_nickname', 'sn1', 'sn2', 'fn', 'sn', 'fullname', 'fullname_ns', 'alphaname', 'resident_first_name', 'resident_last_name', 'res_fn', 'res_fn1', 'res_fn2', 'res_sn', 'res_sn1', 'res_sn2']:
  cen = cen.withColumn(variable, when(col(variable) == '', lit(None)).otherwise(col(variable)))

# Check
# cen.select('first_name', 'middle_name', 'last_name', 'forenames', 'fn', 'fn1', 'fn2', 'fn1_nickname', 'sn', 'sn1', 'sn2', 'fullname', 'fullname_ns').show(100, False)

# ------------------------------------------------------------------------------------------------------------------- #
# ------------------------------------------------------- Sex ------------------------------------------------------- #
# ------------------------------------------------------------------------------------------------------------------- #  

# Missing value
cen = cen.withColumn('sex', when(cen.sex.isin(['-9', '-7']), lit(None)).otherwise(cen.sex))

# Numbers only & convert to integer
cen = cen.withColumn('sex', (regexp_replace('sex','[^0-9]','')).cast('int'))
  
# ------------------------------------------------------------------------------------------------------------------- #
# ------------------------------------------------------- DOB ------------------------------------------------------- #
# ------------------------------------------------------------------------------------------------------------------- #  

# Remake DOB (YYYY-MM-DD)
cen = cen.withColumn('dob', concat(substring('full_DOB', 5, 4), lit("-"), substring('full_DOB', 3, 2), lit("-"), substring('full_DOB', 1, 2))).drop('full_DOB')

# Create D/M/Y from new DOB variable
cen = cen.withColumn('year',substring('dob', 1, 4))
cen = cen.withColumn('mon', substring('dob', 6, 2))
cen = cen.withColumn('day', substring('dob', 9, 2))

# Age missingness
cen = cen.withColumn('age', when(cen.resident_age == '-9',  lit('')).otherwise(cen.resident_age)).drop('resident_age')

# Convert Age variable to integer type
cen = cen.withColumn('age', cen['age'].cast('integer'))

# Replace all missing values with None
for variable in ['day', 'mon', 'year', 'dob', 'age']:
  cen = cen.withColumn(variable, when(col(variable).isin(['XXXX-XX-XX', 'XXXX', 'XX', '']), lit(None)).otherwise(col(variable)))

# ------------------------------------------------------------------------------------------------------------------- #
# ---------------------------------- Current Address / Postcode / UPRN / House Number ------------------------------- #
# ------------------------------------------------------------------------------------------------------------------- #  

# Read in geography data from questionnaire table
cen_geo = sparkSession.read.csv('/data/dap/c21_processing_zone/c21_cmatch_hdfs_h/file/cms/census/{}/{}/{}/raw/census_questionnaires/census_questionnaires.csv'.format(YEAR, MONTH, DAY), header = True)
cen_geo = cen_geo.select('response_id', 'display_address', 'address_raw', 'address', 'address_postcode', 'uprn').drop_duplicates()

# Replace -9 & -8 with None
for variable in ['display_address', 'address', 'address_raw', 'address_postcode']:
  cen_geo = cen_geo.withColumn(variable, when(col(variable).isin(['-9', '-8']), lit(None)).otherwise(col(variable)))
  
# Add comma to address_raw (helps with house number function later on)
cen_geo = cen_geo.withColumn('address_raw', concat(col('address_raw'), lit(',')))

# Replace missing address with raw address then display address (equivalent to CMS variable 'address_combined')
cen_geo = cen_geo.withColumn('address', when(col('address').isNull() == True, col('address_raw')).otherwise(col('address'))).drop('address_raw')
cen_geo = cen_geo.withColumn('address', when(col('address').isNull() == True, col('display_address')).otherwise(col('address'))).drop('display_address')

# Clean Postcode
cen_geo = cen_geo.withColumn('pc', upper(regexp_replace(col('address_postcode'), "[^0-9A-Za-z]+", ""))).drop('address_postcode')
cen_geo = cen_geo.withColumn('pc', when(col('pc') == '', lit(None)).otherwise(col('pc')))

# Add sec/dist/area 
cen_geo = cen_geo.withColumn('pc_sect', F.expr("substring({0},1, length({0}) - 2)".format("pc")))\
                 .withColumn('pc_dist', F.expr("""IF(length({0}) = 5, substring({0},1,4), IF(length({0}) = 4, substring({0},1,3), substring({0},1,2)))""".format("pc_sect")))\
                 .withColumn('pc_area', F.expr("""IF(substring({0},2,1) in('1','2','3','4','5','6','7','8','9'), substr({0},1,1), substring({0},1,2))""".format("pc_sect")))

# Create House Number using UDF
cen_geo = cen_geo.withColumn('house_no', HF.house_number_udf(col('address')))

# Postcode minus last character
cen_geo = cen_geo.withColumn('pc_substr', PF.remove_final_char_udf(col('pc')))

# Join variables on via response_id
cen = cen.join(cen_geo, on = 'response_id', how = 'left')

# ------------------------------------------------------------------------------------------------------------------- #
# -------------------------------- Alternative Address / Postcode / UPRN / House Number ----------------------------- #
# ------------------------------------------------------------------------------------------------------------------- #  

# Replace -9 with None
for variable in ['address_alt', 'postcode_alt', 'uprn_alt', '1_Year_Ago_Address_Postcode']:
  cen = cen.withColumn(variable, when(col(variable).isin(['-9', '-8']), lit(None)).otherwise(col(variable)))

# Clean Alt. Postcode
cen = cen.withColumn('pc_alt', upper(regexp_replace(col('postcode_alt'), "[^0-9A-Za-z]+", ""))).drop('postcode_alt')
cen = cen.withColumn('pc_alt', when(col('pc_alt') == '', lit(None)).otherwise(col('pc_alt')))

# Create House Number using UDF
cen = cen.withColumn('house_no_alt', HF.house_number_udf(col('address_alt')))

# Clean Postcode 1 Year Ago
cen = cen.withColumn('pc_1Y', upper(regexp_replace(col('1_Year_Ago_Address_Postcode'), "[^0-9A-Za-z]+", ""))).drop('1_Year_Ago_Address_Postcode')
cen = cen.withColumn('pc_1Y', when(col('pc_1Y') == '', lit(None)).otherwise(col('pc_1Y')))

# ------------------------------------------------------------------------------------------------------------------- #
# ------------------------------------------------- Other Variables ------------------------------------------------- #
# ------------------------------------------------------------------------------------------------------------------- #  

# Replace missingness with a blank
cen = cen.withColumn('marital_status',          when((cen.marital_status.contains('-')),          lit('')).otherwise(col('marital_status')))
cen = cen.withColumn('ethnic5',                 when((cen.ethnic5.contains('-')),                 lit('')).otherwise(col('ethnic5')))
cen = cen.withColumn('ethnic19',                when((cen.ethnic19.contains('-')),                lit('')).otherwise(col('ethnic19')))
cen = cen.withColumn('in_full_time_education',  when((cen.in_full_time_education.contains('-')),  lit('')).otherwise(col('in_full_time_education')))
cen = cen.withColumn('is_hh_term_time_address', when((cen.is_hh_term_time_address.contains('-')), lit('')).otherwise(col('is_hh_term_time_address')))

# Remove non numerics & cast to integer
cen = cen.withColumn('marital_status',          regexp_replace('marital_status',         '[^0-9]', '').cast('integer'))
cen = cen.withColumn('ethnic5',                 regexp_replace('ethnic5',                '[^0-9]', '').cast('integer'))
cen = cen.withColumn('ethnic19',                regexp_replace('ethnic19',               '[^0-9]', '').cast('integer'))
cen = cen.withColumn('in_full_time_education',  regexp_replace('in_full_time_education', '[^0-9]', '').cast('integer'))
cen = cen.withColumn('is_hh_term_time_address', regexp_replace('is_hh_term_time_address','[^0-9]', '').cast('integer'))    

# Student_Duplicate == 1 means student is NOT at their term time address
cen = cen.withColumn('Student_Duplicate', when(((((col('is_hh_term_time_address') == 2) | (col('is_hh_term_time_address') == 3))) & (col('in_full_time_education') == 1)), lit(1)).otherwise(lit(0)))

# Save students (in_full_time_education == 1) NOT at their term time address (is_hh_term_time_address == 2 OR 3) in separate pot and then remove from cleaned CEN
cen_students_NTTA = cen.filter(col('Student_Duplicate') == 1).selectExpr('id as id_cen', 'hh_id as hh_id_cen', 'Student_Duplicate', 'pc as pc_cen', 'dob as dob_cen')
cen = cen.filter(col('Student_Duplicate') == 0)

# Read in extra RMR records that need removing 
cen_RMR = sparkSession.read.parquet(FILE_PATH('Stage_1_RMR_cases_to_remove_census')).selectExpr('id_cen as id')

# Join on all cleaned variables ready to save separately
cen_RMR = cen_RMR.join(cen, on = 'id', how = 'inner').selectExpr('id as id_cen', 'hh_id as hh_id_cen', 'pc as pc_cen', 'dob as dob_cen')

# Remove extra RMR cases with missing names from final cleaned dataset
cen = cen.join(cen_RMR, on = [cen.id == cen_RMR.id_cen], how = 'left_anti')

# ------------------------------------------------------------------------------------------------------------------- #
# ------------------------------------------------------ Save  ------------------------------------------------------ #
# ------------------------------------------------------------------------------------------------------------------- #    

# Select columns
cen2 = cen.selectExpr('id',
                      'hh_id',
                      'qid',
                      'response_id',
                      'ce_id',

                      'forenames',
                      'last_name',               
                      'fn',
                      'sn',
                      'fn1',
                      'fn2',
                      'sn1',
                      'sn2',
                      'fn1_nickname',
                      'fullname',
                      'fullname_ns',
                      'alphaname',
                      
                      'res_fn',
                      'res_fn1',
                      'res_fn2',
                      'res_sn',
                      'res_sn1',
                      'res_sn2',
 
                      'day',
                      'mon',
                      'year',
                      'dob',
                      'age',

                      'address',
                      'pc',
                      'pc_sect',
                      'pc_dist',
                      'pc_area',
                      'uprn',
                      'house_no',
                      'pc_substr',
                      
                      'address_alt',
                      'pc_alt',
                      'uprn_alt',
                      'house_no_alt',    
                      
                      'pc_1Y',
                      
                      'sex',
                      'marital_status',
                      'ethnic5',
                      'ethnic19',
                      'mode')

# Column Names
for col in cen2.columns:
  cen2 = cen2.withColumnRenamed(col, col + "_cen")
          
# Save
cen2.write.mode("overwrite").parquet(FILE_PATH('Stage_1_clean_census'))
cen_students_NTTA.write.mode("overwrite").parquet(FILE_PATH('Stage_1_clean_cen_students_TTA'))
cen_RMR.write.mode("overwrite").parquet(FILE_PATH('Stage_1_clean_cen_RMR_removed'))

sparkSession.stop()