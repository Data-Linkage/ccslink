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

#set up sparksession
from CCSLink import Spark_Session as SS
sparkSession = SS.SPARK()

# Changes to SparkSession 
sparkSession.conf.set('spark.sql.codegen.wholeStage', 'false')

# ----------------------- #
# -------- DATA --------- #
# ----------------------- #              

# Set year, month, date for file path
YEAR, MONTH, DAY =  '2021', '11', '16'

# Read in CCS person data
ccs = sparkSession.read.csv('/data/dap/c21_processing_zone/c21_cmatch_hdfs_h/file/cms/census/{}/{}/{}/raw/ccs_residents/ccs_residents.csv'.format(YEAR, MONTH, DAY), header = True)

# Select columns
ccs = ccs.selectExpr('resident_id as id', 'qid', 'residence_type', 'ce_id', 'household_id as hh_id', 'first_name', 'middle_name', 'last_name',
                     'resident_first_name', 'resident_last_name', 'full_dob_ccs as full_dob', 'resident_age', 'resident_age_last_birthday',
                     'estimate_age', 'born_in_uk', 'sex', 'marital_status', 'ethnic05_20 as ethnic5', 'ethnic19_20 as ethnic19',
                     'alternative_address_type', 'alternative_address_indicator', 'alternative_address as address_alt',
                     'alternative_address_postcode as postcode_alt', 'alternative_address_uprn as uprn_alt',
                     'in_full_time_education', 'is_hh_term_time_address', 'resident_response_mode_20 as mode').persist()
ccs.count()

# --------------------------------------------------------------------------------------------------------------------- #
# ------------------------------------------------------- Names ------------------------------------------------------- #
# --------------------------------------------------------------------------------------------------------------------- #  

# Replace -9 with a blank
ccs = ccs.withColumn('first_name',  when(ccs.first_name ==  '-9',  lit('')).otherwise(ccs.first_name))
ccs = ccs.withColumn('middle_name', when(ccs.middle_name == '-9',  lit('')).otherwise(ccs.middle_name))
ccs = ccs.withColumn('last_name',   when(ccs.last_name ==   '-9',  lit('')).otherwise(ccs.last_name))

# Do the same for the resident FN and SN
ccs = ccs.withColumn('resident_first_name', when(ccs.resident_first_name.isin(['-9', '-4']), lit('')).otherwise(ccs.resident_first_name))
ccs = ccs.withColumn('resident_last_name',  when(ccs.resident_last_name.isin(['-9', '-4']),  lit('')).otherwise(ccs.resident_last_name))

# Upper case
for var in ['first_name', 'middle_name', 'last_name', 'resident_first_name', 'resident_last_name']:
  ccs = ccs.withColumn(var, upper(ccs[var]))

# Create Forenames column from FN & MN
ccs = ccs.withColumn('forenames', concat_ws(' ', 'first_name', 'middle_name'))
ccs = ccs.withColumn('forenames', when(ccs.forenames == ' ',  lit('')).otherwise(ccs.forenames))

# Forename / Res Forename / Surname / Res Surname variables with no spaces
ccs = ccs.withColumn('fn',  PF.clean_names_nospace(ccs.forenames))
ccs = ccs.withColumn('sn',  PF.clean_names_nospace(ccs.last_name))
ccs = ccs.withColumn('res_fn', PF.clean_names_nospace(ccs.resident_first_name))
ccs = ccs.withColumn('res_sn', PF.clean_names_nospace(ccs.resident_last_name))

# Forename / Res Forename Split 1 & 2
ccs = ccs.withColumn('fn1',  split(PF.clean_names_AZ(PF.single_space(PF.hyphen_to_space(ccs.forenames))), ' ').getItem(0))
ccs = ccs.withColumn('fn2',  split(PF.clean_names_AZ(PF.single_space(PF.hyphen_to_space(ccs.forenames))), ' ').getItem(1))
ccs = ccs.withColumn('res_fn1', split(PF.clean_names_AZ(PF.single_space(PF.hyphen_to_space(ccs.resident_first_name))), ' ').getItem(0))
ccs = ccs.withColumn('res_fn2', split(PF.clean_names_AZ(PF.single_space(PF.hyphen_to_space(ccs.resident_first_name))), ' ').getItem(1))

# Surname / Res Surname Split 1 & 2
ccs = ccs.withColumn('sn1',  split(PF.clean_names_AZ(PF.single_space(PF.hyphen_to_space(ccs.last_name))), ' ').getItem(0))
ccs = ccs.withColumn('sn2',  split(PF.clean_names_AZ(PF.single_space(PF.hyphen_to_space(ccs.last_name))), ' ').getItem(1))
ccs = ccs.withColumn('res_sn1', split(PF.clean_names_AZ(PF.single_space(PF.hyphen_to_space(ccs.resident_last_name))), ' ').getItem(0))
ccs = ccs.withColumn('res_sn2', split(PF.clean_names_AZ(PF.single_space(PF.hyphen_to_space(ccs.resident_last_name))), ' ').getItem(1))

# Full Name with spaces
ccs = ccs.withColumn('fullname', trim(PF.single_space(concat(PF.clean_names_AZ(PF.hyphen_to_space('forenames')), lit(' '), PF.clean_names_AZ(PF.hyphen_to_space('last_name'))))))

# Full Name NS
ccs = ccs.withColumn('fullname_ns', PF.clean_names_nospace(ccs.fullname))

# Full Name NS sorted alphabetically
ccs = ccs.withColumn('alphaname', PF.alpha_udf(ccs.fullname_ns))

# Nickname lookup
lookup = sparkSession.read.csv('/dap/landing_zone/ons/nickname_lookup/v1/nickname_dictionary_Oct_2019.csv', header = True).selectExpr('in_name as fn1', 'out_name as fn1_nickname')

# Create FN1 nickname column
ccs = ccs.join(lookup, on = 'fn1', how = 'left')

# Replace all blanks with None
for variable in ['first_name', 'middle_name', 'last_name', 'forenames', 'fn1', 'fn2', 'fn1_nickname', 'sn1', 'sn2', 'fn', 'sn', 'fullname', 'fullname_ns', 'alphaname', 'resident_first_name', 'resident_last_name', 'res_fn', 'res_fn1', 'res_fn2', 'res_sn', 'res_sn1', 'res_sn2']:
  ccs = ccs.withColumn(variable, when(col(variable) == '', lit(None)).otherwise(col(variable)))
    
# Check
# ccs.select('first_name', 'middle_name', 'last_name', 'forenames', 'fn', 'fn1', 'fn2', 'fn1_nickname', 'sn', 'sn1', 'sn2', 'fullname', 'fullname_ns').show(100, False)

# ------------------------------------------------------------------------------------------------------------------- #
# ------------------------------------------------------- Sex ------------------------------------------------------- #
# ------------------------------------------------------------------------------------------------------------------- #  

# Missing value
ccs = ccs.withColumn('sex', when(ccs.sex.isin(['-9', '-7']), lit(None)).otherwise(ccs.sex))

# Numbers only & convert to integer
ccs = ccs.withColumn('sex', (regexp_replace('sex','[^0-9]','')).cast('int'))
  
# ------------------------------------------------------------------------------------------------------------------- #
# ------------------------------------------------------- DOB ------------------------------------------------------- #
# ------------------------------------------------------------------------------------------------------------------- #  

# Replace X with Y for full_DOB
ccs = ccs.withColumn('full_DOB', regexp_replace('full_DOB', 'X', 'Y'))

# Remake DOB (YYYY-MM-DD)
ccs = ccs.withColumn('dob', concat(substring('full_DOB', 5, 4), lit("-"), substring('full_DOB', 3, 2), lit("-"), substring('full_DOB', 1, 2))).drop('full_DOB')

# Create D/M/Y from new DOB variable
ccs = ccs.withColumn('year',substring('dob', 1, 4))
ccs = ccs.withColumn('mon', substring('dob', 6, 2))
ccs = ccs.withColumn('day', substring('dob', 9, 2))

# Age missingness
ccs = ccs.withColumn('resident_age',                when((ccs.resident_age == '-9'),  lit('')).otherwise(ccs.resident_age))
ccs = ccs.withColumn('resident_age_last_birthday',  when(ccs.resident_age_last_birthday.isin(['-9', '-8', '-5', '-4']), lit('')).otherwise(ccs.resident_age_last_birthday))

# Use age last birthday if age missing
ccs = ccs.withColumn('age', when(col('resident_age') != '', col('resident_age')).otherwise(col('resident_age_last_birthday'))).drop('resident_age_last_birthday', 'resident_age')

# Convert Age variable to integer type
ccs = ccs.withColumn('age', ccs['age'].cast('integer'))

# Estimated age indicator
ccs = ccs.withColumn('estimate_age', when(col('estimate_age').isin(['-8', '-4']), lit(None)).otherwise(col('estimate_age')))

# Replace all missing values with None
for variable in ['day', 'mon', 'year', 'dob', 'age']:
  ccs = ccs.withColumn(variable, when(col(variable).isin(['YYYY-YY-YY', 'YYYY', 'YY', '']), lit(None)).otherwise(col(variable)))

# ------------------------------------------------------------------------------------------------------------------- #
# ---------------------------------- Current Address / Postcode / UPRN / House Number ------------------------------- #
# ------------------------------------------------------------------------------------------------------------------- #  

# Read in geography data from questionnaire table
ccs_geo = sparkSession.read.csv('/data/dap/c21_processing_zone/c21_cmatch_hdfs_h/file/cms/census/{}/{}/{}/raw/ccs_questionnaires/ccs_questionnaires.csv'.format(YEAR, MONTH, DAY), header = True)
ccs_geo = ccs_geo.select('qid', 'display_address', 'address', 'address_postcode', 'uprn').drop_duplicates()

# Replace -9 & -8 with None
for variable in ['display_address', 'address', 'address_postcode']:
  ccs_geo = ccs_geo.withColumn(variable, when(col(variable).isin(['-9', '-8']), lit(None)).otherwise(col(variable)))
  
# Add comma to display_address (helps with house number function later on)
ccs_geo = ccs_geo.withColumn('display_address', concat(col('display_address'), lit(',')))

# Replace missing address with display address (equivalent to CMS variable 'address_combined')
ccs_geo = ccs_geo.withColumn('address', when(col('address').isNull() == True, col('display_address')).otherwise(col('address'))).drop('display_address')

# Clean Postcode
ccs_geo = ccs_geo.withColumn('pc', upper(regexp_replace(col('address_postcode'), "[^0-9A-Za-z]+", ""))).drop('address_postcode')
ccs_geo = ccs_geo.withColumn('pc', when(col('pc') == '', lit(None)).otherwise(col('pc')))

# Create House Number using UDF
ccs_geo = ccs_geo.withColumn('house_no', HF.house_number_udf(col('address')))

# Join variables on via qid
ccs = ccs.join(ccs_geo.dropDuplicates(['qid']), on = 'qid', how = 'left')

# ------------------------------------------------------------------------------------------------------------------- #
# -------------------------------- Census Day Address / Postcode / UPRN / House Number ------------------------------ #
# ------------------------------------------------------------------------------------------------------------------- #  

# Read in raw household data to identify movers
ccs_move = sparkSession.read.csv('/data/dap/c21_processing_zone/c21_cmatch_hdfs_h/file/cms/census/{}/{}/{}/raw/ccs_households/ccs_households.csv'.format(YEAR, MONTH, DAY), header = True).drop_duplicates()
ccs_move = ccs_move.selectExpr('household_id as hh_id',
                               'census_address_indicator',
                               'census_address as address_cenday', 
                               'census_address_postcode as pc_cenday', 
                               'census_address_uprn as uprn_cenday')

# Indicator update: If census day postcode exists (and census day address is not -8), set to 1, otherwise set to 0
ccs_move = ccs_move.withColumn('census_address_indicator', when(col('pc_cenday').isin(['-9', '-8']), lit(0)).otherwise(lit(1)))
ccs_move = ccs_move.withColumn('census_address_indicator', when(col('address_cenday') == '-8', lit(0)).otherwise(col('census_address_indicator')))

# Take mover households only: No one in household was at current address on census day and their census day address was in the UK
ccs_move = ccs_move.filter(col('census_address_indicator') == 1)

# Replace -9 with None
for variable in ['census_address_indicator', 'address_cenday', 'pc_cenday', 'uprn_cenday']:
  ccs_move = ccs_move.withColumn(variable, when(col(variable).isin(['-9', '-8']), lit(None)).otherwise(col(variable)))

# Clean Census Day Postcode
ccs_move = ccs_move.withColumn('pc_cenday', regexp_replace('pc_cenday', ' ', ''))

# Create House Number using UDF
ccs_move = ccs_move.withColumn('house_no_cenday', HF.house_number_udf(col('address_cenday')))

# Join
ccs = ccs.join(ccs_move, on = 'hh_id', how = 'left')

# Update mover indicator to 0 if pc_cenday = pc (ccsday)
ccs = ccs.withColumn('census_address_indicator', when(col('pc_cenday') == col('pc'), lit(None)).otherwise(col('census_address_indicator')))

# ------------------------------------------------------------------------------------------------------------------- #
# -------------------------------------- Update Geographic Variables for Movers ------------------------------------- #
# ------------------------------------------------------------------------------------------------------------------- #

# Firstly, save geographic variables on CCS day in new columns
ccs = ccs.withColumn('pc_ccsday',       col('pc'))
ccs = ccs.withColumn('uprn_ccsday',     col('uprn'))
ccs = ccs.withColumn('house_no_ccsday', col('house_no'))
ccs = ccs.withColumn('address_ccsday',  col('address'))

# Next, if CCS person is a mover, update their primary geographic variables with their census_day variables
ccs = ccs.withColumn('pc',       when(col('census_address_indicator') == 1, col('pc_cenday')).otherwise(col('pc')))
ccs = ccs.withColumn('uprn',     when(col('census_address_indicator') == 1, col('uprn_cenday')).otherwise(col('uprn')))
ccs = ccs.withColumn('house_no', when(col('census_address_indicator') == 1, col('house_no_cenday')).otherwise(col('house_no')))
ccs = ccs.withColumn('address',  when(col('census_address_indicator') == 1, col('address_cenday')).otherwise(col('address')))

# Create primary sect/dist/area for primary postcode
ccs = ccs.withColumn('pc_sect', F.expr("substring({0},1, length({0}) - 2)".format("pc")))\
         .withColumn('pc_dist', F.expr("""IF(length({0}) = 5, substring({0},1,4), IF(length({0}) =4, substring({0},1,3), substring({0},1,2)))""".format("pc_sect")))\
         .withColumn('pc_area', F.expr("""IF(substring({0},2,1) in('1','2','3','4','5','6','7','8','9'), substr({0},1,1), substring({0},1,2))""".format("pc_sect")))

# Postcode minus last character
ccs = ccs.withColumn('pc_substr', PF.remove_final_char_udf(col('pc')))

# ------------------------------------------------------------------------------------------------------------------- #
# -------------------------------- Alternative Address / Postcode / UPRN / House Number ----------------------------- #
# ------------------------------------------------------------------------------------------------------------------- #  

# Replace -9 with None
for variable in ['address_alt', 'postcode_alt', 'uprn_alt']:
  ccs = ccs.withColumn(variable, when(col(variable).isin(['-9', '-8']), lit(None)).otherwise(col(variable)))

# Clean Postcode
ccs = ccs.withColumn('pc_alt', upper(regexp_replace(col('postcode_alt'), "[^0-9A-Za-z]+", ""))).drop('postcode_alt')
ccs = ccs.withColumn('pc_alt', when(col('pc_alt') == '', lit(None)).otherwise(col('pc_alt')))

# Create House Number using UDF
ccs = ccs.withColumn('house_no_alt', HF.house_number_udf(col('address_alt')))

# ------------------------------------------------------------------------------------------------------------------- #
# ----------------------------------------------- LA + Region + HTC ------------------------------------------------- #
# ------------------------------------------------------------------------------------------------------------------- # 

# LA + Region lookup
LA_lookup = sparkSession.sql("SELECT pcd as pc_ccsday, laua as LA, rgn as Region, lsoa11 as LSOA FROM national_statistics_postcode_lookup.nspl_nov_2020_uk_std")

# Clean PC
LA_lookup = LA_lookup.withColumn('pc_ccsday', regexp_replace('pc_ccsday', ' ', ''))

# Join on variables
ccs = ccs.join(LA_lookup, on = 'pc_ccsday', how = 'left')

# Read in willingness index 
HTC = pd.read_csv('/home/cdsw/collaborative_method_matching/pipeline_scripts/A_Pre_Linkage/04_CLEANING/G_HTC_Index.csv')
HTC = sparkSession.createDataFrame(HTC).selectExpr('lsoa_code as LSOA', 'HtC_willingness as HTCW')

# Join on index via LSOA
ccs = ccs.join(HTC, on = 'LSOA', how = 'left').drop('LSOA')

# ------------------------------------------------------------------------------------------------------------------- #
# ------------------------------------------------- Other Variables ------------------------------------------------- #
# ------------------------------------------------------------------------------------------------------------------- #  

# Replace missingness with a blank
ccs = ccs.withColumn('marital_status',          when((ccs.marital_status.contains('-')),          lit('')).otherwise(col('marital_status')))
ccs = ccs.withColumn('ethnic5',                 when((ccs.ethnic5.contains('-')),                 lit('')).otherwise(col('ethnic5')))
ccs = ccs.withColumn('ethnic19',                when((ccs.ethnic19.contains('-')),                lit('')).otherwise(col('ethnic19')))
ccs = ccs.withColumn('in_full_time_education',  when((ccs.in_full_time_education.contains('-')),  lit('')).otherwise(col('in_full_time_education')))
ccs = ccs.withColumn('is_hh_term_time_address', when((ccs.is_hh_term_time_address.contains('-')), lit('')).otherwise(col('is_hh_term_time_address')))

# Remove non numerics & cast to integer
ccs = ccs.withColumn('marital_status',          regexp_replace('marital_status',         '[^0-9]', '').cast('integer'))
ccs = ccs.withColumn('ethnic5',                 regexp_replace('ethnic5',                '[^0-9]', '').cast('integer'))
ccs = ccs.withColumn('ethnic19',                regexp_replace('ethnic19',               '[^0-9]', '').cast('integer'))
ccs = ccs.withColumn('in_full_time_education',  regexp_replace('in_full_time_education', '[^0-9]', '').cast('integer'))
ccs = ccs.withColumn('is_hh_term_time_address', regexp_replace('is_hh_term_time_address','[^0-9]', '').cast('integer'))

# Student_Duplicate == 1 means student is NOT at their term time address
ccs = ccs.withColumn('Student_Duplicate', when(((col('is_hh_term_time_address') == 2) & (col('in_full_time_education') == 1)), lit(1)).otherwise(lit(0)))

# Save students (in_full_time_education == 1) NOT at their term time address (is_hh_term_time_address == 2) in separate pot and then remove from cleaned CCS
ccs_students_NTTA = ccs.filter(col('Student_Duplicate') == 1).selectExpr('id as id_ccs', 'hh_id as hh_id_ccs', 'Student_Duplicate', 'pc_ccsday as pc_ccsday_ccs', 'pc_cenday as pc_cenday_ccs', 'dob as dob_ccs')
ccs = ccs.filter(col('Student_Duplicate') == 0)

# ------------------------------------------------------------------------------------------------------------------- #
# ------------------------------------------------------ Save  ------------------------------------------------------ #
# ------------------------------------------------------------------------------------------------------------------- #    

# Columns
ccs2 = ccs.selectExpr('id',
                      'hh_id',
                      'qid',
                      'residence_type',
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
                      'estimate_age',

                      'address',
                      'pc',
                      'pc_sect',
                      'pc_dist',
                      'pc_area',
                      'uprn',
                      'house_no',
                      'pc_substr',

                      'address_cenday',
                      'pc_cenday',
                      'uprn_cenday',
                      'house_no_cenday',
                      
                      'address_ccsday',
                      'pc_ccsday',
                      'uprn_ccsday',
                      'house_no_ccsday',  
                      'LA',
                      'Region',
                      
                      'address_alt',
                      'pc_alt',
                      'uprn_alt',
                      'house_no_alt',                      
                      
                      'sex',
                      'marital_status',
                      'ethnic5',
                      'ethnic19',
                      'mode',
                      'HTCW')

# Column Names
for col_ in ccs2.columns:
  ccs2 = ccs2.withColumnRenamed(col_, col_ + "_ccs")
  
# Save
ccs2.write.mode("overwrite").parquet(FILE_PATH('Stage_1_clean_ccs'))
ccs_students_NTTA.write.mode("overwrite").parquet(FILE_PATH('Stage_1_clean_ccs_students_TTA'))

sparkSession.stop()