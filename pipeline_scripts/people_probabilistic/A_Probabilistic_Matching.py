# ----------------------- #
# -------- SETUP -------- #
# ----------------------- #

# Changes to SparkSession 
sparkSession.conf.set('spark.sql.codegen.wholeStage', 'false')

# Import PySpark, Parameters, File Paths, Functions & Packages
import pyspark
from CCSLink import Parameters
from CCSLink.Parameters import FILE_PATH
from CCSLink import Person_Functions as PF
from CCSLink import Household_Functions as HF
from CCSLink import Cluster_Function as CF
exec(open("/home/cdsw/collaborative_method_matching/CCSLink/Packages.py").read()) 


# ------------------------- #
# --------- DATA ---------- #
# ------------------------- #        

# Full Cleaned CEN & CCS
CCS = sparkSession.read.parquet(FILE_PATH('Stage_1_clean_ccs'))
CEN = sparkSession.read.parquet(FILE_PATH('Stage_1_clean_census'))

# Select columns required
CCS = CCS.select('id_ccs','fullname_ccs','fn_ccs','fn1_ccs','fn1_nickname_ccs','sn_ccs','sn1_ccs','dob_ccs','day_ccs','mon_ccs','year_ccs','age_ccs','sex_ccs','pc_ccs','pc_alt_ccs','uprn_ccs','address_ccs','address_cenday_ccs','pc_cenday_ccs','address_ccsday_ccs','pc_ccsday_ccs')
CEN = CEN.select('id_cen','fullname_cen','fn_cen','fn1_cen','fn1_nickname_cen','sn_cen','sn1_cen','dob_cen','day_cen','mon_cen','year_cen','age_cen','sex_cen','pc_cen','pc_alt_cen','uprn_cen','address_cen')

# ----------------------------- #
# --------- BLOCKING ---------- #
# ----------------------------- #

# Block on postcodes
BP1 = [CCS.pc_cenday_ccs == CEN.pc_cen]     # 0
BP2 = [CCS.pc_cenday_ccs == CEN.pc_alt_cen] # 0
BP3 = [CCS.pc_ccsday_ccs == CEN.pc_cen]     # 29538316
BP4 = [CCS.pc_ccsday_ccs == CEN.pc_alt_cen] # 261564
BP5 = [CCS.pc_alt_ccs    == CEN.pc_cen]     # 535470
BP6 = [CCS.pc_alt_ccs    == CEN.pc_alt_cen] # 11678

# Combine
for i, BP in enumerate([BP1,BP2,BP3,BP4,BP5,BP6], 1):
  
  if i == 1:
    combined_blocks = CCS.join(CEN, on = BP, how = 'inner')
    print(combined_blocks.count())
    
  if i > 1:
    combined_blocks_2 = CCS.join(CEN, on = BP, how = 'inner')
    print(combined_blocks_2.count())
    combined_blocks = combined_blocks.union(combined_blocks_2).drop_duplicates(['id_ccs', 'id_cen'])

# Count
combined_blocks.persist().count() 

# -------------------------------------------------- #
# --------------- AGREEMENT VECTORS ---------------- #
# -------------------------------------------------- #

# Agreement vector is created which is then inputted into the EM Algorithm.
# Set v1, v2,... , vx... as the agreement variables

# Select agreement variables
v1 = 'fn1'
v2 = 'sn1'
v3 = 'dob'
v4 = 'sex'

# All agreement variables used to calculate match weights & probabilities
all_variables = [v1, v2, v3, v4]

# Variables using partial agreement (string similarity)
edit_distance_variables = [v1, v2]
dob_variables = [v3]

# Cut off values for edit distance variables
cutoff_values = [0.45, 0.45]
dob_cutoff_values = [0.50]

# Remaining Variables - Only zero or full agreement for these
remaining_variables = [v4]

# Create a Edit Distance Column for all pairs
for variable in edit_distance_variables:
  combined_blocks = combined_blocks.withColumn(variable + "_agreement", Parameters.lev_score(variable + "_ccs", variable + "_cen"))

# --------------------------------------------------------- #
# ------------ UPDATE NAMES AGREEMENT SCORES -------------- #
# --------------------------------------------------------- #

# Update name scores using full FN/SN (deals with accidental spaces in names)
combined_blocks = combined_blocks.withColumn('fn1_agreement',  when((Parameters.lev_score("fn_ccs", "fn_cen") > col('fn1_agreement')), Parameters.lev_score("fn_ccs", "fn_cen")).otherwise(col('fn1_agreement')))
combined_blocks = combined_blocks.withColumn('sn1_agreement',  when((Parameters.lev_score("sn_ccs", "sn_cen") > col('sn1_agreement')), Parameters.lev_score("sn_ccs", "sn_cen")).otherwise(col('sn1_agreement')))

# If score above 0.90, set to 1
combined_blocks = combined_blocks.withColumn('fn1_agreement',  when(col('fn1_agreement') > 0.90, 1).otherwise(col('fn1_agreement')))
combined_blocks = combined_blocks.withColumn('sn1_agreement',  when(col('sn1_agreement') > 0.90, 1).otherwise(col('sn1_agreement')))

# Update FN score using nicknames
combined_blocks = combined_blocks.withColumn('fn1_agreement',  when((col('fn1_nickname_ccs') == col('fn1_nickname_cen')), 1).otherwise(col('fn1_agreement')))

# If Score None then update to 0
combined_blocks = combined_blocks.withColumn('fn1_agreement', when(col('fn1_agreement').isNull(), lit(0)).otherwise(col('fn1_agreement')))
combined_blocks = combined_blocks.withColumn('sn1_agreement', when(col('sn1_agreement').isNull(), lit(0)).otherwise(col('sn1_agreement')))

# --------------------------------------------------------- #
# ---------------- INITIAL M & U VALUES ------------------- #
# --------------------------------------------------------- #

# Read in M and U values
m_values = sparkSession.read.parquet(FILE_PATH('Stage_4_M_Values')).toPandas()
u_values = sparkSession.read.parquet(FILE_PATH('Stage_4_U_Values')).toPandas()

# Save individual M values
FN_M =  m_values[m_values.variable == 'fn1'].iloc[0][1]
SN_M =  m_values[m_values.variable == 'sn1'].iloc[0][1]
SEX_M = m_values[m_values.variable == 'sex'].iloc[0][1]
DOB_M = m_values[m_values.variable == 'dob'].iloc[0][1]

# Save individual U values
FN_U =  u_values[u_values.variable == 'fn1'].iloc[0][1]
SN_U =  u_values[u_values.variable == 'sn1'].iloc[0][1]
SEX_U = u_values[u_values.variable == 'sex'].iloc[0][1]
DOB_U = u_values[u_values.variable == 'dob'].iloc[0][1]

# Add M values back into file
combined_blocks = combined_blocks\
.withColumn('fn1_m',lit(FN_M))\
.withColumn('sn1_m',lit(SN_M))\
.withColumn('sex_m',lit(SEX_M))\
.withColumn('dob_m',lit(DOB_M))

# Add U values back into file
combined_blocks = combined_blocks\
.withColumn('fn1_u',lit(FN_U))\
.withColumn('sn1_u',lit(SN_U))\
.withColumn('sex_u',lit(SEX_U))\
.withColumn('dob_u',lit(DOB_U))

# Add Agreement / Disagreement Weights
for var in all_variables:
  combined_blocks = combined_blocks.withColumn(var + "_agreement_weight", log2((col(var + "_m") / col(var + "_u"))))
  combined_blocks = combined_blocks.withColumn(var + "_disagreement_weight", log2(((1 - col(var + "_m")) / (1 - col(var + "_u")))))
                              
# --------------------------------------------------- #
# ------------------ MATCH SCORES  ------------------ #
# --------------------------------------------------- #

''' An agreement value between 0 and 1 is calculated for each agreeement variable '''  
''' This is done for every candidate record pair '''  

# --------------------------------------- #
# ------------- DOB SCORE  -------------- #
# --------------------------------------- #  

# Partial scores
combined_blocks = combined_blocks.withColumn('day_score',   when(combined_blocks.day_ccs  == combined_blocks.day_cen,  1/3).otherwise(0))
combined_blocks = combined_blocks.withColumn('month_score', when(combined_blocks.mon_ccs  == combined_blocks.mon_cen,  1/3).otherwise(0))
combined_blocks = combined_blocks.withColumn('year_score',  when(combined_blocks.year_ccs == combined_blocks.year_cen, 1/3).otherwise(0))

# Final Score
combined_blocks = combined_blocks.withColumn('DOB_agreement', F.expr("day_score + month_score + year_score")).drop('day_score', 'month_score', 'year_score')

# DOB Agreement Adjustment for strong candidates with missing DOB and AGE within 3
combined_blocks = combined_blocks.withColumn('DOB_agreement', 
                                             when(((abs_(combined_blocks.age_ccs - combined_blocks.age_cen) < 4) & 
                                                   (combined_blocks.fn1_agreement > 0.65) & (combined_blocks.sn1_agreement > 0.65) &
                                                   (combined_blocks.sex_cen == combined_blocks.sex_ccs) &
                                                   (combined_blocks.dob_cen.isNull() | combined_blocks.dob_ccs.isNull())), lit(0.55)).otherwise(col('DOB_agreement')))

# DOB Agreement Adjustment for strong candidates with missing DOB and AGE within 1
combined_blocks = combined_blocks.withColumn('DOB_agreement', 
                                             when(((abs_(combined_blocks.age_ccs - combined_blocks.age_cen) < 2) & 
                                                   (combined_blocks.fn1_agreement > 0.65) & (combined_blocks.sn1_agreement > 0.65) &
                                                   (combined_blocks.sex_cen == combined_blocks.sex_ccs) &
                                                   (combined_blocks.dob_cen.isNull() | combined_blocks.dob_ccs.isNull())), lit(0.7)).otherwise(col('DOB_agreement')))

# ---------------------------------------- #
# ---------- PARTIAL CUT OFFS ------------ #
# ---------------------------------------- #

# All partial variables except DOB
for variable, cutoff in zip(edit_distance_variables, cutoff_values):
  
  # If agreement below a certain level, set agreement to 0. Else, leave agreeement as it is
  combined_blocks = combined_blocks.withColumn(variable + "_agreement", when(col(variable + "_agreement") <= cutoff, 0).otherwise(col(variable + "_agreement")))

# DOB
for variable, cutoff in zip(dob_variables, dob_cutoff_values):
  
  # If agreement below a certain level, set agreement to 0. Else, leave agreeement as it is
  combined_blocks = combined_blocks.withColumn(variable + "_agreement", when(col(variable + "_agreement") <= cutoff, 0).otherwise(col(variable + "_agreement")))

# Remaining variables (no partial scores)
for variable in remaining_variables:
  
  # Calculate 1/0 Agreement Score (no partial scoring)
  combined_blocks = combined_blocks.withColumn(variable + "_agreement", when(col(variable + "_ccs") == col(variable + "_cen"), 1).otherwise(0))

# ------------------------------------------------------------------ #
# -------------------------  WEIGHTS ------------------------------- #
# ------------------------------------------------------------------ #

# https://www.census.gov/content/dam/Census/library/working-papers/1991/adrm/rr91-9.pdf
# weight = Agreement_Weight if Agreement = 1, and
#          MAX{(Agreement_Weight - (Agreement_Weight - Disgreement_Weight)*(1-Agreement)*(9/2)), Disgreement_Weight} if 0 <= Agreement < 1.

# Start by giving all records agreement weights
for variable in all_variables:
  combined_blocks = combined_blocks.withColumn(variable + "_weight", col(variable + "_agreement_weight"))

# Update for partial agreement / disagreement (only when agreement < 1)
for variable in all_variables:
  combined_blocks = combined_blocks.withColumn(variable + "_weight", when(col(variable + "_agreement") < 1,
                                                                                 greatest(((col(variable + "_agreement_weight")) - (((col(variable + "_agreement_weight")) - col(variable + "_disagreement_weight")) * (1 - col(variable + "_agreement")) * (lit(2)))),
                                                                                           (col(variable + "_disagreement_weight")))).otherwise(col(variable + "_weight"))) 

# Set weights to 0 (instead of disagreement_weight) if there is missingess in CCS or CEN variable (agreement == 0 condition needed for DOB)
for variable in all_variables:
  combined_blocks = combined_blocks.withColumn(variable + "_weight", when((((col(variable + "_ccs").isNull()) | (col(variable + "_cen")).isNull()) & 
                                                                  (col(variable + '_agreement') == 0)), lit(0)).otherwise(col(variable + "_weight"))) 
# Sum column wise across the above columns - create match score
combined_blocks = combined_blocks.withColumn("match_score", sum(combined_blocks[col] for col in ["fn1_weight", "sn1_weight", "dob_weight", "sex_weight"]))

# ------------------------------------------------------------------ #
# -----------------------  ADJUSTMENTS ----------------------------- #
# ------------------------------------------------------------------ #

# To reduce false matches going to clerical, if ages are dissimilar set score to 0
combined_blocks = combined_blocks.withColumn("match_score", when(((col('age_ccs').isNotNull()) & 
                                                                  (col('age_cen').isNotNull()) &
                                                                  (abs_(combined_blocks.age_ccs - combined_blocks.age_cen) > 5)), lit(0)).otherwise(col('match_score')))
# -------------------------------------- #
# --------------  SAVE ----------------- #
# -------------------------------------- #

# Save
combined_blocks.write.mode('overwrite').parquet(FILE_PATH('Stage_5_Probabilistic_Scores'))

sparkSession.stop()
