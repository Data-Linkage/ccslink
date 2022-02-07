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

# ------------------------- #
# --------- DATA ---------- #
# ------------------------- #   
  
# Read in Deterministic & Probabilistic Matches
DET  = sparkSession.read.parquet(FILE_PATH('Stage_4_All_Matches_Post_Associative')).select('id_cen', 'id_ccs', 'mkey').persist()
PROB = sparkSession.read.parquet(FILE_PATH('Stage_5_Probabilistic_Scores')).select('id_cen', 'id_ccs', 'match_score').persist()
PROB.count()
DET.count()

# ------------------------------- #
# --------- THRESHOLDS ---------- #
# ------------------------------- #  

# Upper/Lower Probabilistic Thresholds
UPPER_THRESHOLD = Parameters.UPPER_PROB_THRESHOLD()
LOWER_THRESHOLD = Parameters.LOWER_PROB_THRESHOLD()

# Automatically accept matches above threshold
PROB_ACCEPT = PROB.filter(PROB.match_score > UPPER_THRESHOLD)

# Clerical Resolution Zone
PROB_CLERICAL = PROB.filter((PROB.match_score.between(LOWER_THRESHOLD, UPPER_THRESHOLD)))

# ---------------------------- #
# --------- COMBINE ---------- #
# ---------------------------- #  

# MATCHKEY NUMBER for Probabilistic pairs 
PROB_ACCEPT = PROB_ACCEPT.withColumn('mkey', lit(None))
PROB_CLERICAL = PROB_CLERICAL.withColumn('mkey', lit(None))

# MATCH SCORE for Auto Accept Probabilistic & Deterministic pairs
DET = DET.withColumn('match_score', lit(None))

# CLERICAL Indicators
DET = DET.withColumn('CLERICAL', lit(0))
PROB_ACCEPT = PROB_ACCEPT.withColumn('CLERICAL', lit(0))
PROB_CLERICAL = PROB_CLERICAL.withColumn('CLERICAL', lit(1))

# Combine 
MATCHES = DET.unionByName(PROB_ACCEPT)  
MATCHES = MATCHES.unionByName(PROB_CLERICAL).sort(desc('id_cen'))

# Max Score and Min KEY for each CEN/CCS pair
# Ensures PROB and DET values are shown together on a single row (for pairs matched using both methods)
MATCHES = MATCHES.withColumn('match_score', F.max('match_score').over(Window.partitionBy("id_cen", "id_ccs")))
MATCHES = MATCHES.withColumn('mkey', F.min('mkey').over(Window.partitionBy("id_cen", "id_ccs")))

# Remove duplicates (exist when a pair was matched in DET and PROB stages)
MATCHES = MATCHES.drop_duplicates(["id_cen", "id_ccs"])

# ------------------------------- #
# ----- CLERICAL INDICATOR ------ #
# ------------------------------- #  

# ID Counts
MATCHES = MATCHES.withColumn('id_cen_count', count('id_cen').over(Window.partitionBy("id_cen")))
MATCHES = MATCHES.withColumn('id_ccs_count', count('id_ccs').over(Window.partitionBy("id_ccs")))

# Update CLERICAL indicator to include conflicts
MATCHES = MATCHES.withColumn('CLERICAL', when((((MATCHES.id_cen_count > 1) | (MATCHES.id_ccs_count > 1))), lit(1)).otherwise(col('CLERICAL')))

# Remove unique probabilistic record pair from CLERICAL if matched by a matchkey
MATCHES = MATCHES.withColumn('CLERICAL', when(((MATCHES.id_cen_count == 1) & (MATCHES.id_ccs_count == 1) & (MATCHES.mkey.isNull() == False)), lit(0)).otherwise(col('CLERICAL')))

# Update CLERICAL indicator for any CLERICAL MATCHKEYS
MATCHES = MATCHES.withColumn('CLERICAL', when(col('mkey').isin(Parameters.Clerical_Keys()), lit(1)).otherwise(col('CLERICAL')))

# July 30th Update: Remove probabilistic matches scoring between 14.6 and 14.65 (different sex twins)
#MATCHES = MATCHES.filter(~((col('match_score').between(14.6, 14.65)) & (col('mkey').isNull())))

# Dataset containing clerical records only
CLERICAL = MATCHES.filter(MATCHES.CLERICAL == 1)

# Counts
MATCHES.groupBy('CLERICAL').count().show()

# ------------------------- #
# --------- SAVE ---------- #
# ------------------------- #  

# Save ALL matches and CLERICAL matches
MATCHES.write.mode('overwrite').parquet(FILE_PATH('Stage_5_All_Matches_Pre_Clerical'))
CLERICAL.write.mode('overwrite').parquet(FILE_PATH('Stage_5_Clerical_Matches')) 

sparkSession.stop()
