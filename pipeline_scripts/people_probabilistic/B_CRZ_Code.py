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
PROB = sparkSession.read.parquet(FILE_PATH('Stage_5_Probabilistic_Scores'))

# Columns  
PROB = PROB.select('id_cen',  'id_ccs',  'fullname_ccs', 'fullname_cen', 'dob_ccs', 'dob_cen', 'age_cen', 'age_ccs',
                   'sex_ccs', 'sex_cen', 'pc_ccs', 'pc_cen', 'address_ccs', 'address_cen', 'pc_alt_cen', 'pc_alt_ccs', 'match_score') 
  
# ------------------------------- #
# --------- THRESHOLDS ---------- #
# ------------------------------- #  

# Add column indicating if match was made deterministically
DET = sparkSession.read.parquet(FILE_PATH('Stage_2_All_Deterministic_People_Matches')).filter(~(col('mkey').isin(Parameters.Clerical_Keys()))).select('id_cen', 'id_ccs', 'mkey').withColumn('DET', lit(1))
PROB = PROB.join(DET, on = ['id_ccs', 'id_cen'], how = 'left').persist()
PROB.count()

# Pandas df to append to
df = pd.DataFrame(columns = ['TOTAL_PAIRS_IN_RANGE', 'SCORE_MIN', 'SCORE_MAX', 'MATCHED_MKEYS', 'NOT_MATCHED_MKEYS'])

# Score ranges to loop through
max_score = round(PROB.agg({"match_score": "max"}).collect()[0][0])
min_score = 0

# Loop
for score in list(reversed(list(np.arange(min_score, max_score + 1, 0.5)))):
  PAIRS = PROB.filter(PROB.match_score.between(score, score + 0.5))
  TOTAL = PAIRS.count()
  
  if TOTAL != 0:
    TRUE = PAIRS.filter(PAIRS.DET == 1).count()
    FALSE = PAIRS.filter(PAIRS.DET.isNull()).count()
    
    # Append to df
    df = df.append({'TOTAL_PAIRS_IN_RANGE': TOTAL, 'SCORE_MIN': str(score), 'SCORE_MAX': str(score + 0.5), 'MATCHED_MKEYS': TRUE, 'NOT_MATCHED_MKEYS': FALSE}, ignore_index = True)
  
# ------------------------- #
# --------- SAVE ---------- #
# ------------------------- #

# Sort
df['SCORE_MAX'] = df['SCORE_MAX'].astype(float)
df = df.sort_values(['SCORE_MAX'], ascending = False)

# Save csv for plots to derive threshold
df.to_csv('some_path/Data.csv')

# Convert to Spark DF and save data for clerical
df = sparkSession.createDataFrame(df)
df.write.mode('overwrite').parquet(FILE_PATH('Stage_5_Clerical_Resolution_Zone_Thresholds'))

sparkSession.stop()
