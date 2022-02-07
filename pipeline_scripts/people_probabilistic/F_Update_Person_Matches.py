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

# Read in automatic person matches that we have so far (CLERICAL == 0)
MATCHES = sparkSession.read.parquet(FILE_PATH('Stage_5_All_Matches_Pre_Clerical')).filter(col('CLERICAL') == 0).select('id_cen', 'id_ccs', 'match_score', 'mkey', 'CLERICAL')

# ------------------------- #
# ----- RELOAD PATHS ------ #
# ------------------------- #

# File paths of extra person decisions made in the CMS
paths = ['/data/dap/c21_processing_zone/c21_cmatch_hdfs_h/file/cms_outputs/2021/08/16/16/residents_match/*.csv',         # All CRZ matches after resolving conflicts
         '/data/dap/c21_processing_zone/c21_cmatch_hdfs_h/file/cms_outputs/2021/08/16/17/residents_match/*.csv',         # Extra CRZ matches after resident name fix
         '/data/dap/c21_processing_zone/c21_cmatch_hdfs_h/file/cms_outputs/2021/08/27/30/householdsppl_match/ab/*.csv',  # Households AB person matches
         '/data/dap/c21_processing_zone/c21_cmatch_hdfs_h/file/cms_outputs/2021/09/02/37/householdsppl_match/cd/*.csv',  # Households CD (1) person matches
         '/data/dap/c21_processing_zone/c21_cmatch_hdfs_h/file/cms_outputs/2021/09/02/38/householdsppl_match/cd/*.csv',  # Households CD (2) person matches
         '/data/dap/c21_processing_zone/c21_cmatch_hdfs_h/file/cms_outputs/2021/10/27/113/residents_match/*.csv',        # V16: All CRZ matches after resolving conflicts
         '/data/dap/c21_processing_zone/c21_cmatch_hdfs_h/file/cms_outputs/2021/11/02/123/householdsppl_match/ab/*.csv', # V16: Households AB person matches
         '/data/dap/c21_processing_zone/c21_cmatch_hdfs_h/file/cms_outputs/2021/11/08/125/householdsppl_match/cd/*.csv', # V16: Households CD person matches
         '/data/dap/c21_processing_zone/c21_cmatch_hdfs_h/file/cms_outputs/2021/11/17/134/residents_match/*.csv',        # V20: All CRZ matches
         '/data/dap/c21_processing_zone/c21_cmatch_hdfs_h/file/cms_outputs/2021/11/22/139/householdsppl_match/ab/*.csv', # V20: Households AB person matches
         '/data/dap/c21_processing_zone/c21_cmatch_hdfs_h/file/cms_outputs/2021/11/23/140/householdsppl_match/cd/*.csv', # V20: Households CD person matches
         '/data/dap/c21_processing_zone/c21_cmatch_hdfs_h/file/cms_outputs/2021/12/08/155/residents_match/*.csv',        # V20: Presearch matches
         '/data/dap/c21_processing_zone/c21_cmatch_hdfs_h/file/cms_outputs/2021/12/09/159/householdsppl_match/cd/*.csv'] # V20: Households EF person matches

# ------------------------- #
# ----- CMS DECISIONS ----- #
# ------------------------- #

# --- Combine all decisions from CMS across all loads --- #

# Empty dataset to append all decisions made in CMS
DECISIONS = sparkSession.createDataFrame([], StructType([StructField("id_ccs", StringType(), True),
                                                         StructField("id_cen", StringType(), True),
                                                         StructField("match_status", IntegerType(), True),
                                                         StructField("Load", IntegerType(), True)]))
# Run if paths list not empty
if len(paths) > 0:

  # Combine into single set of decisions
  for i, path in enumerate(paths, 1):

    # Read in all decisions
    df = sparkSession.read.option("delimiter", ";").csv(path, header = True).selectExpr('CCS_Resident_ID as id_ccs', 'census_Resident_ID as id_cen', 'match_status')
    
    # Add load number
    df = df.withColumn('Load', lit(i))
    
    # Combine decisions
    DECISIONS = DECISIONS.unionByName(df)
    
    # Resolve possible conflicts by taking most recent load decision (e.g. match in load 1 but nonmatch in load 2)
    DECISIONS = DECISIONS.withColumn('Max_Load', F.max(col('Load')).over(Window.partitionBy(['id_ccs', 'id_cen'])))
    DECISIONS = DECISIONS.filter(col('Max_Load') == col('Load')).drop('Max_Load')
       
# Separate matches from non-matches
ACCEPTED = DECISIONS.filter(col('match_status') == 1).select('id_cen', 'id_ccs')
REJECTED = DECISIONS.filter(col('match_status') == 0).select('id_cen', 'id_ccs')

# ---------------------------- #
# --- ADD ACCEPTED MATCHES --- #
# ---------------------------- #
    
# Null Columns
ACCEPTED = ACCEPTED.withColumn('match_score', lit(99))
ACCEPTED = ACCEPTED.withColumn('mkey', lit(None))

# Clerical Indicator
ACCEPTED = ACCEPTED.withColumn('CLERICAL', lit(1))

# Combine
MATCHES_UPDATED = MATCHES.unionByName(ACCEPTED)
MATCHES_UPDATED.persist().count()

# If a CMS match has already been made automatically, keep the auto match
MATCHES_UPDATED = MATCHES_UPDATED.withColumn('CLERICAL', F.min('CLERICAL').over(Window.partitionBy("id_cen", "id_ccs")))
MATCHES_UPDATED = MATCHES_UPDATED.drop_duplicates(["id_cen", "id_ccs"])
  
# ------------------------------- #
# --- DELETE REJECTED MATCHES --- #
# ------------------------------- #
  
# If any rejected CMS matches also exist in our set of person matches, remove them
MATCHES_UPDATED = MATCHES_UPDATED.join(REJECTED.withColumn('Join', lit(1)), on = ['id_cen', 'id_ccs'], how = 'left')
MATCHES_UPDATED = MATCHES_UPDATED.filter(col('Join').isNull()).drop('Join')

# -------------------- #
# ------- SAVE ------- #
# -------------------- #

# Save
MATCHES_UPDATED.write.mode('overwrite').parquet(FILE_PATH('Stage_5_All_Matches_Post_Clerical'))

