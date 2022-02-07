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

# Full Cleaned CEN & CCS
CCS = sparkSession.read.parquet(FILE_PATH('Stage_1_clean_ccs'))
CEN = sparkSession.read.parquet(FILE_PATH('Stage_1_clean_census'))

# Treat deterministic matches as a gold standard to calculate M and U values
GS = sparkSession.read.parquet(FILE_PATH('Stage_2_All_Deterministic_People_Matches')).withColumn('Load', lit(0)).withColumn('match_status', lit(1))

# Filter out candidates from clerical matcheys
GS = GS.filter(~(col('mkey').isin(Parameters.Clerical_Keys()))).drop('mkey')

# ------------------------------------- #
# ---------- ACTIVE LEARNING ---------- #
# ------------------------------------- #

# File paths of all individual decisions already made in the CMS
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

# Update set of matched records before calculating M & U values
if len(paths) > 0:

  # Combine into single set of decisions
  for i, path in enumerate(paths, 1):

    # Read in all decisions
    df = sparkSession.read.option("delimiter", ";").csv(path, header = True).selectExpr('CCS_Resident_ID as id_ccs', 'census_Resident_ID as id_cen', 'match_status')
    
    # Add load number
    df = df.withColumn('Load', lit(i))
    
    # Combine decisions
    GS = GS.unionByName(df)
    
    # Resolve possible conflicts by taking most recent load decision (e.g. match in load 1 but nonmatch in load 2)
    GS = GS.withColumn('Max_Load', F.max(col('Load')).over(Window.partitionBy(['id_ccs', 'id_cen'])))
    GS = GS.filter(col('Max_Load') == col('Load')).drop('Max_Load')
    
# Take matches only once all matches have been combined
GS = GS.filter(col('match_status') == 1).drop('match_status', 'Load')

# ------------------------------------- #
# -------- Join CEN & CCS to GS ------- #
# ------------------------------------- #

# Join the CCS / CEN person data to the GS
GS = GS.join(CCS, on = 'id_ccs', how = 'inner')
GS = GS.join(CEN, on = 'id_cen', how = 'inner')
GS.persist().count()

# Variables to calculate M/U values for
variables = Parameters.MU_Variables()

# --------------------------- #
# --------- M VALUES -------- #
# --------------------------- #

# Create empty dataframe to add m values to
m_values = pd.DataFrame([])

# Store total number of records for use in calculation
total_records = GS.count()

# --- for loop --- #

# For each variable:
for v in variables:
  print(v)  
  
  # Create a column that stores whether or not there is exact agreement for that pair
  GS = GS.withColumn((v + "_exact"), when(col((v + "_ccs")) == GS[(v + "_cen")], 1).otherwise(0))
  
  # Use the sum_col function to create a total number of pairs with exact agreement
  exact = GS.select(F.sum(v + "_exact")).collect()[0][0]
  
  # Divide the total number of exact matches by the total number of records
  value = exact / total_records
  
  # Store the results in a data frame
  m_values = m_values.append(pd.DataFrame({'variable': v, 'm_value': value}, index=[1]), ignore_index=True)

print(m_values)

# ------------------------------ #
# ---------- U VALUES ---------- #
# ------------------------------ #

# ----- Sample for calculating U values from full census ----- #


# Randomly sort datasets
CEN = CEN.withColumn("random", F.rand()).sort(desc("random"))
CCS = CCS.withColumn("random", F.rand()).sort(desc("random"))

# Add a ID column to join on
CCS = CCS.withColumn("id", F.monotonically_increasing_id()).drop('random')
CEN = CEN.withColumn("id", F.monotonically_increasing_id()).drop('random')

# Join to match the random samples together
sample = CCS.join(CEN, on = 'id', how = 'inner').persist()
sample.count()

# Convert variable types
sample = sample.withColumn("sex_ccs", sample["sex_ccs"].cast(StringType()))
sample = sample.withColumn("sex_cen", sample["sex_cen"].cast(StringType()))

# DataFrame to append to
u_values = pd.DataFrame([])

# For name variables:
for v in ['fn1', 'sn1', 'sex', 'dob']:
  
  # Remove missing CCS rows
  sample = sample.filter(sample[(v + "_ccs")].isNull() == False)
  
  # Remove missing CEN rows
  sample = sample.filter(sample[(v + "_cen")].isNull() == False)
  
  # Count
  total = sample.count()
  
  # Agreement count 
  exact = sample.filter(sample[(v + "_ccs")] == sample[(v + "_cen")]).count()
  
  # Proportion
  value = exact / total
  
  # Append to DataFrame
  u_values = u_values.append(pd.DataFrame({'variable': v, 'u_value': value}, index=[1]), ignore_index=True)

# Add DOB U value if needed
# u_values = u_values.append(pd.DataFrame(data = ({'u_value': [(1/(365*80)) * 100], 'variable': ['dob']})), ignore_index = True)

# Print
print(u_values)

# ------------------------------------- #
# --------------- SAVE ---------------- #
# ------------------------------------- #

# Spark DataFrame
m_values_spark = sparkSession.createDataFrame(m_values)
u_values_spark = sparkSession.createDataFrame(u_values)

# Save M/U values
m_values_spark.write.mode('overwrite').parquet(FILE_PATH('Stage_4_M_Values'))
u_values_spark.write.mode('overwrite').parquet(FILE_PATH('Stage_4_U_Values'))

sparkSession.stop()
