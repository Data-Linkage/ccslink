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

# Clerical matches
clerical = sparkSession.read.parquet(FILE_PATH('Stage_5_Clerical_Matches')).select('id_ccs', 'id_cen').persist()
clerical.count()

# ------------------------ #
# ------- RELOADS -------- #
# ------------------------ #

# --- If we want to send Kainos an updated set of individual matches, we need to remove any pairs where a decision has already been made in the CMS --- #

# Already given status in previous CMS loads
DECISIONS = sparkSession.read.parquet(FILE_PATH('Stage_5_CMS_Decisions')).select('id_ccs', 'id_cen')

# Remove from next set of CMS records
clerical = clerical.join(DECISIONS, on = ['id_cen', 'id_ccs'], how = 'left_anti')

# ------------------------ #
# ------- CLUSTER -------- #
# ------------------------ #

# Add cluster number & sort
clerical = CF.cluster_number(clerical, 'id_cen', 'id_ccs')

# Take Census & CCS person IDs with cluster number & dataset indicator
cen = clerical.selectExpr('id_cen as Unique_ID', 'Cluster_Number').withColumn('Dataset', lit('census')).dropDuplicates()
ccs = clerical.selectExpr('id_ccs as Unique_ID', 'Cluster_Number').withColumn('Dataset', lit('ccs')).dropDuplicates()

# Combine & Sort
df = cen.unionByName(ccs).sort(asc('Cluster_Number'))

# Counts for ingest form
print("the number of rows to be ingested to the CMS is " + str(df.count()))
print("the number of clusters to be ingested to the CMS is " + str(df.selectExpr('max(Cluster_Number) as Max_Cluster_Number').first().Max_Cluster_Number))

# ------------------------- #
# --------- SAVE ---------- #
# ------------------------- #

# Save
df.write.mode('overwrite').parquet(FILE_PATH('Stage_5_Clerical_Matches_Cluster_No'))

# Save for Kainos
df.write.mode("overwrite").csv('/data/dap/c21_processing_zone/c21_cmatch_hdfs_h/file/cms/'+ datetime.date.today().strftime("%Y/%m/%d") + '/matching_algorithm_outputs/residents_match/01_PPL_Matches.csv', header = True)

sparkSession.stop()
