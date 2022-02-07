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

# Read in file containing samples from each matchkey
QA = sparkSession.read.parquet(FILE_PATH('Stage_2_MK_QA_Sample')).selectExpr('id_ccs', 'id_cen', 'mkey').persist()
QA.count() # 20,832

# Check counts in QA file
QA.groupBy('mkey').count().sort(asc('mkey')).show(50)

# Remove MK column
QA = QA.select('id_ccs', 'id_cen')

# Add cluster number & sort
df = CF.cluster_number(QA, 'id_cen', 'id_ccs')

# Take Census & CCS person IDs with cluster number & dataset indicator
cen = df.selectExpr('id_cen as Unique_ID', 'Cluster_Number').withColumn('Dataset', lit('census')).dropDuplicates()
ccs = df.selectExpr('id_ccs as Unique_ID', 'Cluster_Number').withColumn('Dataset', lit('ccs')).dropDuplicates()

# Combine & Sort
QA = cen.unionByName(ccs).sort(asc('Cluster_Number'))

# Counts for ingest form
print("the number of rows to be ingested to the CMS is " + str(QA.count()))
print("the number of clusters to be ingested to the CMS is " + str(QA.agg({"Cluster_Number": "max"}).collect()[0]))

# ------------------------- #
# --------- SAVE ---------- #
# ------------------------- #

# Save for clerical matching (_QA added to file name)
QA.write.mode("overwrite").csv('some_path' + datetime.date.today().strftime("%Y/%m/%d") + '/matching_algorithm_outputs/residents_match/01_PPL_Matches.csv', header = True)

sparkSession.stop()
