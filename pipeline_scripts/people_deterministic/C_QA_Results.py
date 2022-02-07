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

# ---------------------------- #
# --------- RESULTS ---------- #
# ---------------------------- #

# Pairs sent for QA
MKQA = sparkSession.read.parquet(FILE_PATH('Stage_2_MK_QA_Sample')).selectExpr('id_ccs', 'id_cen', 'mkey').persist()
MKQA.count()

# Export Date + LOAD NO.
DATE_LOAD = '2021/07/29/' + '3'

# File path of QA results from CMS
path = '/data/dap/c21_processing_zone/c21_cmatch_hdfs_h/file/cms_outputs/{}/residents_match/*.csv'.format(DATE_LOAD)

# Read in results
RESULTS = sparkSession.read.option("delimiter", ";").csv(path, header = True)
RESULTS = RESULTS.selectExpr('CCS_Resident_ID as id_ccs', 'census_Resident_ID as id_cen', 'match_status')

# Join IDs, MK and match_status together
MKQA = MKQA.join(RESULTS, on = ['id_cen', 'id_ccs'], how = 'left')

# Check match_status added to all record pairs
MKQA.filter(col('match_status').isNull()).count()

# Total matches, TP and FP in each MK
MKQA = MKQA.withColumn('TOTAL', count('mkey').over(Window.partitionBy("mkey")))
MKQA = MKQA.withColumn('TP', F.sum('match_status').over(Window.partitionBy("mkey")))
MKQA = MKQA.withColumn('FP', (col('TOTAL') - col('TP')))

# Show results
MKQA.dropDuplicates(['mkey']).sort(asc('mkey')).show(50, False)

# ---------------------------- #
# --------- VIEW FP ---------- #
# ---------------------------- #

# Read in cleaned ccs/cen
ccs = sparkSession.read.parquet(FILE_PATH('Stage_1_clean_ccs')).persist()
cen = sparkSession.read.parquet(FILE_PATH('Stage_1_clean_census')).persist()
ccs.count()
cen.count()

# Select columns
ccs = ccs.select('id_ccs', 'fullname_ccs', 'dob_ccs', 'age_ccs', 'sex_ccs', 'address_ccs', 'pc_ccs', 'uprn_ccs', 'address_alt_ccs', 'pc_alt_ccs')
cen = cen.select('id_cen', 'fullname_cen', 'dob_cen', 'age_cen', 'sex_cen', 'address_cen', 'pc_cen', 'uprn_cen', 'address_alt_cen', 'pc_alt_cen')

# Join on variables
MKQA = MKQA.join(ccs, on = 'id_ccs', how = 'left').join(cen, on = 'id_cen', how = 'left').persist()
MKQA.count()

# Sort by MK and show
MKQA.select('fullname_ccs', 'fullname_cen', 'dob_ccs', 'dob_cen','address_ccs',
            'address_cen', 'pc_ccs', 'pc_cen', 'address_alt_ccs', 'address_alt_cen', 'mkey')\
            .filter('match_status == 0').sort(asc('mkey')).show(100, False)

# ----------------------------- #
# ----- RESEND FP TO CMS ------ #
# ----------------------------- #

# Filter to keep FP only
FP = MKQA.filter('match_status == 0').select('id_cen', 'id_ccs')

# Add cluster number & sort
clerical = CF.cluster_number(FP, 'id_cen', 'id_ccs')

# Take Census & CCS person IDs with cluster number & dataset indicator
cen = clerical.selectExpr('id_cen as Unique_ID', 'Cluster_Number').withColumn('Dataset', lit('census')).dropDuplicates()
ccs = clerical.selectExpr('id_ccs as Unique_ID', 'Cluster_Number').withColumn('Dataset', lit('ccs')).dropDuplicates()

# Combine & Sort
df = cen.unionByName(ccs).sort(asc('Cluster_Number'))

# Counts for ingest form
print("the number of rows to be ingested to the CMS is " + str(df.count()))
print("the number of clusters to be ingested to the CMS is " + str(QA.selectExpr('max(Cluster_Number) as Max_Cluster_Number').first().Max_Cluster_Number))

# Save for CMS
df.write.mode("overwrite").csv('/data/dap/c21_processing_zone/c21_cmatch_hdfs_h/file/cms/'+ datetime.date.today().strftime("%Y/%m/%d") + '/matching_algorithm_outputs/residents_match/01_PPL_Matches.csv', header = True)
