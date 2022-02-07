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

# Read in cleaned ccs
ccs = sparkSession.read.parquet(FILE_PATH('Stage_1_clean_ccs')).persist()
ccs.count()

# Read in cleaned cen
cen = sparkSession.read.parquet(FILE_PATH('Stage_1_clean_census')).persist()
cen.count()


# ------------------------------ #
# --------- VARIABLES ---------- #
# ------------------------------ #


# Select the CCS columns required for this stage
ccs = ccs.select('id_ccs','fullname_ns_ccs','fullname_ccs','fn_ccs','fn1_ccs','fn1_nickname_ccs','fn2_ccs','sn_ccs','sn1_ccs','sn2_ccs',
                 'res_fn1_ccs','res_sn1_ccs','dob_ccs','day_ccs','mon_ccs','year_ccs','age_ccs','sex_ccs','alphaname_ccs',
                 'address_ccs','pc_ccs','pc_area_ccs','pc_sect_ccs','uprn_ccs','house_no_ccs','pc_substr_ccs',
                 'address_ccsday_ccs','address_cenday_ccs', 'pc_ccsday_ccs', 'pc_alt_ccs')

# Select the Census columns required for this stage
cen = cen.select('id_cen','fullname_ns_cen','fullname_cen','fn_cen','fn1_cen','fn1_nickname_cen','fn2_cen','sn_cen','sn1_cen','sn2_cen',
                 'res_fn1_cen','res_sn1_cen','dob_cen','day_cen','mon_cen','year_cen','age_cen','sex_cen','alphaname_cen',
                 'address_cen','pc_cen','pc_area_cen','pc_sect_cen','uprn_cen','house_no_cen','pc_substr_cen', 'pc_alt_cen')


# ------------------------------------------- #
# ---------------- MATCHKEYS ---------------- #
# ------------------------------------------- #


# We import the matchkeys from the parameter file
# For code to work, df1 must be CCS and df2 must be CENSUS
keys = Parameters.MATCHKEYS(df1 = ccs, df2 = cen)

# Columns to keep in matched file
columns = ['id_ccs', 'id_cen', 'mkey']
    
# Create an schema that we use to create an empty dataframe
schema_1 = StructType([StructField("id_ccs", StringType(), True),
                       StructField("id_cen", StringType(), True),
                       StructField("mkey",   IntegerType(),True)])

# Create the empty dataframe using the schema to append candidate pairs to
matches = sparkSession.createDataFrame([],schema_1)

# Import list of Matchkeys that use Jaro / Multiple moves / Contained Name
loose_jaro_mkeys = Parameters.Loose_Jaro_Keys()
strict_jaro_mkeys = Parameters.Strict_Jaro_Keys()
move_mkeys = Parameters.Move_Keys()
cont_mkeys = Parameters.Cont_Keys()

# Matching LOOP
for i, EX in enumerate(keys, 1):
    
    # Print key number
    print("\n MATCHKEY",i)
    
    # Join on blocking pass i 
    df = ccs.join(cen, on = EX, how = 'inner')
        
    # Apply Jaro Winkler filter to required mkeys
    if i in loose_jaro_mkeys:
      import jellyfish
      df = df.filter((PF.jaro_udf(col('fn1_cen'), col('fn1_ccs')) > 0.80) & (PF.jaro_udf(col('sn1_cen'), col('sn1_ccs')) > 0.80))

    # Apply stricter forename Jaro Winkler filter to required mkeys
    if i in strict_jaro_mkeys:
      import jellyfish
      df = df.filter((PF.jaro_udf(col('fn1_cen'), col('fn1_ccs')) > 0.90) & (PF.jaro_udf(col('sn1_cen'), col('sn1_ccs')) > 0.80))
      
    # Apply Multiple Moves filter to required mkeys
    if i in move_mkeys:
      
      # Calculate number of moves between households & apply filter
      df = df.withColumn('mult_moves', when(((col('pc_cen').isNull() == False)   & (col('pc_ccs').isNull() == False)   & (F.count('*').over(Window.partitionBy('pc_cen', 'pc_ccs', 'house_no_cen', 'house_no_ccs')) > 1)), lit(1)).otherwise(lit(0)))
      df = df.withColumn('mult_moves', when(((col('uprn_cen').isNull() == False) & (col('uprn_ccs').isNull() == False) & (F.count('*').over(Window.partitionBy('uprn_cen', 'uprn_ccs')) > 1)), lit(1)).otherwise(col('mult_moves')))
      df = df.filter(col('mult_moves') == 1).drop('mult_moves')
      
      # Count occurrences of cen and ccs id
      df = df.withColumn('id_cen_count', count('id_cen').over(Window.partitionBy("id_cen")))
      df = df.withColumn('id_ccs_count', count('id_ccs').over(Window.partitionBy("id_ccs")))
      
      # For ID Counts filter where count > 1
      df = df.filter((df.id_cen_count == 1) & (df.id_ccs_count == 1))
      
    # Apply Contained Name filter to required mkeys
    if i in cont_mkeys:
      df = df.filter((col('fullname_ccs').isNull() == False) & (col('fullname_cen').isNull() == False))
      df = df.filter(PF.contained_name_udf('fullname_ccs', 'fullname_cen') == 1)
      
    # Create the KEY column
    df = df.withColumn('mkey', lit(i)).select(columns)
        
    # Append pairs to final dataset
    matches = matches.union(df)

    # Minimum blocking pass for every unique PAIR
    matches = matches.withColumn('Min_Key_match', F.min('mkey').over(Window.partitionBy("id_ccs", "id_cen")))

    # Remove duplicates whilst keeping duplicate from best mkey
    matches = matches.filter(matches.mkey == matches.Min_Key_match).drop('Min_Key_match')
    
    # Checkpoint
    if (i % 15) == 0:
      matches.write.mode("overwrite").parquet(FILE_PATH('SPARK_MEMORY_CLEAR_1'))
      matches = sparkSession.read.parquet(FILE_PATH('SPARK_MEMORY_CLEAR_1')) 
      
    # Count update
    matches.persist()
    print(matches.count())

# View matchkey counts
matches.groupBy('mkey').count().sort(F.asc('mkey')).show(50)


# ------------------------------ #
# ----------- SAVE ------------- #
# ------------------------------ #


# Save datasets
matches.write.mode("overwrite").parquet(FILE_PATH('Stage_2_All_Deterministic_People_Matches'))

sparkSession.stop()
