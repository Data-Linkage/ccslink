# ----------------------- #
# -------- SETUP -------- #
# ----------------------- #


# Set up Spark Session
import Spark_Session as SS
sparkSession = SS.SPARK()

# Import PySpark, Parameters, File Paths, Functions & Packages
import pyspark
import Parameters
from Parameters import FILE_PATH
import Person_Functions as PF
import Household_Functions as HF
import Cluster_Function as CF
exec(open("/home/cdsw/collaborative_method_matching/CCSLink/Packages.py").read()) 


# ------------------------- #
# --------- DATA ---------- #
# ------------------------- #


# Read in clean data
CCS = sparkSession.read.parquet(FILE_PATH('Stage_1_clean_HHs_ccs'))
CEN = sparkSession.read.parquet(FILE_PATH('Stage_1_clean_HHs_census'))

# Select cols
CEN = CEN.select('hh_id_cen', 'typaccom_cen', 'tenure_cen', 'no_resi_cen', 'fn_set_cen', 'sn_set_cen', 'address_cen', 'hh_size_cen', 'pc_cen', 'pc_sect_cen', 'dob_set_cen', 'uprn_cen', 'age_set_cen', 'house_no_cen', 'flat_no_cen')
CCS = CCS.select('hh_id_ccs', 'typaccom_ccs', 'tenure_ccs', 'no_resi_ccs', 'fn_set_ccs', 'sn_set_ccs', 'address_ccs', 'hh_size_ccs', 'pc_ccs', 'pc_sect_ccs', 'dob_set_ccs', 'uprn_ccs', 'age_set_ccs', 'house_no_ccs', 'flat_no_ccs', 'address_ccsday_ccs', 'address_cenday_ccs', 'pc_ccsday_ccs', 'pc_cenday_ccs')

# Remove HHs with no people before matching
CEN = CEN.filter(col('hh_size_cen').isNotNull()).persist() 
CCS = CCS.filter(col('hh_size_ccs').isNotNull()).persist() 
CEN.count()
CCS.count()


# ------------------------------------------- #
# ------ FILES TO RESUSE IN MATCHING -------- #
# ------------------------------------------- #  


# UPRN Join
UPRN_Join = CEN.join(CCS, on = [CEN.uprn_cen == CCS.uprn_ccs], how = 'inner').persist()
UPRN_Join.count()

# Postcode Join
PC_Join =  CEN.join(CCS, on = [CEN.pc_cen == CCS.pc_ccs], how = 'inner').persist()
PC_Join.count()

# UPRN, Type, Tenure, Res Count
UPRN_TTR_Join = CEN.join(CCS, on = [CEN.uprn_cen == CCS.uprn_ccs,
                                    CEN.tenure_cen == CCS.tenure_ccs,
                                    CEN.typaccom_cen == CCS.typaccom_ccs,
                                    CEN.no_resi_cen == CCS.no_resi_ccs], how = 'inner').persist()   
UPRN_TTR_Join.count()

# PC, Type, Tenure, Res Count
PC_TTR_Join = CEN.join(CCS, on = [CEN.pc_cen == CCS.pc_ccs,
                                  CEN.tenure_cen == CCS.tenure_ccs,
                                  CEN.typaccom_cen == CCS.typaccom_ccs,
                                  CEN.no_resi_cen == CCS.no_resi_ccs], how = 'inner').persist()
PC_TTR_Join.count()

# PC Sector + HN
SECT_HN_Join = CEN.join(CCS, on = [CEN.pc_sect_cen  == CCS.pc_sect_ccs,
                                   CEN.house_no_cen == CCS.house_no_ccs], how = 'inner').persist()
SECT_HN_Join.count()


# ------------------------------------------------------------------------------------- #
# ------ MK1: UPRN + One Common Surname + One Common Forename + One Common DOB -------- #
# ------------------------------------------------------------------------------------- #  


# MK1
matches_1 = UPRN_Join.filter((HF.common_udf('fn_set_cen', 'fn_set_ccs') >= 1) & 
                             (HF.common_udf('sn_set_cen', 'sn_set_ccs') >= 1) & 
                             (HF.common_udf('dob_set_cen', 'dob_set_ccs') >= 1))


# --------------------------------------------------------------------------------------------- #
# ---- MK2: UPRN + Tenure + TYPE + RESCOUNT + (One Common Surname OR One Common Forename) ----- #
# --------------------------------------------------------------------------------------------- #


# MK2
matches_2 = UPRN_TTR_Join.filter((HF.common_udf('fn_set_cen', 'fn_set_ccs') >= 1) |
                                 (HF.common_udf('sn_set_cen', 'sn_set_ccs') >= 1))


# ------------------------------------------------------------------------------------------------------------ #
# ------ MK3: UPRN + 2 of Tenure, Type, Rescount + (One Common Surname OR Common Forename) + Common DOB ------ #
# ------------------------------------------------------------------------------------------------------------ # 
  
  
# MK3
matches_3 = CEN.join(CCS, on = [CEN.uprn_cen == CCS.uprn_ccs,
                             (((CEN.tenure_cen == CCS.tenure_ccs) & (CEN.typaccom_cen == CCS.typaccom_ccs)) | 
                              ((CEN.tenure_cen == CCS.tenure_ccs) & (CEN.no_resi_cen == CCS.no_resi_ccs))   | 
                              ((CEN.no_resi_cen == CCS.no_resi_ccs) & (CEN.typaccom_cen == CCS.typaccom_ccs)))], how = 'inner')
  
matches_3 = matches_3.filter(((HF.common_udf('fn_set_cen', 'fn_set_ccs') >= 1) | (HF.common_udf('sn_set_cen', 'sn_set_ccs') >= 1)) &
                              (HF.common_udf('dob_set_cen', 'dob_set_ccs') >= 1))
  

# ------------------------------------------------------------------------------------- #
# ----------------- MK4: UPRN + One Common Surname + One Common DOB ------------------- #
# ------------------------------------------------------------------------------------- #  


# MK4
matches_4 = UPRN_Join.filter((HF.common_udf('sn_set_cen', 'sn_set_ccs')   >= 1) & 
                             (HF.common_udf('dob_set_cen', 'dob_set_ccs') >= 1))


# ----------------------------------------------------------------------------------- #
# -------- MK5: UPRN + Max FN LEV > 0.80 + Max SN LEV > 0.80 + Common DOB ----------- #
# ----------------------------------------------------------------------------------- #
  
  
# MK5
matches_5 = UPRN_Join.filter((HF.max_lev_udf('fn_set_cen', 'fn_set_ccs') > 0.80) & 
                             (HF.max_lev_udf('sn_set_cen', 'sn_set_ccs') > 0.80) & 
                             (HF.common_udf('dob_set_cen', 'dob_set_ccs') >= 1))


# ----------------------------------------------------------------------------- #
# ------ MK6: UPRN + Max FN JAR > 0.80 + Max SN JAR > 0.80 + Common DOB ------- #
# ----------------------------------------------------------------------------- #
  
  
# MK6
matches_6 = UPRN_Join.filter((HF.max_jaro_udf('fn_set_cen', 'fn_set_ccs') > 0.80) & 
                             (HF.max_jaro_udf('sn_set_cen', 'sn_set_ccs') > 0.80) & 
                             (HF.common_udf('dob_set_cen', 'dob_set_ccs') >= 1))


# ---------------------------------------------------------------------------------------------------------------------------------- #
# -- MK7: UPRN + Max FN LEV > 0.60 + Max SN LEV > 0.60 + Common DOB (or 2 common DOB if 4 + people) + TYPE + TENURE + RESCOUNT ----- #
# ---------------------------------------------------------------------------------------------------------------------------------- #
 
  
# MK7
matches_7 = UPRN_TTR_Join.filter((HF.max_lev_udf('fn_set_cen', 'fn_set_ccs') > 0.60) & 
                                 (HF.max_lev_udf('sn_set_cen', 'sn_set_ccs') > 0.60) & 
                                 (HF.common_udf('dob_set_cen', 'dob_set_ccs') >= 1))

# 2 Common DOB required if 4+ people
matches_7 = matches_7.filter(~((HF.common_udf('dob_set_cen', 'dob_set_ccs') == 1) & (matches_7.hh_size_cen >= 4) & (matches_7.hh_size_ccs >= 4)))


# ---------------------------------------------------------------------------------------------------------------------------------- #
# -- MK8: UPRN + Max FN JARO > 0.60 + Max SN JARO > 0.60 + Common DOB (or 2 common DOB if 4 + people) + TYPE + TENURE + RESCOUNT --- #
# ---------------------------------------------------------------------------------------------------------------------------------- #
  

# MK8
matches_8 = UPRN_TTR_Join.filter((HF.max_jaro_udf('fn_set_cen', 'fn_set_ccs') > 0.60) & 
                                 (HF.max_jaro_udf('sn_set_cen', 'sn_set_ccs') > 0.60) & 
                                 (HF.common_udf('dob_set_cen', 'dob_set_ccs') >= 1))

# 2 Common DOB required if 4+ people
matches_8 = matches_8.filter(~((HF.common_udf('dob_set_cen', 'dob_set_ccs') == 1) & (matches_8.hh_size_cen >= 4) & (matches_8.hh_size_ccs >= 4)))  


# ------------------------------------------------------------------------------------------------------ #
# --- MK9: UPRN + (Max FN LEV > 0.80 OR Max SN LEV > 0.80) + Common DOB + TYPE + TENURE + RESCOUNT ----- #
# ------------------------------------------------------------------------------------------------------ #


# MK9
matches_9 = UPRN_TTR_Join.filter(((HF.max_lev_udf('fn_set_cen', 'fn_set_ccs') > 0.80) | (HF.max_lev_udf('sn_set_cen', 'sn_set_ccs') > 0.80)) & 
                                  (HF.common_udf('dob_set_cen', 'dob_set_ccs') >= 1))


# ------------------------------------------------------------------------------------------------------------ #
# ------ MK10: Single Person HH, UPRN, 2 of TYPE,TENURE,RESCOUNT, Equal DOB, Max FN or SN Lev > 0.50  --------- #
# ------------------------------------------------------------------------------------------------------------ #
  

# MK10
matches_10 = UPRN_Join.filter(((HF.max_lev_udf('fn_set_cen', 'fn_set_ccs') > 0.50) | (HF.max_lev_udf('sn_set_cen', 'sn_set_ccs') > 0.50)) & 
                             (((UPRN_Join.tenure_cen == UPRN_Join.tenure_ccs)   & (UPRN_Join.typaccom_cen == UPRN_Join.typaccom_ccs)) | 
                              ((UPRN_Join.tenure_cen == UPRN_Join.tenure_ccs)   & (UPRN_Join.no_resi_cen  == UPRN_Join.no_resi_ccs)) | 
                              ((UPRN_Join.no_resi_cen == UPRN_Join.no_resi_ccs) & (UPRN_Join.typaccom_cen == UPRN_Join.typaccom_ccs))) & 
                               (UPRN_Join.dob_set_cen == UPRN_Join.dob_set_ccs))
                                 
# Single Person HHs
matches_10 = matches_10.filter((matches_10.hh_size_cen == 1) & (matches_10.hh_size_ccs == 1)) 


# ------------------------------------------------------------------------------------------------------------ #
# -------- MK11: Single Person HH, UPRN, 2 of TYPE,TENURE,RESCOUNT, Equal DOB, FN or SN Jaro > 0.60  --------- #
# ------------------------------------------------------------------------------------------------------------ #
  

# MK11
matches_11 = UPRN_Join.filter(((HF.max_jaro_udf('fn_set_cen', 'fn_set_ccs') > 0.60) | (HF.max_jaro_udf('sn_set_cen', 'sn_set_ccs') > 0.60)) & 
                             (((UPRN_Join.tenure_cen == UPRN_Join.tenure_ccs)   & (UPRN_Join.typaccom_cen == UPRN_Join.typaccom_ccs)) | 
                              ((UPRN_Join.tenure_cen == UPRN_Join.tenure_ccs)   & (UPRN_Join.no_resi_cen  == UPRN_Join.no_resi_ccs)) | 
                              ((UPRN_Join.no_resi_cen == UPRN_Join.no_resi_ccs) & (UPRN_Join.typaccom_cen == UPRN_Join.typaccom_ccs))) & 
                               (UPRN_Join.dob_set_cen == UPRN_Join.dob_set_ccs))
                                 
# Single Person HHs
matches_11 = matches_11.filter((matches_11.hh_size_cen == 1) & (matches_11.hh_size_ccs == 1)) 


# ------------------------------------------------------------------------------------------------ #
# ------- MK12: Single Person HH, UPRN, 2 of TYPE,TENURE,RESCOUNT, Max FN or SN Lev > 0.80 ------- #
# ------------------------------------------------------------------------------------------------ #
  
  
# MK12
matches_12 = UPRN_Join.filter(((HF.max_lev_udf('fn_set_cen', 'fn_set_ccs') > 0.80) | (HF.max_lev_udf('sn_set_cen', 'sn_set_ccs') > 0.80)) & 
                             (((UPRN_Join.tenure_cen == UPRN_Join.tenure_ccs)   & (UPRN_Join.typaccom_cen == UPRN_Join.typaccom_ccs)) | 
                              ((UPRN_Join.tenure_cen == UPRN_Join.tenure_ccs)   & (UPRN_Join.no_resi_cen  == UPRN_Join.no_resi_ccs)) | 
                              ((UPRN_Join.no_resi_cen == UPRN_Join.no_resi_ccs) & (UPRN_Join.typaccom_cen == UPRN_Join.typaccom_ccs))))
                                 
# Single Person HHs
matches_12 = matches_12.filter((matches_12.hh_size_cen == 1) & (matches_12.hh_size_ccs == 1)) 

  
# ------------------------------------------------------------------------------------------------ #
# -------- MK13: Single Person HH, UPRN, 2 of TYPE,TENURE,RESCOUNT, Max FN or SN Jaro > 0.90 ----- #
# ------------------------------------------------------------------------------------------------ #
  

# MK13
matches_13 = UPRN_Join.filter(((HF.max_jaro_udf('fn_set_cen', 'fn_set_ccs') > 0.90) | (HF.max_jaro_udf('sn_set_cen', 'sn_set_ccs') > 0.90)) & 
                             (((UPRN_Join.tenure_cen == UPRN_Join.tenure_ccs)   & (UPRN_Join.typaccom_cen == UPRN_Join.typaccom_ccs)) | 
                              ((UPRN_Join.tenure_cen == UPRN_Join.tenure_ccs)   & (UPRN_Join.no_resi_cen  == UPRN_Join.no_resi_ccs)) | 
                              ((UPRN_Join.no_resi_cen == UPRN_Join.no_resi_ccs) & (UPRN_Join.typaccom_cen == UPRN_Join.typaccom_ccs))))
                                 
# Single Person HHs
matches_13 = matches_13.filter((matches_13.hh_size_cen == 1) & (matches_13.hh_size_ccs == 1)) 
                               

# ------------------------------------------------------------------------------------- #
# ------ MK14: PC + One Common Surname + One Common Forename + One Common DOB --------- #
# ------------------------------------------------------------------------------------- #  


# MK14
matches_14 = PC_Join.filter((HF.common_udf('fn_set_cen', 'fn_set_ccs') >= 1) & 
                            (HF.common_udf('sn_set_cen', 'sn_set_ccs') >= 1) & 
                            (HF.common_udf('dob_set_cen', 'dob_set_ccs') >= 1))


# ----------------------------------------------------------------------------------------------- #
# ----- MK15: PC + Tenure + TYPE + RESCOUNT + (One Common Surname AND One Common Forename) ------ #
# ----------------------------------------------------------------------------------------------- #


# MK15
matches_15 = PC_TTR_Join.filter((HF.common_udf('fn_set_cen', 'fn_set_ccs') >= 1) &
                                (HF.common_udf('sn_set_cen', 'sn_set_ccs') >= 1))


# ------------------------------------------------------------------------------------------------------------ #
# -------- MK16: PC + 2 of Tenure, TYPE, RESCOUNT + (One Common Surname OR Common Forename) + Common DOB ----- #
# ------------------------------------------------------------------------------------------------------------ # 
  
  
# MK16
matches_16 = CEN.join(CCS, on = [CEN.pc_cen == CCS.pc_ccs,
                              (((CEN.tenure_cen == CCS.tenure_ccs) & (CEN.typaccom_cen == CCS.typaccom_ccs)) | 
                               ((CEN.tenure_cen == CCS.tenure_ccs) & (CEN.no_resi_cen == CCS.no_resi_ccs))   | 
                               ((CEN.no_resi_cen == CCS.no_resi_ccs) & (CEN.typaccom_cen == CCS.typaccom_ccs)))], how = 'inner')
  
matches_16 = matches_16.filter(((HF.common_udf('fn_set_cen', 'fn_set_ccs') >= 1) | (HF.common_udf('sn_set_cen', 'sn_set_ccs') >= 1)) &
                                (HF.common_udf('dob_set_cen', 'dob_set_ccs') >= 1))


# -------------------------------------------------------------------------------------------------------------------------------------- #
# --- MK17: PC + (Max FN LEV > 0.80 OR Max SN LEV > 0.80) + Common DOB (or 2 common DOB if 3+ people) + TYPE + TENURE + RESCOUNT ------- #
# -------------------------------------------------------------------------------------------------------------------------------------- #
  

# MK17
matches_17 = PC_TTR_Join.filter(((HF.max_lev_udf('fn_set_cen', 'fn_set_ccs') > 0.80) |
                                 (HF.max_lev_udf('sn_set_cen', 'sn_set_ccs') > 0.80)) & 
                                 (HF.common_udf('dob_set_cen', 'dob_set_ccs') >= 1))

# 2 Common DOB required if 4+ people
matches_17 = matches_17.filter(~((HF.common_udf('dob_set_cen', 'dob_set_ccs') == 1) & (matches_17.hh_size_cen >= 3) & (matches_17.hh_size_ccs >= 3)))


# -------------------------------------------------------------------------------------------------------------------------------------- #
# ---- MK18: PC + (Max FN JARO > 0.80 OR Max SN JARO > 0.80) + Common DOB (or 2 common DOB if 3+ people) + TYPE + TENURE + RESCOUNT ---- #
# -------------------------------------------------------------------------------------------------------------------------------------- #
  

# MK18
matches_18 = PC_TTR_Join.filter(((HF.max_jaro_udf('fn_set_cen', 'fn_set_ccs') > 0.80) |
                                 (HF.max_jaro_udf('sn_set_cen', 'sn_set_ccs') > 0.80)) & 
                                 (HF.common_udf('dob_set_cen', 'dob_set_ccs') >= 1))

# 2 Common DOB required if 4+ people
matches_18 = matches_18.filter(~((HF.common_udf('dob_set_cen', 'dob_set_ccs') == 1) & (matches_18.hh_size_cen >= 3) & (matches_18.hh_size_ccs >= 3)))


# ----------------------------------------------------------- #
# ---- MK19: PC + All Surnames and All Forenames Match ------ #
# ----------------------------------------------------------- #  


# MK19
matches_19 = CEN.join(CCS, on = [(CEN.pc_cen == CCS.pc_ccs) & (CEN.sn_set_cen == CCS.sn_set_ccs) & (CEN.fn_set_cen == CCS.fn_set_ccs)], how = 'inner')


# --------------------------------------------------------------------------------------- #
# --- MK20: PC, House No, Common FN, Common SN, Common Age, 2 of TYPE,TENURE,RESCOUNT --- #
# --------------------------------------------------------------------------------------- #  
  
  
# MK20
matches_20 = PC_Join.filter((PC_Join.house_no_cen == PC_Join.house_no_ccs) &
                           (((PC_Join.tenure_cen == PC_Join.tenure_ccs)   & (PC_Join.typaccom_cen == PC_Join.typaccom_ccs)) | 
                            ((PC_Join.tenure_cen == PC_Join.tenure_ccs)   & (PC_Join.no_resi_cen  == PC_Join.no_resi_ccs)) | 
                            ((PC_Join.no_resi_cen == PC_Join.no_resi_ccs) & (PC_Join.typaccom_cen == PC_Join.typaccom_ccs))) & 
                            ((HF.common_udf('fn_set_cen', 'fn_set_ccs') >= 1) & (HF.common_udf('sn_set_cen', 'sn_set_ccs') >= 1) & 
                             (HF.common_age_udf('age_set_cen', 'age_set_ccs') >= 1)))


# -------------------------------------------------------------------- #
# ---------- MK21: UPRN + (Two of Common FN / SN / DOB / Age) -------- #
# -------------------------------------------------------------------- #  


# MK21
matches_21 = UPRN_Join.filter(((HF.common_udf('fn_set_cen', 'fn_set_ccs') >= 1)   & (HF.common_udf('sn_set_cen',  'sn_set_ccs')  >= 1)) |
                              ((HF.common_udf('fn_set_cen', 'fn_set_ccs') >= 1)   & (HF.common_udf('dob_set_cen', 'dob_set_ccs') >= 1)) | 
                              ((HF.common_udf('fn_set_cen', 'fn_set_ccs') >= 1)   & (HF.common_age_udf('age_set_cen', 'age_set_ccs') >= 1)) | 
                              ((HF.common_udf('sn_set_cen', 'sn_set_ccs') >= 1)   & (HF.common_udf('dob_set_cen', 'dob_set_ccs') >= 1)) | 
                              ((HF.common_udf('sn_set_cen', 'sn_set_ccs') >= 1)   & (HF.common_udf('age_set_cen', 'age_set_ccs') >= 1)))


# --------------------------------------------------------------------------- #
# ----------------- MK22: UPRN + JARO FN + JARO SN + SIZE ------------------- #
# --------------------------------------------------------------------------- #  


# MK22
matches_22 = UPRN_Join.filter((HF.max_jaro_udf('fn_set_cen', 'fn_set_ccs') > 0.80) &
                              (HF.max_jaro_udf('sn_set_cen', 'sn_set_ccs') > 0.80) & 
                              (UPRN_Join.hh_size_cen == UPRN_Join.hh_size_ccs))


# ------------------------------------------------------------------------------- #
# ---- MK23: PC SECTOR + HN + Common FN + Common SN + 2X Common DOB + SIZE  ----- #
# ------------------------------------------------------------------------------- #


# MK23
matches_23 = SECT_HN_Join.filter((HF.common_udf('fn_set_cen', 'fn_set_ccs') >= 1) & 
                                 (HF.common_udf('sn_set_cen', 'sn_set_ccs') >= 1) & 
                                 (HF.common_udf('dob_set_cen', 'dob_set_ccs') >= 2) & 
                                 (SECT_HN_Join.hh_size_cen == SECT_HN_Join.hh_size_ccs))


# ------------------------------------------------------------------ #
# ----------------- MK24: UPRN + SIZE + ALL AGES ------------------- #
# ------------------------------------------------------------------ #

# MK24
matches_24 = UPRN_Join.filter((UPRN_Join.age_set_cen == UPRN_Join.age_set_ccs) &
                              (UPRN_Join.hh_size_cen == UPRN_Join.hh_size_ccs))


# ------------------------------------------------------------------------- #
# -------- MK25: PC + HN + FLAT + (Common FN OR SN OR DOB) + SIZE  -------- #
# ------------------------------------------------------------------------- #


# MK25
matches_25 = PC_Join.filter((PC_Join.flat_no_cen == PC_Join.flat_no_ccs) &
                            (PC_Join.house_no_cen == PC_Join.house_no_ccs) &
                            (PC_Join.hh_size_cen  == PC_Join.hh_size_ccs) &
                           ((HF.common_udf('fn_set_cen', 'fn_set_ccs') >= 1) | 
                            (HF.common_udf('dob_set_cen', 'dob_set_ccs') >= 1) | 
                            (HF.common_udf('sn_set_cen', 'sn_set_ccs') >= 1)))


# --------------------------------------------------------------- #
# -------- MK26: PC + FLAT + (Common FN AND DOB) + SIZE  -------- #
# --------------------------------------------------------------- #


# MK26
matches_26 = PC_Join.filter((PC_Join.flat_no_cen == PC_Join.flat_no_ccs) &
                            (PC_Join.hh_size_cen == PC_Join.hh_size_ccs) &
                            (HF.common_udf('fn_set_cen', 'fn_set_ccs') >= 1) &
                            (HF.common_udf('dob_set_cen', 'dob_set_ccs') >= 1))


# --------------------------------------------------------------- #
# -------- MK27: PC + FLAT + (Common SN AND DOB) + SIZE  -------- #
# --------------------------------------------------------------- #


# MK27
matches_27 = PC_Join.filter((PC_Join.flat_no_cen == PC_Join.flat_no_ccs) &
                            (PC_Join.hh_size_cen == PC_Join.hh_size_ccs) &
                            (HF.common_udf('sn_set_cen', 'sn_set_ccs') >= 1) &
                            (HF.common_udf('dob_set_cen', 'dob_set_ccs') >= 1))


# -------------------------------------------------------------- #
# ------ MK28: UPRN + (Common FN OR SN OR DOB) + SIZE = 1  ----- #
# -------------------------------------------------------------- #


# MK28
matches_28 = UPRN_Join.filter((PC_Join.hh_size_cen  == 1) &
                              (PC_Join.hh_size_ccs  == 1) &
                             ((HF.common_udf('fn_set_cen', 'fn_set_ccs') >= 1) | 
                              (HF.common_udf('dob_set_cen', 'dob_set_ccs') >= 1) | 
                              (HF.common_udf('sn_set_cen', 'sn_set_ccs') >= 1)))


# ----------------------------------------------------------------- #
# ------ MK29: PC + HN + (Common FN OR SN OR DOB) + SIZE = 1  ----- #
# ----------------------------------------------------------------- #


# MK29
matches_29 = PC_Join.filter((PC_Join.house_no_cen == PC_Join.house_no_ccs) &
                            (PC_Join.hh_size_cen  == 1) &
                            (PC_Join.hh_size_ccs  == 1) &
                           ((HF.common_udf('fn_set_cen', 'fn_set_ccs') >= 1) | 
                            (HF.common_udf('dob_set_cen', 'dob_set_ccs') >= 1) | 
                            (HF.common_udf('sn_set_cen', 'sn_set_ccs') >= 1)))


# --------------------------------------------------------------------------- #
# ------ MK30: UPRN + COMMON AGE + JARO FN + JARO SN + NO FLAT NUMBERS  ----- #
# --------------------------------------------------------------------------- #


# MK30
matches_30 = UPRN_Join.filter((PC_Join.flat_no_cen.isNull() == True) & 
                              (PC_Join.flat_no_ccs.isNull() == True) & 
                              (HF.common_age_udf('age_set_cen', 'age_set_ccs') >= 1) & 
                              (HF.max_jaro_udf('fn_set_cen', 'fn_set_ccs') > 0.80) &                            
                              (HF.max_jaro_udf('sn_set_cen', 'sn_set_ccs') > 0.70))      


# ------------------------------------------------------------------------------ #
# ------ MK31: PC + HN + COMMON AGE + JARO FN + JARO SN + NO FLAT NUMBERS  ----- #
# ------------------------------------------------------------------------------ #


# MK31
matches_31 = PC_Join.filter((PC_Join.house_no_cen == PC_Join.house_no_ccs) &
                            (PC_Join.flat_no_cen.isNull() == True) & 
                            (PC_Join.flat_no_ccs.isNull() == True) & 
                            (HF.common_age_udf('age_set_cen', 'age_set_ccs') >= 1) & 
                            (HF.max_jaro_udf('fn_set_cen', 'fn_set_ccs') > 0.80) &                            
                            (HF.max_jaro_udf('sn_set_cen', 'sn_set_ccs') > 0.70)) 


# ------------------------------------------------------------------------------------- #
# --------------------------- Combine and Dedup Matchkeys ----------------------------- #
# ------------------------------------------------------------------------------------- #  
  

# Set of matches
all_matches = [matches_1,  matches_2,  matches_3,  matches_4,
               matches_5,  matches_6,  matches_7,  matches_8,
               matches_9,  matches_10, matches_11, matches_12,
               matches_13, matches_14, matches_15, matches_16,
               matches_17, matches_18, matches_19, matches_20,
               matches_21, matches_22, matches_23, matches_24,
               matches_25, matches_26, matches_27, matches_28,
               matches_29, matches_30, matches_31]

# Schema for empty dataframe to collect links
schema = StructType([StructField("hh_id_cen", LongType(), True),
                     StructField("hh_id_ccs", LongType(), True),
                     StructField("KEY",       IntegerType(), True)])

# Create empty dataframe to collect unique links
matches = sparkSession.createDataFrame([],schema)
    
# Loop to combine all unique matches from mkeys
for i, df in enumerate(all_matches, 1):
  
  # Select cols
  df = df.select('hh_id_cen', 'hh_id_ccs')
  
  # Key number 
  df = df.withColumn('KEY', lit(i))
                     
  # Append pairs to final dataset
  matches = matches.union(df)
                       
  # Minimum blocking pass for every unique pair
  matches = matches.withColumn('Min_Key', F.min('KEY').over(Window.partitionBy('hh_id_cen', 'hh_id_ccs')))
                 
  # Remove duplicates whilst keeping duplicate from best mkey
  matches = matches.filter(matches.KEY == matches.Min_Key).drop('Min_Key')

  # Count update
  matches.persist()
  print(matches.count())

# Counts
matches.groupBy('KEY').count().sort(F.asc('KEY')).show(30)


# ----------------------------------- #
# -------------- SAVE --------------- #
# ----------------------------------- #


# Save all matches
matches.write.mode('overwrite').parquet(FILE_PATH('Stage_3_All_HH_Matches'))

sparkSession.stop()
