'''
This module contains various parameters used to set:
* Filepaths for inputs/outputs
* Deterministic person matchkeys (and rules applied to them)
* probabilistic and associative matching thresholds
* blocking pass conditions
'''
from pyspark.sql.functions import col, levenshtein, soundex, length, greatest, abs as abs_


# Final run indicator
def FINAL_RUN():
  
  # Before starting final run, set value to True
  value = True
  
  return value 


# -------------------------------------- #
# ---- FILE PATH FOR SAVING OUTPUTS ---- #
# -------------------------------------- #


def FILE_PATH(string, csv=None):
    '''
    Function to create filepath for inputs and outputs.
    
    Parameters
    ----------
    string: a string representing the input/output file name.
    
    csv: a boolean value that returns a parquet format filepath if False or None,
    and csv if True.
    '''
    
    # DATE
    Y, M, D = '2021', '11', '16'
    
    # Parquet / CSV
    if csv is None:
      file_path = 'some_path' + '/collaborative_method_matching/{}/{}/{}/{}.parquet'.format(
          Y, M, D, string)
      
    else:
      file_path = 'some_path' + '/collaborative_method_matching/{}/{}/{}/{}.csv'.format(
          Y, M, D, string)
      
    return file_path


# ------------------------------------ #
# --------- PERSON MATCHKEYS --------- #
# ------------------------------------ #

# Levenstein Score Function
def lev_score(x, y):
    score = (1 - ((levenshtein(col(x), col(y))) /
                  greatest(length(col(x)), length(col(y)))))
    return score


def MATCHKEYS(df1, df2):
    '''
    These are the matchkeys used to deterministically match people records to each other,
    based on the specified variables. These require either exact agreement or some element
    of fuzzyness.

    Some matchkeys use custom User-Defined Jaro distance functions, these are specified
    in Jaro_Keys and reference to this has been made in the associated matchkeys themselves.

    Parameters
      ----------
      df1/df2: dataframe
          dataframes containing person records to be matched based on variables specified
          in matchkeys

    Returns
    -------
    keys
        a list of all matchkeys
    '''
    # MK1 FullnameNS, DOB, Sex, PC
    cond01 = [df1.fullname_ns_ccs == df2.fullname_ns_cen,
              df1.dob_ccs == df2.dob_cen,
              df1.sex_ccs == df2.sex_cen,
              df1.pc_ccs == df2.pc_cen]

    # MK2 = FN, SN, DOB, Sex, UPRN
    cond02 = [((df1.fn1_ccs == df2.fn1_cen) | (df1.fn_ccs == df2.fn_cen)),
              ((df1.sn1_ccs == df2.sn1_cen) | (df1.sn_ccs == df2.sn_cen)),
              df1.dob_ccs == df2.dob_cen,
              df1.sex_ccs == df2.sex_cen,
              df1.uprn_ccs == df2.uprn_cen]

    # MK3 = FN, SN, DOB, Sex, PC
    cond03 = [((df1.fn1_ccs == df2.fn1_cen) | (df1.fn_ccs == df2.fn_cen)),
              ((df1.sn1_ccs == df2.sn1_cen) | (df1.sn_ccs == df2.sn_cen)),
              df1.dob_ccs == df2.dob_cen,
              df1.sex_ccs == df2.sex_cen,
              df1.pc_ccs == df2.pc_cen]

    # MK4 Alphaname, DOB, Sex, PC
    cond04 = [df1.alphaname_ccs == df2.alphaname_cen,
              df1.dob_ccs == df2.dob_cen,
              df1.sex_ccs == df2.sex_cen,
              df1.pc_ccs == df2.pc_cen]

    # MK5 FN Nickname, SN, DOB, Sex, PC
    cond05 = [df1.fn1_nickname_ccs == df2.fn1_nickname_cen,
              ((df1.sn1_ccs == df2.sn1_cen) | (df1.sn_ccs == df2.sn_cen)),
              df1.dob_ccs == df2.dob_cen,
              df1.sex_ccs == df2.sex_cen,
              df1.pc_ccs == df2.pc_cen]

    # MK6 FN Nickname, SN, AGE, Sex, UPRN
    cond06 = [df1.fn1_nickname_ccs == df2.fn1_nickname_cen,
              ((df1.sn1_ccs == df2.sn1_cen) | (df1.sn_ccs == df2.sn_cen)),
              df1.age_ccs == df2.age_cen,
              df1.sex_ccs == df2.sex_cen,
              df1.uprn_ccs == df2.uprn_cen]

    # MK7 Contained Name, DOB, Sex, PC+HN / UPRN
    cond07 = [df1.dob_ccs == df2.dob_cen,
              df1.sex_ccs == df2.sex_cen,
              ((df1.uprn_ccs == df2.uprn_cen) |
               ((df1.pc_ccs == df2.pc_cen) & (df1.house_no_ccs == df2.house_no_cen)))]
              # Plus Contained Name]

    # MK8 Lev Fullname, DOB, Sex, PC
    cond08 = [levenshtein(df1.fullname_ns_ccs, df2.fullname_ns_cen) < 3,
              df1.dob_ccs == df2.dob_cen,
              df1.sex_ccs == df2.sex_cen,
              df1.pc_ccs == df2.pc_cen]

    # MK9 Soundex FN, Soundex SN, DOB, Sex, PC
    cond09 = [soundex(df1.fn1_ccs) == soundex(df2.fn1_cen),
              soundex(df1.sn1_ccs) == soundex(df2.sn1_cen),
              df1.dob_ccs == df2.dob_cen,
              df1.sex_ccs == df2.sex_cen,
              df1.pc_ccs == df2.pc_cen]

    # MK10 Allowing for error only in Sex
    cond10 = [((df1.fn1_ccs == df2.fn1_cen) | (df1.fn_ccs == df2.fn_cen)),
              ((df1.sn1_ccs == df2.sn1_cen) | (df1.sn_ccs == df2.sn_cen)),
              df1.dob_ccs == df2.dob_cen,
              df1.pc_ccs == df2.pc_cen]

    # MK11 Allowing for error in name (strict Jaro)
    cond11 = [# JARO(df1.fn1_ccs, df2.fn1_cen) > 0.9,
              # JARO(df1.sn1_ccs, df2.sn1_cen) > 0.8,
              df1.dob_ccs == df2.dob_cen,
              df1.sex_ccs == df2.sex_cen,
              df1.pc_ccs == df2.pc_cen]

    # MK12 Allowing for error in Name (Levenstein)
    cond12 = [lev_score('fn1_ccs', 'fn1_cen') > 0.60,
              lev_score('sn1_ccs', 'sn1_cen') > 0.60,
              df1.dob_ccs == df2.dob_cen,
              df1.sex_ccs == df2.sex_cen,
              df1.pc_ccs == df2.pc_cen]

    # MK13 Lev FN-SN & SN-FN, DOB, Sex, PC
    cond13 = [lev_score('fn1_ccs', 'sn1_cen') > 0.60,
              lev_score('sn1_ccs', 'fn1_cen') > 0.60,
              df1.dob_ccs == df2.dob_cen,
              df1.sex_ccs == df2.sex_cen,
              df1.pc_ccs == df2.pc_cen]

    # MK14 FN2, SN, DOB, Sex, PC (CLERICAL MATCHKEY)
    cond14 = [df1.fn2_ccs == df2.fn2_cen,
              ((df1.sn1_ccs == df2.sn1_cen) | (df1.sn_ccs == df2.sn_cen)),
              df1.dob_ccs == df2.dob_cen,
              df1.sex_ccs == df2.sex_cen,
              df1.pc_ccs == df2.pc_cen]

    # MK15 Lev Edit Distance FN1, lev Edit Distance FN2, DOB, Sex, PC
    cond15 = [lev_score('fn1_ccs', 'fn1_cen') > 0.60,
              lev_score('fn2_ccs', 'fn2_cen') > 0.60,
              df1.dob_ccs == df2.dob_cen,
              df1.sex_ccs == df2.sex_cen,
              df1.pc_ccs == df2.pc_cen]

    # MK16 Swapped FNs, SN, DOB, PC
    cond16 = [((df1.fn1_ccs == df2.fn2_cen) | (df1.fn2_ccs == df2.fn1_cen)),
              ((df1.sn1_ccs == df2.sn1_cen) | (df1.sn_ccs == df2.sn_cen)),
              df1.dob_ccs == df2.dob_cen,
              df1.sex_ccs == df2.sex_cen,
              ((df1.uprn_ccs == df2.uprn_cen) |
               ((df1.pc_ccs == df2.pc_cen) & (df1.house_no_ccs == df2.house_no_cen)))]

    # MK17 FN, DOB, Sex, (UPRN or PC + HN)
    cond17 = [((df1.fn1_ccs == df2.fn1_cen) | (df1.fn_ccs == df2.fn_cen)),
              df1.dob_ccs == df2.dob_cen,
              df1.sex_ccs == df2.sex_cen,
              ((df1.uprn_ccs == df2.uprn_cen) |
               ((df1.pc_ccs == df2.pc_cen) & (df1.house_no_ccs == df2.house_no_cen)))]

    # MK18 FN Lev > 0.5, SN, DOB, Sex, (UPRN or PC + HN)
    cond18 = [lev_score('fn1_ccs', 'fn1_cen') > 0.60,
              ((df1.sn1_ccs == df2.sn1_cen) | (df1.sn_ccs == df2.sn_cen)),
              df1.dob_ccs == df2.dob_cen,
              df1.sex_ccs == df2.sex_cen,
              ((df1.uprn_ccs == df2.uprn_cen) |
               ((df1.pc_ccs == df2.pc_cen) & (df1.house_no_ccs == df2.house_no_cen)))]

    # MK19 FN Bigram, SN, DOB, Sex, (UPRN or PC + HN)
    cond19 = [df1.fn1_ccs.substr(1, 2) == df2.fn1_cen.substr(1, 2),
              ((df1.sn1_ccs == df2.sn1_cen) | (df1.sn_ccs == df2.sn_cen)),
              df1.dob_ccs == df2.dob_cen,
              df1.sex_ccs == df2.sex_cen,
              ((df1.uprn_ccs == df2.uprn_cen) |
               ((df1.pc_ccs == df2.pc_cen) & (df1.house_no_ccs == df2.house_no_cen)))]

    # MK20 FN, SN, PC, SEX, AGE
    cond20 = [((df1.fn1_ccs == df2.fn1_cen) | (df1.fn_ccs == df2.fn_cen)),
              ((df1.sn1_ccs == df2.sn1_cen) | (df1.sn_ccs == df2.sn_cen)),
              df1.age_ccs == df2.age_cen,
              df1.sex_ccs == df2.sex_cen,
              df1.pc_ccs == df2.pc_cen]

    # MK21 FN, SN, PC, Sex, Day, Month, Year within 10
    cond21 = [((df1.fn1_ccs == df2.fn1_cen) | (df1.fn_ccs == df2.fn_cen)),
              ((df1.sn1_ccs == df2.sn1_cen) | (df1.sn_ccs == df2.sn_cen)),
              df1.sex_ccs == df2.sex_cen,
              df1.day_ccs == df2.day_cen,
              df1.mon_ccs == df2.mon_cen,
              df1.pc_ccs == df2.pc_cen,
              (abs_(df1.year_ccs - df2.year_cen) < 11)]

    # MK22 FN, SN, PC, Sex, Year
    cond22 = [((df1.fn1_ccs == df2.fn1_cen) | (df1.fn_ccs == df2.fn_cen)),
              ((df1.sn1_ccs == df2.sn1_cen) | (df1.sn_ccs == df2.sn_cen)),
              df1.sex_ccs == df2.sex_cen,
              df1.year_ccs == df2.year_cen,
              df1.pc_ccs == df2.pc_cen]

    # MK23 Jaro FN, Jaro SN, Lev DOB < 2, Sex, UPRN
    cond23 = [# JARO(df1.fn1_ccs, df2.fn1_cen) > 0.8,
              # JARO(df1.sn1_ccs, df2.sn1_cen) > 0.8,
              levenshtein(df1.dob_ccs, df2.dob_cen) < 2,
              df1.sex_ccs == df2.sex_cen,
              df1.uprn_ccs == df2.uprn_cen]

    # MK24 Lev FN, Lev SN, Lev DOB < 2, Sex, UPRN
    cond24 = [lev_score('fn1_ccs', 'fn1_cen') > 0.60,
              lev_score('sn1_ccs', 'sn1_cen') > 0.60,
              levenshtein(df1.dob_ccs, df2.dob_cen) < 2,
              df1.sex_ccs == df2.sex_cen,
              df1.uprn_ccs == df2.uprn_cen]

    # MK25 FN Lev, SN Lev, Age diff < 2 years, Sex, UPRN
    cond25 = [lev_score('fn1_ccs', 'fn1_cen') > 0.60,
              lev_score('sn1_ccs', 'sn1_cen') > 0.60,
              abs_(df1.age_ccs - df2.age_cen) < 2,
              df1.sex_ccs == df2.sex_cen,
              df1.uprn_ccs == df2.uprn_cen]

    # MK26 FN Lev, SN Lev, Age diff < 2 years, Sex, PC + HN
    cond26 = [lev_score('fn1_ccs', 'fn1_cen') > 0.60,
              lev_score('sn1_ccs', 'sn1_cen') > 0.60,
              abs_(df1.age_ccs - df2.age_cen) < 2,
              df1.sex_ccs == df2.sex_cen,
              ((df1.pc_ccs == df2.pc_cen) & (df1.house_no_ccs == df2.house_no_cen))]

    # MK27 Lev FN, Lev SN, Sex Agree/Missing, DOB, UPRN
    cond27 = [lev_score('fn1_ccs', 'fn1_cen') > 0.60,
              lev_score('sn1_ccs', 'sn1_cen') > 0.60,
              ((df1.sex_ccs == df2.sex_cen) |
               (df1.sex_ccs.isNull()) | (df2.sex_cen.isNull())),
              df1.dob_ccs == df2.dob_cen,
              df1.uprn_ccs == df2.uprn_cen]

    # MK28 Multiple Moves (PC+HN / UPRN) (used to be 30)
    cond28 = [((df1.fn1_ccs == df2.fn1_cen) | (df1.fn_ccs == df2.fn_cen)),
              ((df1.sn1_ccs == df2.sn1_cen) | (df1.sn_ccs == df2.sn_cen)),
              df1.dob_ccs == df2.dob_cen,
              df1.sex_ccs == df2.sex_cen]
              # Plus Multiple moves]

    # MK29 FN1, SN1, DOB, Sex, Postcode (minus last character)
    cond29 = [df1.fn1_ccs == df2.fn1_cen,
              df1.sn1_ccs == df2.sn1_cen,
              df1.dob_ccs == df2.dob_cen,
              df1.sex_ccs == df2.sex_cen,
              df1.pc_substr_ccs == df2.pc_substr_cen]

    # MK30 FN1, SN1, DOB, Sex, Sector, House Number
    cond30 = [df1.fn1_ccs == df2.fn1_cen,
              df1.sn1_ccs == df2.sn1_cen,
              df1.dob_ccs == df2.dob_cen,
              df1.sex_ccs == df2.sex_cen,
              df1.pc_sect_ccs == df2.pc_sect_cen,
              df1.house_no_ccs == df2.house_no_cen]

    # MK31 FN, SN, DOB, PCA, Sex (CLERICAL MATCHKEY)
    cond31 = [((df1.fn1_ccs == df2.fn1_cen) | (df1.fn_ccs == df2.fn_cen)),
              ((df1.sn1_ccs == df2.sn1_cen) | (df1.sn_ccs == df2.sn_cen)),
              df1.dob_ccs == df2.dob_cen,
              df1.pc_area_ccs == df2.pc_area_cen,
              df1.sex_ccs == df2.sex_cen]

    # MK32 FN, SN, Sex, Age within 6, PC (CLERICAL MATCHKEY)
    cond32 = [lev_score('fn1_ccs', 'fn1_cen') > 0.80,
              lev_score('sn1_ccs', 'sn1_cen') > 0.80,
              abs_(df1.age_ccs - df2.age_cen) < 6,
              df1.pc_ccs == df2.pc_cen,
              df1.sex_ccs == df2.sex_cen]
    
    # MK33 Resident FN1, Resident SN1, DOB, Sex, PC
    cond33 = [(((df1.res_fn1_ccs == df2.fn1_cen) & (df1.res_sn1_ccs == df2.sn1_cen)) | ((df2.res_fn1_cen == df1.fn1_ccs) & (df2.res_sn1_cen == df1.sn1_ccs))),
              df1.dob_ccs == df2.dob_cen,
              df1.sex_ccs == df2.sex_cen,
              df1.pc_ccs == df2.pc_cen]
    
    # MK34 Nickname, lev FN1 and SN1, dob, sex, Alt PC.
    cond34 = [((df1.fn1_nickname_ccs == df2.fn1_nickname_cen) | (lev_score('fn1_ccs', 'fn1_cen') > 0.80)),
             (lev_score('sn1_ccs', 'sn1_cen') > 0.80),
             df1.dob_ccs == df2.dob_cen,
             df1.sex_ccs == df2.sex_cen,
             ((df1.pc_alt_ccs == df2.pc_cen) | (df1.pc_ccs == df2.pc_alt_cen) | (df1.pc_alt_ccs == df2.pc_alt_cen))]
    
    # MK35 Nickname, lev FN1 & SN1, AGE within 1, UPRN, Sex
    cond35 = [(df1.fn1_nickname_ccs == df2.fn1_nickname_cen) | (levenshtein(df1.fn1_ccs, df2.fn1_cen) < 2),
              levenshtein(df1.sn1_ccs, df2.sn1_cen) < 2,
              abs_(df1.age_ccs - df2.age_cen) < 2,
              df1.uprn_ccs == df2.uprn_cen,
              df1.sex_ccs == df2.sex_cen]

    # Save list
    keys = [cond01, cond02, cond03, cond04, cond05, cond06,
            cond07, cond08, cond09, cond10, cond11, cond12,
            cond13, cond14, cond15, cond16, cond17, cond18,
            cond19, cond20, cond21, cond22, cond23, cond24,
            cond25, cond26, cond27, cond28, cond29, cond30,
            cond31, cond32, cond33, cond34, cond35]

    return keys


# ------------------------------------- #
# --- JARO / MOVES / CONTAINED KEYS --- #
# ------------------------------------- #


def Cont_Keys():
    '''
    List containing Contained keys
    '''
    keys = [7]
    return keys


def Loose_Jaro_Keys():
    '''
    List containing Jaro keys
    '''
    keys = [23]
    return keys


def Strict_Jaro_Keys():
    '''
    List containing Jaro surname only keys
    '''
    keys = [11]
    return keys


def Move_Keys():
    '''
    List containing Moves keys
    '''
    keys = [28]
    return keys
  

def Clerical_Keys():
  '''
  List containing clerical matchkeys
  '''
  keys = ['14', '31','32']
  return keys

# ---------------------------------------- #
# -------- PROBABILISTIC VARIABLES ------- #
# ---------------------------------------- #


def MU_Variables():
    '''
    Calculate M and U vales for these variables
    '''
    variables = ['fn1', 'sn1', 'sex', 'dob']
    return variables


# ----------------------------------------- #
# -------- PROBABILISTIC THRESHOLDS ------- #
# ----------------------------------------- #


def UPPER_PROB_THRESHOLD():
    value = 40
    return value


def LOWER_PROB_THRESHOLD():
    value = 8.5
    return value


# ----------------------------------------- #
# ----- ASSOCIATIVE STAGE 1 THRESHOLD ----- #
# ----------------------------------------- #


# Upper
def ASSOC_THRESHOLD():
    value = 27
    return value


# ----------------------------------------- #
# ------- PRESEARCH BLOCKING PASSES ------- #
# ----------------------------------------- #


def BLOCKING(df1, df2):

    cond01 = [df1.dob_ccs == df2.dob_cen,
              ((1 - ((levenshtein(df1.fn1_ccs, df2.fn1_cen) /
                      greatest(length(df1.fn1_ccs), length(df2.fn1_cen))))) > 0.6),
              ((1 - ((levenshtein(df1.sn1_ccs, df2.sn1_cen) /
                      greatest(length(df1.sn1_ccs),  length(df2.sn1_cen))))) > 0.6)]

    cond02 = [df1.age_ccs == df2.age_cen,
              ((1 - ((levenshtein(df1.fn1_ccs, df2.fn1_cen) /
                      greatest(length(df1.fn1_ccs), length(df2.fn1_cen))))) > 0.6),
              ((1 - ((levenshtein(df1.sn1_ccs, df2.sn1_cen) /
                      greatest(length(df1.sn1_ccs),  length(df2.sn1_cen))))) > 0.6)]

    cond03 = [df1.fn1_ccs == df2.fn1_cen,
              df1.dob_ccs == df2.dob_cen,
              ((1 - ((levenshtein(df1.sn1_ccs, df2.sn1_cen) /
                      greatest(length(df1.sn1_ccs), length(df2.sn1_cen))))) > 0.4)]

    cond04 = [df1.sn1_ccs == df2.sn1_cen,
              df1.dob_ccs == df2.dob_cen,
              ((1 - ((levenshtein(df1.fn_ccs, df2.fn_cen) /
                      greatest(length(df1.fn_ccs), length(df2.fn_cen))))) > 0.4)]

    cond05 = [df1.fn1_ccs == df2.fn1_cen,
              df1.sn1_ccs == df2.sn1_cen]

    cond06 = [df1.fn2_ccs == df2.fn2_cen,
              df1.sn1_ccs == df2.sn1_cen]

    cond07 = [df1.fn1_ccs == df2.fn1_cen,
              df1.fn2_ccs == df2.fn2_cen]
    
    cond08 = [df1.fn1_ccs == df2.fn1_cen,
              df1.dob_ccs == df2.dob_cen]   
    
    cond09 = [df1.fn2_ccs == df2.fn2_cen,
              df1.dob_ccs == df2.dob_cen]       
    
    cond10 = [df1.sn1_ccs == df2.sn1_cen,
              df1.dob_ccs == df2.dob_cen]       
    
    cond11 = [df1.fn1_nickname_ccs == df2.fn1_nickname_cen,
              df1.sn1_ccs == df2.sn1_cen,
              df1.age_ccs == df2.age_cen]

    cond12 = [df1.sn1_ccs == df2.sn1_cen,
              df1.pc_ccs == df2.pc_1Y_cen,
	      		  ((1 - ((levenshtein(df1.fn1_ccs, df2.fn1_cen) /
                      greatest(length(df1.fn1_ccs), length(df2.fn1_cen))))) > 0.6)]

    # Save list
    blocks = [cond01, cond02, cond03, cond04, cond05, cond06,
              cond07, cond08, cond09, cond10, cond11, cond12]

    return blocks
