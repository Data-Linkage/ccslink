'''
This module contains standard and udf functions to calculate comparisons between 
households for use in household matching.
'''
import itertools
import re
from pyspark.sql.types import FloatType, IntegerType, StringType
from pyspark.sql.functions import udf
import numpy as np
from nltk.metrics import edit_distance
import jellyfish


# ---------------------------------------- #
# ---- Functions using PySpark Arrays ---- #
# ---------------------------------------- #


# Common elements function
def common(a, b):
    """
    Calculates the number of unique common elements between two lists.

    Parameters
    ----------
    a: list
        list of strings to be compared to b
    b: list
        list of strings to be compared to a

    Raises
    ------
    TypeError
        if a or b are not lists

    Returns
    -------
    integer
        number of unique common elements

    Example
    --------
    >>> list_1 = ['dog', 'dog', 'cat', 'fish']
    >>> list_2 = ['dog', 'fish']
    >>> common(list_1, list_2)
    2
    """
    # Check variable types
    if not ((isinstance(a, list)) and (isinstance(b, list))):
        raise TypeError('Both variables being compared must contain lists')

    # Number of common elements
    value = len(list(set(filter(lambda x: x in a, b)))
                ) if a is not None and b is not None else None

    return value


# Common ages function
def common_age(a, b):
    """
    Calculates the number of ages in common between two lists of ages.
    Allows for ages to be one year apart.

    Parameters
    ----------
    a: list
        list of age strings to be compared to b
    b: list
        list of age strings to be compared to a

    Raises
    ------
    TypeError
        if a or b are not lists

    Returns
    -------
    integer
        number of ages in common

    Example
    --------
    >>> list_1 = ['15', '20', '2']
    >>> list_2 = ['15', '15', '20', '2', '99']
    >>> common_age(list_1, list_2)
    4
    """
    # Check variable types
    if not ((isinstance(a, list)) and (isinstance(b, list))):
        raise TypeError('Both variables being compared must contain lists')

    # Compare two age sets against each other
    comparisons = list(itertools.product(a, b))

    # Count how many are equal or 1 year apart
    value = [abs(int(x)-int(y)) for x, y in comparisons]
    value = len(list(filter(lambda x: x <= 1, value)))

    return value


# Max Lev Distance between two sets of names
def max_lev(a, b):
    """
    Compares two lists of names and calculates the maximum standardised edit distance
    score between the names being compared

    Parameters
    ----------
    a: list
        list of name strings to be compared to b
    b: list
        list of name strings to be compared to a

    Raises
    ------
    TypeError
        if a or b are not lists

    Returns
    -------
    float
        maximum standardised edit distance score

    Example
    --------
    >>> list_1 = ['Charlie', 'John', 'Steve', 'Bob']
    >>> list_2 = ['Dave', 'Charles']
    >>> max_lev(list_1, list_2)
    0.7142857142857143
    """
    # Check variable types
    if not ((isinstance(a, list)) and (isinstance(b, list))):
        raise TypeError('Both variables being compared must contain lists')

    # Compare two name sets and calcualte max lev score
    comparisons = list(itertools.product(a, b))
    max_score = float(np.max(
        [(1 - (edit_distance(x, y) / (np.max([len(x), len(y)])))) for x, y in comparisons]))

    return max_score


# Name similarity Function - Max Jaro Distance between two sets of names
def max_jaro(a, b):
    """
    Compares two lists of names and calculates the maximum jaro winkler similarity
    score between the names being compared

    Parameters
    ----------
    a: list
        list of name strings to be compared to b
    b: list
        list of name strings to be compared to a

    Raises
    ------
    TypeError
        if a or b are not lists

    Returns
    -------
    float
        maximum jaro winkler similarity score

    Example
    --------
    >>> list_1 = ['Charlie', 'John', 'Steve', 'Bob']
    >>> list_2 = ['Dave', 'Charles']
    >>> max_lev(list_1, list_2)
    0.9428571428571428
    """
    # Check variable types
    if not ((isinstance(a, list)) and (isinstance(b, list))):
        raise TypeError('Both variables being compared must contain lists')

    # Compare two name sets and calcualte max lev score
    comparisons = list(itertools.product(a, b))
    max_score = max([jellyfish.jaro_winkler(x, y) for x, y in (comparisons)])

    return max_score


# -------------------------------------- #
# ---- House / Flat Number Functions --- #
# -------------------------------------- #


# Terms to remove from addresses before we apply house number function
terms = ['FIRST FLOOR FLAT', 'FLAT 1ST FLOOR', 'SECOND FLOOR FLAT',
         'FLAT 2ND FLOOR','FLAT 1ST AND 2ND FLOOR', 'FLAT 1ST 2ND AND 3RD FLOOR', 'THIRD FLOOR FLAT',
         'FLAT 2ND AND 3RD FLOOR', 'FLAT 3RD AND 4TH FLOOR', 'FLAT 4TH AND 5TH FLOOR',
         'GROUND FLOOR FLAT']


def replace_all(string, terms):
    """
    Removes any occurances of words specified in the terms list from the chosen string

    Parameters
    ----------
    string: str
        string to remove terms from
    terms: list
        list of terms to be removed from string

    Returns
    -------
    str
        string with terms removed

    Example
    --------
    >>> string = 'FIRST FLOOR FLAT 20 PARK ROAD'
    >>> terms = ['FIRST FLOOR FLAT', 'SECOND FLOOR FLAT']
    >>> replace_all(string, terms)
    '20 PARK ROAD'
    """
    for term in terms:
        string = string.replace(term, '').strip()
    return string


def house_number(address):
    """
    Extracts house number from an address string. Flat numbers and apartment numbers are removed before 
    extraction to make it more likely that the correct number is extracted, although this function will not
    get it right 100% of the time.

    Parameters
    ----------
    address: str
        address string to extract house number from

    Returns
    -------
    str
        house number

    Example
    --------
    >>> address = 'FIRST FLOOR FLAT 20 PARK ROAD'
    >>> house_number(address)
    '20'
    """

    # Upper Case
    address = address.upper()
    
    # If postcode is in address, remove from the end of the string (after the final comma)
    address = ','.join(address.split(',')[:-1])

    # Remove terms from address to improve performance
    address = replace_all(address, terms)

    # Remove possible flat/apartment numbers from address
    address = re.sub('FLAT(\s*)(\d+)',      '', address).strip()
    address = re.sub('APARTMENT(\s*)(\d+)', '', address).strip()
    address = re.sub('UNIT(\s*)(\d+)',      '', address).strip()
    address = re.sub('ROOM(\s*)(\d+)',      '', address).strip()
    
    # Create list of possible house numbers from address
    numbers = re.findall(r'\d+', address)

    # Remove zeros & leading zeros
    numbers = [number.lstrip('0') for number in numbers if number not in ['0']]

    # Take first number as house number
    if len(numbers) > 0:
        house_no = str(numbers[0])
    else:
        house_no = None

    return house_no


def flat_number(address):
    """
    Extracts flat/apartment number from an address string.

    Parameters
    ----------
    address: str
        address string to extract flat/apartment number from

    Returns
    -------
    str
        flat/apartment number

    Example
    --------
    >>> address = 'FLAT 5, 15 PARK ROAD'
    >>> flat_number(address)
    '5'
    """

    # Upper Case
    address = address.upper()

    # Get FLAT/APARTMENT NUMBER
    flat = re.findall(r'FLAT (\d+)', address)
    apart = re.findall(r'APARTMENT (\d+)', address)

    if len(flat) > 0:
        number = flat[0]

    else:
        if len(apart) > 0:
            number = apart[0]
        else:
            number = '0'

    # Remove zeros & leading zeros
    if number != '0':
        number = number.lstrip('0')
    else:
        number = None

    return number


# ---------------------------------- #
# ------------ UDFs ---------------- #
# ---------------------------------- #


common_udf = udf(common,     IntegerType())
common_age_udf = udf(common_age, IntegerType())
max_lev_udf = udf(max_lev,  FloatType())
max_jaro_udf = udf(max_jaro, FloatType())
house_number_udf = udf(lambda x: house_number(x) if x is not None else None, StringType())
flat_number_udf = udf(lambda x: flat_number(x) if x is not None else None, StringType())
