'''
This module contains rules for the editing/standardisation of person
name variables, along with rules for string comparators of records.
'''
from pyspark.sql.types import FloatType, IntegerType, StringType
from pyspark.sql.functions import regexp_replace, trim, udf


def clean_names_nospace(col):
    """
    Removes any non-alpha characters (including spaces) from a string.
    Replaces these characters with an empty space. Trims outer whitespace.

    Example
    --------
    >>> Input: "JOHN SMITH 2000"
    >>> Output: "JOHNSMITH"
    """
    return trim(regexp_replace(col, "[^A-Z]+", ""))


def clean_names_AZ(col):
    """
    Removes any non-alpha characters (excluding spaces) from a string.
    Replaces these characters with an empty space. Trims outer whitespace.

    Example
    --------
    >>> Input: "JOHN SMITH 2000"
    >>> Output: "JOHN SMITH"
    """
    return trim(regexp_replace(col, "[^A-Z ]+", ""))


def hyphen_to_space(col):
    """
    Replaces all hyphens with single spaces. Trims outer whitespace.

    Example
    --------
    >>> Input: "CHARLIE WILLIAM-JOSEPH"
    >>> Output: "CHARLIE WILLIAM JOSEPH"
    """
    return trim(regexp_replace(col, "-+", " "))


def space_to_hyphen(col):
    """
    Replaces all spaces with hyphens. Trims outer whitespace.

    Example
    --------
    >>> Input: "CHARLIE WILLIAM JOSEPH"
    >>> Output: "CHARLIE-WILLIAM-JOSEPH"
    """
    return trim(regexp_replace(col, " ", "-"))


def single_space(col):
    """
    Replaces mutliple spaces with a single space. Trims outer whitespace.

    Example
    --------
    >>> Input: "   CHARLIE   WILLIAM JOSEPH   "
    >>> Output: "CHARLIE WILLIAM JOSEPH"
    """
    return trim(regexp_replace(col, "\\s+", " "))


# Jaro Winkler
def jaro(string1, string2):
    """
    Applies the Jaro Winkler string similarity function to two strings and calculates a score between 0 and 1.

    Parameters
    ----------
    string1: str
        string to be compared to string2
    string2: str
        string to be compared to string1

    Raises
    ------
    TypeError
        if string1 or string2 are not strings or None

    Returns
    -------
    float
        similarity score between 0 and 1

    Example
    --------
    >>> string1 = "CHARLIE"
    >>> string1 = "CHARLES"
    >>> jaro(string1, string2)
    0.9428571428571428
    """
    # Packages
    import jellyfish

    # Check variable types
    if not ((isinstance(string1, (str, type(None)))) and (isinstance(string2, (str, type(None))))):
        raise TypeError(
            'Both variables being compared must be strings or None')

    # Similarity score between 2 strings
    score = jellyfish.jaro_winkler(
        string1, string2) if string1 is not None and string2 is not None else None

    return score


def remove_final_char(string):
    """
    Removes the final character from a string from a string.
    Replaces this character with an empty space.

    Example
    --------
    >>> Input: "PO15 5RR"
    >>> Output: "PO15 5R "
    """
    substring_ = string[:-1]
    return substring_


def contained_name(name1, name2):
    """
    Compares two lists of names (strings) and checks to see if all the names in one list also appear in the other list

    Parameters
    ----------
    name1: list
        list of strings to be compared to name2
    name2: list
        list of strings to be compared to name1

    Returns
    -------
    integer
        1 if contained, else 0

    Example
    --------
    >>> name1 = ['JOHN', 'JOHN', 'DAVE', 'JIM']
    >>> name2 = ['JOHN', 'DAVE']
    >>> contained_name(name1, name2)
    1
    """
    # Common elements
    common_elements = 0

    # Compare unique values in both lists
    words1 = set(name1.split())
    for word in set(name2.split()):
        if word in words1:
            common_elements += 1

    # Minimum no. of elements
    min_elements = min(len([s[0] for s in name1.split()]),
                       len([t[0] for t in name2.split()]))

    # Contained Indicator
    if min_elements > 1 and min_elements == common_elements:
        contained = 1
    else:
        contained = 0
    return contained


# ---------------------------------- #
# ------------ UDFs ---------------- #
# ---------------------------------- #


alpha_udf = udf(lambda x: ''.join(sorted(x))
                if x is not None else None, StringType())
remove_final_char_udf = udf(lambda x: remove_final_char(
    x) if x is not None else None, StringType())
contained_name_udf = udf(contained_name, IntegerType())
jaro_udf = udf(jaro, FloatType())
