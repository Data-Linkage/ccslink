# ccslink
code used for matching census to the census coverage survey

This has been stripped down from the matching pipeline ran in a secure computing environment - some dependencies are out of date due to this, but these have been left as-is so that they could easily be ported back into that environment.

## project structure
* All custom functions are found within the project package - the `ccslink` subdirectory. Deterministic person matchkeys can be found within the `parameters.py` module of ccslink.
* The code for data cleaning and standardising, along with the deterministic and probabilistic matching loops can be found within `pipeline_scripts`
