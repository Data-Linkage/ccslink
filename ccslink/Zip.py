import os, shutil
from CCSLink import Spark_Session as SS

def add_zipped_dependency(zip_from, zip_target):
    """
    This method creates a zip of the code to be sent to the executors.
    It essentially zips the Python packages installed by PIP and
    submits them via addPyFile in the current PySpark context

    E.g. if we want to submit "metaphone" package so that we
    can do use `import metaphone` and use its methods inside UDF,
    we run this method with:
    - zip_from = /home/cdsw/.local/lib/python3.6/site-packages/
    - zip_target = metaphone
    """

    # change this to a path in your project
    zipped_fpath = f'/home/cdsw/zipped_packages/{zip_target}'

    if os.path.exists(zipped_fpath + '.zip'):
        os.remove(zipped_fpath + '.zip')
    shutil.make_archive(

        # path to the resulting zipped file (without the suffix)
        base_name=zipped_fpath, # resulting filename

        # specifies the format --> implies .zip suffix
        format='zip',

        # the root dir from where we want to zip
        root_dir=zip_from,

        # the dir (relative to root dir) which we want to zip
        # (all files in the final zip will have this prefix)
        base_dir=zip_target,
    )

    # add the files to the executors
    SS.SPARK().sparkContext.addPyFile(f'{zipped_fpath}.zip')
