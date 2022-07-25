import os
import sys
from importlib import import_module
import utils.utils_integrationtests as tu

os.chdir(os.path.dirname(__file__))
#sys.path.insert(0, os.path.abspath("../"))
#sys.path.insert(0, os.path.abspath("../utils/"))

nb_name = "pipeline_wrangling"
nb = import_module(nb_name, package=None)


def test_clean_pipeline():
    # source data

    df_src = tu.create_spark_df_from_dict(
        {
            "Company": [
                'Alexander Thamm GmbH', 'Liefarando GmbH', 'Food Panda GmbH'
            ],

            "Programming":[
                ['Python', 'Java',], ['Python', 'Pyspark'], ['Scala'],
            ]
        }
    )

    df_expected = tu.create_spark_df_from_dict(
        {
            "Company": [
                'Alexander Thamm GmbH', 'Liefarando GmbH', 'Food Panda GmbH'
            ],

            "Programming":[
                ['Python', 'Java',], ['Python', 'Pyspark'], ['Scala'],
            ]
        }
    )

    df_observed = nb.clean_pipeline(df_src)
    print(df_observed.columns)
    tu.assert_equal_df(df_expected, df_observed)

