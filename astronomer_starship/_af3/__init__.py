from astronomer_starship.compat import AIRFLOW_V_3

# make sure you can't import this module if not on Airflow 3.x
if not AIRFLOW_V_3:
    raise ImportError("`astronomer_starship._af3` module only supports Airflow 3.x")
