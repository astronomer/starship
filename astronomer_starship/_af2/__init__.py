from astronomer_starship.compat import AIRFLOW_V_2

# make sure you can't import this module if not on Airflow 2.x
if not AIRFLOW_V_2:
    raise ImportError("`astronomer_starship._af2` module only supports Airflow 2.x")
