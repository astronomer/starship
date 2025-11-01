from astronomer_starship.compat import AIRFLOW_V_2, AirflowVersionError

if AIRFLOW_V_2:
    from astronomer_starship._af2.starship_compatability import (
        StarshipCompatabilityLayer,
        get_test_data,
    )
else:
    raise AirflowVersionError()

__all__ = [
    "get_test_data",
    "StarshipCompatabilityLayer",
]
