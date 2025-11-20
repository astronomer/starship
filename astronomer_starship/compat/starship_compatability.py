from astronomer_starship.compat import AIRFLOW_V_2, AIRFLOW_V_3, AirflowVersionError

if AIRFLOW_V_2:
    from astronomer_starship._af2.starship_compatability import (
        StarshipCompatabilityLayer,
    )
elif AIRFLOW_V_3:
    from astronomer_starship._af3.starship_compatability import (
        StarshipCompatabilityLayer,
    )
else:
    raise AirflowVersionError()

__all__ = [
    "StarshipCompatabilityLayer",
]
