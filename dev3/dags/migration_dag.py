from astronomer_starship.providers.starship.operators.starship import StarshipAirflowMigrationDAG

StarshipAirflowMigrationDAG(
    target_http_conn_id="starship_default",
    source_http_conn_id="starship_source",
)
