from airflow.models.baseoperator import BaseOperator

from astronomer_starship.providers.starship.hooks.starship import (
    StarshipDagRunMigrationHook,
)


class StarshipOperator(BaseOperator):
    """
    Migrate dag run and task run history by using this operator as a task in a DAG
    """

    def __init__(self, hook: StarshipDagRunMigrationHook = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.hook = hook

    def execute(self, context):
        conf = context["conf"]

        if not self.hook:
            self.hook = StarshipDagRunMigrationHook(**conf)

        return self.hook.load_dagruns_to_target(dag_ids=conf.get("dag_ids"))
