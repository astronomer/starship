from airflow.models.baseoperator import BaseOperator

from astronomer_starship.compat.starship_hook import StarshipDagRunMigrationHook


class StarshipOperator(BaseOperator):
    def __init__(self, hook: StarshipDagRunMigrationHook = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.hook = hook

    def execute(self, context):
        ctx = context["conf"].as_dict()

        if not self.hook:
            self.hook = StarshipDagRunMigrationHook(**ctx)

        return self.hook.load_dagruns_to_target(dag_ids=ctx.get("dag_ids"))
