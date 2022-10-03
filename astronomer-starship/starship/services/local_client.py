from airflow.utils.session import provide_session
from sqlalchemy.orm import Session


class LocalAirflowClient:
    @provide_session
    def get_variables_from_metastore(self, session: Session):
        from airflow.models import Variable

        variables = session.query(Variable).all()
        return variables

    @provide_session
    def get_connections(self, session: Session):
        from airflow.models import Connection

        connections = session.query(Connection).all()
        return connections

    @provide_session
    def get_variables(self, session: Session):
        from airflow.models import Variable

        vars = session.query(Variable).all()
        return vars

    def get_dags(self):
        from airflow.models import DagBag

        dags = DagBag(read_dags_from_db=True).dags
        return dags
