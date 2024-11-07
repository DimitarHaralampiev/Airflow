from airflow.models import DagBag

def test_dag_loads():
    dag_bag = DagBag()
    dag = dag_bag.get_dag(dag_id="etl_pipeline")
    assert dag is not None
    assert (len(dag.tasks) == 3)