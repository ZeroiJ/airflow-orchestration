import pytest
from airflow.models import DagBag

def test_dag_loaded():
    dagbag = DagBag(dag_folder='dags/', include_examples=False)
    assert len(dagbag.import_errors) == 0, "No import errors"
    
    dag_id = 'aviation_pipeline'
    assert dag_id in dagbag.dags
    
    dag = dagbag.get_dag(dag_id)
    
    # Verify tasks
    task_ids = {task.task_id for task in dag.tasks}
    assert task_ids == {'run_ingestion', 'run_dbt_models', 'run_dbt_tests'}
    
    # Verify dependencies
    ingest_task = dag.get_task('run_ingestion')
    assert 'run_dbt_models' in ingest_task.downstream_task_ids
    
    dbt_models_task = dag.get_task('run_dbt_models')
    assert 'run_dbt_tests' in dbt_models_task.downstream_task_ids

    # Verify configs
    assert dag.catchup is True
    assert dag.default_args['retries'] == 3
    assert dag.default_args['retry_exponential_backoff'] is True
