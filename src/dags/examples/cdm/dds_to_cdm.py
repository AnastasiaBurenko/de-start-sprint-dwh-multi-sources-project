import logging

import pendulum
from airflow.decorators import dag, task
from examples.cdm.cdm_loader import CdmLoader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'dds', 'cdm', 'example'],
    is_paused_upon_creation=True
)
def sprint5_dds_to_cdm_dag():
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task(task_id="load_cdm")
    def load_cdm():
        rest_loader = CdmLoader(dwh_pg_connect, log)
        rest_loader.load_cdm()

    cdm_load = load_cdm()

    cdm_load


cdm_dds_to_cdm_dag = sprint5_dds_to_cdm_dag()