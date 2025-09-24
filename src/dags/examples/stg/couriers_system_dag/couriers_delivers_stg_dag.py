import logging
from datetime import datetime, timedelta
import pendulum
from airflow.decorators import dag, task

from examples.stg.couriers_system_dag.couriers_loader import CourierLoader
from examples.stg.couriers_system_dag.deliveries_loader import DeliveryLoader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждые 15 минут.
    start_date=pendulum.now().subtract(days=7),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=True,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'stg', 'origin', 'example'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def delievery_stg_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task(task_id="couriers_load")
    def load_couriers():
        # создаем экземпляр класса, в котором реализована логика.
        courier_loader = CourierLoader(dwh_pg_connect, log)
        courier_loader.load_couriers()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    couriers_dict = load_couriers()

    @task(task_id="deliveries_load")
    def load_deliveries():
        # создаем экземпляр класса, в котором реализована логика.
        courier_loader = DeliveryLoader(dwh_pg_connect, log)
        courier_loader.load_deliveries()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    deliveries_dict = load_deliveries()

    deliveries_dict >> couriers_dict


delievery_stg_dag = delievery_stg_dag()
