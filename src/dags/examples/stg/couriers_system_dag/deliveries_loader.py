from logging import Logger
from typing import List
import requests

from examples.stg import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from pydantic import BaseModel

from airflow.models.variable import Variable


class DeliveryObj():
    def __init__(self, delivery_id: str, object_value: str) -> None:
        self.delivery_id = delivery_id
        self.object_value = object_value


class DeliveryOriginRepository:
    def list_deliveries(self, offset: int, limit: int) -> List[DeliveryObj]:
        base_url = Variable.get("BASE_URL")
        host = base_url + "deliveries"
        headers = {
            "X-API-KEY": Variable.get("X_API_KEY"),
            "X-Nickname": Variable.get("NICKNAME"),
            "X-Cohort": Variable.get("COHORT")
        }
        
        get_params = {
            "sort_field": "date",
            "sort_direction": "asc",
            "offset": offset,
            "limit": limit,
            "from": (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')
        }
        objs = []
        try:
            response = requests.get(host, params=get_params, headers=headers).json()
            for entry in response:
                objs.append(
                    DeliveryObj(
                        entry.get("delivery_id"),
                        entry
                    )
                )
        except Exception as e:
            print(f"Error fetching deliveries: {e}")
        return objs


class DeliveryDestRepository:
    def insert_delivery(self, conn: Connection, delivery: DeliveryObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                 """
                    INSERT INTO stg.deliverysystem_deliveries(delivery_id, object_value)
                    VALUES (%(delivery_id)s, %(object_value)s)
                    ON CONFLICT (delivery_id) DO UPDATE
                    SET
                        object_value = EXCLUDED.object_value;
                """,
                {
                    "delivery_id": delivery.delivery_id,
                    "object_value": json2str(delivery.object_value)
                },
            )


class DeliveryLoader:
    WF_KEY = "delivery_origin_to_stg_workflow"
    OFFSET = "offset"
    BATCH_LIMIT = 50

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.origin = DeliveryOriginRepository()
        self.pg_dest = pg_dest
        self.stg = DeliveryDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_deliveries(self):
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.OFFSET: 0})
            offset = wf_setting.workflow_settings[self.OFFSET]
            while True:
                load_queue = self.origin.list_deliveries(offset, self.BATCH_LIMIT) 
                self.log.info(f"Found {len(load_queue)} deliveries to load.")
                if len(load_queue) == 0:
                    self.log.info("No more deliveries to load. Quitting.")
                    break
            for delivery in load_queue:
                self.stg.insert_delivery(conn, delivery)
            wf_setting.workflow_settings[self.OFFSET] = offset + len(load_queue)
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.OFFSET]}")
