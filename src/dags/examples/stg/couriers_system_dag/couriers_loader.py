from logging import Logger
from typing import List
import requests

from examples.stg import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection

from airflow.models.variable import Variable


class CourierSTGObj():
    def __init__(self, _id: str, object_value: str) -> None:
        self._id = _id
        self.object_value = object_value


class CourierOriginRepository:
    def list_couriers(self, offset: int, limit: int) -> List[CourierSTGObj]:
        base_url = Variable.get("BASE_URL")
        host = base_url + "couriers"
        headers = {
            "X-API-KEY": Variable.get("X_API_KEY"),  # ключ API
            "X-Nickname": Variable.get("NICKNAME"),  # авторизационные данные
            "X-Cohort": Variable.get("COHORT")  # авторизационные данные
        }
        get_params = {
            "sort_field": "id",
            "sort_direction": "asc",
            "offset": offset,
            "limit": limit
        }
        objs = []
        response = requests.get(host, params=get_params, headers=headers).json()
        try:
            for entry in response:
                objs.append(
                    CourierSTGObj(
                        entry.get("_id"),
                        entry
                    )
                )
        except Exception as e:
            print(f"Error fetching couriers: {e}")
        return objs


class CourierDestRepository:

    def insert_courier(self, conn: Connection, courier: CourierSTGObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                 """
                    INSERT INTO stg.deliverysystem_couriers(courier_id, object_value)
                    VALUES (%(_id)s, %(object_value)s)
                    ON CONFLICT (courier_id) DO UPDATE
                    SET
                        object_value = EXCLUDED.object_value;
                """,
                {
                    "_id": courier._id,
                    "object_value": json2str(courier.object_value)
                },
            )


class CourierLoader:
    WF_KEY = "courier_origin_to_stg_workflow"
    OFFSET = "offset"
    BATCH_LIMIT = 50

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.origin = CourierOriginRepository()
        self.pg_dest = pg_dest
        self.stg = CourierDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_couriers(self):
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.OFFSET: 0})
            offset = wf_setting.workflow_settings[self.OFFSET]
            couriers = self.origin.list_couriers(offset, self.BATCH_LIMIT)
            self.log.info(f"Fetched couriers: {couriers}")
            load_queue = self.origin.list_couriers(offset, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} couriers to load.")
            if len(load_queue) == 0:
                self.log.info("Quitting.")
                return
            for courier in load_queue:
                self.stg.insert_courier(conn, courier)
            wf_setting.workflow_settings[self.OFFSET] = offset + len(load_queue)
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.OFFSET]}")
