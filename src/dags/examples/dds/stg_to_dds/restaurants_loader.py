from logging import Logger
from typing import List, Union
from datetime import datetime

from examples.dds.dds_settings_repository import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import str2json, json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class RestaurantObj(BaseModel):
    id: int
    restaurant_id: str
    restaurant_name: str
    active_from: datetime


class RestaurantsOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_restaurants(self, restaurants_threshold: int, limit: int) -> List[RestaurantObj]:
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                    SELECT id, object_value, update_ts
                    FROM stg.ordersystem_restaurants
                    WHERE id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": restaurants_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
            modeled_objs = []
            for obj in objs:
                values = str2json(obj[1])
                modeled_obj = RestaurantObj(
                    id=obj[0],
                    restaurant_id=values['_id'],
                    restaurant_name=values['name'],
                    active_from=obj[2]
                )
                modeled_objs.append(modeled_obj)
        return modeled_objs


class RestaurantsDestRepository:

    def insert_restaurants(self, conn: Connection, restaurants: RestaurantObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_restaurants(restaurant_id, restaurant_name, active_from, active_to)
                    VALUES (%(restaurant_id)s, %(restaurant_name)s, %(active_from)s, %(active_to)s)
                    ON CONFLICT (restaurant_id) DO UPDATE
                    SET
                        restaurant_name = EXCLUDED.restaurant_name,
                        active_from = EXCLUDED.active_to
                ;""",
                {
                    "restaurant_id": restaurants.restaurant_id,
                    "restaurant_name": restaurants.restaurant_name,
                    "active_from": restaurants.active_from,
                    "active_to": datetime.strptime('2099-12-31','%Y-%m-%d')
                },
            )


class RestaurantsLoader:
    WF_KEY = "example_restaurants_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 100

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.stg = RestaurantsOriginRepository(pg_dest)
        self.dds = RestaurantsDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_restaurants(self):
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.stg.list_restaurants(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} restaurants to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for restaurants in load_queue:
                self.dds.insert_restaurants(conn, restaurants)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")