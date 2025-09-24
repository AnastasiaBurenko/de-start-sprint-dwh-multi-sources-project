from logging import Logger
from typing import List
from examples.dds.dds_settings_repository import EtlSetting, StgEtlSettingsRepository
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

from lib import PgConnect
from lib.dict_util import json2str


class FctObj(BaseModel):
    id: int
    delivery_id: int
    courier_id: int
    rate: float
    tip_sum: float


class DmDeliverysOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_deliverys(self, fct_threshold: str, limit: int) -> List[FctObj]:
        with self._db.client().cursor(row_factory=class_row(FctObj)) as cur:
            cur.execute(
                """
                    WITH dmcour AS (
                                    SELECT id, courier_id
                                    FROM dds.dm_couriers dc
                                    ),
                        dmdev AS (
                                    SELECT id, delivery_id
                                    FROM dds.dm_deliveries dc
                                    )
                    SELECT
                        dd.id as id,
                        dmdev.id as delivery_id,
                        dmcour.id AS courier_id,
                        dd.object_value::json->>'rate' as rate,
                        dd.object_value::json->>'tip_sum' as tip_sum
                    FROM stg.deliverysystem_deliveries dd
                    LEFT JOIN dmcour ON dd.object_value::json->>'courier_id' = dmcour.courier_id
 				    LEFT join dmdev on dd.delivery_id = dmdev.delivery_id
                    WHERE dd.id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                        AND dmdev.id is NOT null
                    ORDER BY dd.id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем пачку объектов.
                """, {
                    "threshold": fct_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class DmDeliverysDestRepository:

    def insert_dm_delivery(self, conn: Connection, fct: FctObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.fct_courier_deliveries(delivery_id,courier_id,rate,tip_sum)
                    VALUES (%(delivery_id)s,%(courier_id)s,%(rate)s,%(tip_sum)s);
                """,
                {
                    "delivery_id": fct.delivery_id,
                    "courier_id": fct.courier_id,
                    "rate": fct.rate,
                    "tip_sum": fct. tip_sum
                },
            )


class FctLoader:
    WF_KEY = "dds_fct_deliveries_workflow"
    LAST_LOADED_ID_KEY = "last_load_id"
    BATCH_LIMIT = 1000 
    SHEMA_TABLE = 'dds.srv_wf_settings'

    def __init__(self, pg: PgConnect, log: Logger) -> None:
        self.pg = pg
        self.origin = DmDeliverysOriginRepository(pg)
        self.dds = DmDeliverysDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def fct_load(self):
        with self.pg.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: 0})

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_deliverys(last_loaded, self.BATCH_LIMIT)

            self.log.info(f"Found {len(load_queue)} dm_deliverys to load.")

            if not load_queue:
                self.log.info("Quitting.")
                return

            for fct in load_queue:
                self.dds.insert_dm_delivery(conn, fct)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
