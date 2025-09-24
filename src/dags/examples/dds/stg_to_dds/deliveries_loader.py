from logging import Logger
from typing import List
from examples.dds.dds_settings_repository import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime


class DeliveryObj(BaseModel):
    id: int
    order_id: int
    delivery_id: str
    courier_id: int
    address: str
    delivery_ts: datetime


class DeliveriesOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_deliveries(self, threshold: int, limit: int) -> List[DeliveryObj]:
        with self._db.client().cursor(row_factory=class_row(DeliveryObj)) as cur:
            cur.execute(
                """
                WITH dmcour AS (
                                    SELECT id, courier_id
                                    FROM dds.dm_couriers dc
                                    ),
                        dmord AS (
                                    SELECT id, order_key
                                    FROM dds.dm_orders dc
                                    )
                    SELECT
                        dd.id,
                        dd.delivery_id,
                        dmord.id AS order_id,
                        dmcour.id AS courier_id,
                        dd.object_value::json ->> 'address' AS address,
                        (dd.object_value::json ->> 'delivery_ts')::timestamp AS delivery_ts
                    FROM stg.deliverysystem_deliveries dd
                    LEFT JOIN dmcour ON dd.object_value::json->>'courier_id' = dmcour.courier_id
                    LEFT JOIN dmord on dd.object_value::json ->> 'order_id' = dmord.order_key
                    WHERE dd.id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY dd.id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class DeliveriesDestRepository:

    def insert_deliveries(self, conn: Connection, deliver: DeliveryObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_deliveries(order_id, delivery_id, delivery_ts, courier_id, address)
                    VALUES (%(order_id)s, %(delivery_id)s, %(delivery_ts)s, %(courier_id)s, %(address)s)
                    ON CONFLICT (delivery_id) DO UPDATE
                    SET
                        order_id = EXCLUDED.order_id,
                        courier_id = EXCLUDED.courier_id,
                        address = EXCLUDED.address,
                        delivery_ts = EXCLUDED.delivery_ts
                """,
                {   
                    "order_id": deliver.order_id,
                    "delivery_id": deliver.delivery_id,
                    "courier_id": deliver.courier_id,
                    "address": deliver.address,
                    "delivery_ts": deliver.delivery_ts,
                },
            )


class DeliveriesLoader:
    WF_KEY = "deliveries_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 1000

    def __init__(self, pg: PgConnect, log: Logger) -> None:
        self.pg = pg
        self.origin = DeliveriesOriginRepository(pg)
        self.dds = DeliveriesDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_deliveries(self):
        with self.pg.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_deliveries(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} deliveries to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for deliveries in load_queue:
                self.dds.insert_deliveries(conn, deliveries)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
