from logging import Logger
from typing import List, Union
from datetime import datetime

from examples.dds.dds_settings_repository import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import str2json, json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class ProductsObj(BaseModel):
    id: int
    restaurant_id: int
    product_id: str
    product_name: str
    product_price: float
    active_from: datetime


class ProductsOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_products(self, products_threshold: int, limit: int) -> List[ProductsObj]:
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                    SELECT id, object_value, update_ts
                    FROM stg.ordersystem_restaurants
                    WHERE id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": products_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()

            cur.execute("select restaurant_id, id from dds.dm_restaurants;")
            restaurants = dict(cur.fetchall())

            modeled_objs = []
            for obj in objs:
                id = obj[0]
                values = str2json(obj[1])
                active_from = obj[2]
                restaurant_id = restaurants[values['_id']]
                menu = values['menu']
                for prod in menu:
                    product_id = prod['_id']
                    product_name = prod['name']
                    product_price = prod['price']
                    modeled_obj = ProductsObj(
                        id=id,
                        restaurant_id=restaurant_id,
                        product_id=product_id,
                        product_name=product_name,
                        product_price=product_price,
                        active_from=active_from
                    )
                    modeled_objs.append(modeled_obj)
        return modeled_objs


class ProductsDestRepository:

    def insert_products(self, conn: Connection, products: ProductsObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_products(restaurant_id, product_id, product_name, product_price, active_from, active_to)
                    VALUES (%(restaurant_id)s, %(product_id)s, %(product_name)s, %(product_price)s, %(active_from)s, %(active_to)s)
                    ON CONFLICT (product_id) DO UPDATE
                    SET
                        product_name = EXCLUDED.product_name,
                        active_from = EXCLUDED.active_to
                ;""",
                {
                    "restaurant_id": products.restaurant_id,
                    "product_id": products.product_id,
                    "product_name": products.product_name,
                    "product_price": products.product_price,
                    "active_from": products.active_from,
                    "active_to": datetime.strptime('2099-12-31','%Y-%m-%d')
                },
            )


class ProductsLoader:
    WF_KEY = "example_products_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 1000

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.stg = ProductsOriginRepository(pg_dest)
        self.dds = ProductsDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_products(self):
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.stg.list_products(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} products to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for products in load_queue:
                self.dds.insert_products(conn, products)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")