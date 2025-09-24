from logging import Logger
from typing import List

from examples.dds.dds_settings_repository import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import str2json, json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class UserObj(BaseModel):
    id: int
    user_id: str
    user_name: str
    user_login: str


class UsersOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_users(self, users_threshold: int, limit: int) -> List[UserObj]:
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                    SELECT id, object_value
                    FROM stg.ordersystem_users
                    WHERE id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": users_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
            modeled_objs = []
            for obj in objs:
                values = str2json(obj[1])
                modeled_obj = UserObj(
                    id=obj[0],
                    user_id=values['_id'],
                    user_name=values['name'],
                    user_login=values['login']
                )
                modeled_objs.append(modeled_obj)
        return modeled_objs


class UsersDestRepository:

    def insert_users(self, conn: Connection, users: UserObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_users(user_id, user_name, user_login)
                    VALUES (%(user_id)s, %(user_name)s, %(user_login)s)
                ;""",
                {
                    "user_id": users.user_id,
                    "user_name": users.user_name,
                    "user_login": users.user_login,
                },
            )


class UsersLoader:
    WF_KEY = "example_users_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 100

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.stg = UsersOriginRepository(pg_dest)
        self.dds = UsersDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_users(self):
        with self.pg_dest.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.stg.list_users(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} users to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for users in load_queue:
                self.dds.insert_users(conn, users)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")