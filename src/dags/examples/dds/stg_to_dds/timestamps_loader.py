from logging import Logger
from typing import List
from datetime import datetime, time, date

from examples.dds.dds_settings_repository import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import str2json, json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class TimestampObj(BaseModel):
    id: int
    ts: datetime
    year: int
    month: int
    day: int
    time: time
    date: date


class TimestampsOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_timestamps(self, timestamps_threshold: int, limit: int) -> List[TimestampObj]:
        with self._db.client().cursor(row_factory=class_row(TimestampObj)) as cur:
            cur.execute(
                """
                    select
                        id,
                        date_trunc('second', update_ts) as ts,
                        date_part('year', update_ts) as "year",
                        date_part('month', update_ts) as "month",
                        date_part('day', update_ts) as "day",
                        date_trunc('second', update_ts)::time as "time",
                        update_ts::date as "date"
                    from stg.ordersystem_orders
                    WHERE id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": timestamps_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class TimestamptsDestRepository:

    def insert_timestamps(self, conn: Connection, timestamps: TimestampObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_timestamps(id, ts, year, month, day, time, date)
                    VALUES (%(id)s, %(ts)s, %(year)s, %(month)s, %(day)s, %(time)s, %(date)s)
                ;""",
                {
                    "id": timestamps.id,
                    "ts": timestamps.ts,
                    "year": timestamps.year,
                    "month": timestamps.month,
                    "day": timestamps.day,
                    "time": timestamps.time,
                    "date": timestamps.date,
                },
            )


class TimestampsLoader:
    WF_KEY = "example_timestamps_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 10000

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.stg = TimestampsOriginRepository(pg_dest)
        self.dds = TimestamptsDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_timestamps(self):
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.stg.list_timestamps(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} timestamps to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for timestamps in load_queue:
                self.dds.insert_timestamps(conn, timestamps)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")