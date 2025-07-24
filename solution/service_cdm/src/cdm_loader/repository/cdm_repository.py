import uuid
from datetime import datetime
from lib.pg import PgConnect
from pydantic import BaseModel

class CdmRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db
        
    def user_counters_merge(self,
                            user_id: uuid,
                            product_id: uuid,
                            product_name: str,
                            category_id: uuid,
                            category_name: str,
                            order_cnt: int
                            ) -> None:
        """cdm.user_product_counters, cdm.user_category_counters"""

        #cdm.user_product_counters
        sql = f"""
            INSERT INTO cdm.user_product_counters
                (user_id, product_id, product_name, order_cnt)
            VALUES ( '{user_id}', '{product_id}', '{product_name}', '{order_cnt}' )
            ON CONFLICT (user_id, product_id) DO UPDATE
            SET
                product_name = EXCLUDED.product_name,
                order_cnt    = order_cnt + EXCLUDED.order_cnt; """
        
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)

        #cdm.user_category_counters
        sql = f"""
            INSERT INTO cdm.user_category_counters
                (user_id, category_id, category_name, order_cnt)
            VALUES ( '{user_id}', '{category_id}', '{category_name}', '{order_cnt}' )
            ON CONFLICT (user_id, category_id) DO UPDATE
            SET
                category_name = EXCLUDED.category_name,
                order_cnt    = order_cnt + EXCLUDED.order_cnt; """
        
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)

