import uuid
from datetime import datetime
from typing import Any, Dict, List
from logging import Logger

from lib.pg import PgConnect
from pydantic import BaseModel


class DdsRepository:
    def __init__(self, db: PgConnect, logger: Logger) -> None:
        self._db = db
        self._logger = logger

    def orders_merge( self, h_order_pk: str, order_id: int, order_dt: datetime, 
                      cost: float, payment: float, status: str, load_dt: datetime, load_src: str ) -> None:
        # merge dds.h_order, dds.s_order_cost, dds.s_order_status

        # merge dds.h_order
        sql = f"""insert into dds.h_order (h_order_pk, order_id, order_dt, load_dt, load_src)
        values
        (
            '{h_order_pk}', {order_id}, '{order_dt}', '{load_dt}', '{load_src}'
        )
        on conflict(h_order_pk) do update
        set
        order_id = excluded.order_id, 
        order_dt = excluded.order_dt, 
        load_dt  = excluded.load_dt, 
        load_src = excluded.load_src;
        """
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                
        # merge dds.s_order_cost
        sql =f"""insert into dds.s_order_cost (h_order_pk,cost,payment,load_dt,load_src,hk_order_cost_hashdiff)
        values 
        (
            '{h_order_pk}', '{cost}', '{payment}', '{load_dt}', '{load_src}', '{h_order_pk}' 
        )
        on conflict (h_order_pk, load_dt) do update set
            cost = excluded.cost, payment = excluded.payment, load_src = excluded.load_src, hk_order_cost_hashdiff = excluded.h_order_pk;
        """
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
        
        # merge dds.s_order_status
        sql = f"""insert into dds.s_order_status (h_order_pk, status, load_dt, load_src, hk_order_status_hashdiff)
        values
        (
            '{h_order_pk}', '{status}', '{load_dt}', '{load_src}', '{h_order_pk}'
        )
        on conflict (h_order_pk, load_dt) do update
        set
        status = excluded.status, load_src = excluded.load_src, hk_order_status_hashdiff = excluded.hk_order_status_hashdiff;
        """ 
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
        
    def users_merge(self, h_user_pk: str, user_id: str, username: str, userlogin: str, load_dt: datetime, load_src: str) -> None:
        # merge dds.h_user, dds.s_user_names

        # dds.h_user
        sql = f"""insert into dds.h_user (h_user_pk, user_id, load_dt, load_src)
        values
        ( '{h_user_pk}'::uuid, '{user_id}', '{load_dt}', '{load_src}' )
        on conflict (h_user_pk) do update
        set user_id = excluded.user_id, load_dt = excluded.load_dt, load_src = excluded.load_src;"""
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)

        # dds.s_user_names
        sql = f"""insert into dds.s_user_names (h_user_pk, username, userlogin, load_dt, load_src, hk_user_names_hashdiff)
        values( '{h_user_pk}'::uuid, '{username}', '{userlogin}', '{load_dt}', '{load_src}', '{h_user_pk}'::uuid)
        on conflict (h_user_pk, load_dt) do update
        set 
        h_user_pk = excluded.h_user_pk, username = excluded.username, 
        userlogin = excluded.userlogin, load_dt = excluded.load_dt, load_src = excluded.load_src;
        """
        self._logger.info(f"!!! - {sql}")
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                
    def restaurants_merge(self, h_restaurant_pk: str, restaurant_id: str, restaurant_name: str,
                            load_dt: datetime, load_src: str) -> None:
        # merge dds.h_restaurant, dds.s_restaurant_names

        # dds.h_restaurant
        sql = f"""insert into dds.h_restaurant (h_restaurant_pk, restaurant_id, load_dt, load_src)
        values ( '{h_restaurant_pk}'::uuid, '{restaurant_id}', '{load_dt}', '{load_src}' )
        on conflict (h_restaurant_pk) do update
        set
        h_restaurant_pk = excluded.h_restaurant_pk,
        restaurant_id = excluded.restaurant_id,
        load_dt = excluded.load_dt,
        load_src = excluded.load_src;"""
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)

        # dds.s_restaurant_names
        sql = f"""insert into dds.s_restaurant_names 
        ( h_restaurant_pk, name, load_dt, load_src, hk_restaurant_names_hashdiff )
        values
        ( '{h_restaurant_pk}'::uuid, '{restaurant_name}', '{load_dt}', '{load_src}', '{h_restaurant_pk}'::uuid )
        on conflict (hk_restaurant_names_hashdiff) do update
        set
        h_restaurant_pk = excluded.h_restaurant_pk,
        name     = excluded.name,
        load_dt  = excluded.load_dt,
        load_src = excluded.load_src;"""
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)

    def categories_merge(self, h_category_pk: str, category_name: str, load_dt: datetime, load_src: str) -> None:
        # merge dds.h_category
        sql = f"""insert into dds.h_category (h_category_pk, category_name, load_dt, load_src)
        values ( '{h_category_pk}', '{category_name}', '{load_dt}', '{load_src}' )
        on conflict (h_category_pk) do update
        set 
        category_name = excluded.category_name,
        load_dt       = excluded.load_dt,
        load_src      = excluded.load_src;"""
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
    
    def products_merge(self, h_product_pk: str, product_id: str, product_name: str, load_dt: datetime, load_src: str) -> None:
        # merge dds.h_product, dds.s_product_names

        # dds.h_product
        sql = f"""insert into dds.h_product (h_product_pk, product_id, load_dt, load_src)
        values ( '{h_product_pk}'::uuid, '{product_id}', '{load_dt}', '{load_src}' )
        on conflict (h_product_pk) do update
        set
        product_id = excluded.product_id,
        load_dt    = excluded.load_dt,
        load_src   = excluded.load_src;"""
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                
        # dds.s_product_names
        sql = f"""
        insert into dds.s_product_names (h_product_pk, name, load_dt, load_src, hk_product_names_hashdiff)
        values ( '{h_product_pk}'::uuid, '{product_name}', '{load_dt}', '{load_src}', '{h_product_pk}'::uuid )
        on conflict (hk_product_names_hashdiff) do update
        set
	h_product_pk              = excluded.h_product_pk,
        name                      = excluded.name,
        load_src                  = excluded.load_src,
        load_dt                   = excluded.load_dt;"""
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)

    def l_product_category_merge(self, hk_product_category_pk: str,
                                  h_product_pk: str, h_category_pk: str,
                                  load_dt: datetime, load_src: str) -> None:
        # merge dds.l_product_category
        sql = f"""
        insert into dds.l_product_category (hk_product_category_pk, h_product_pk, h_category_pk, load_dt, load_src)
        values( '{hk_product_category_pk}'::uuid, '{h_product_pk}', '{h_category_pk}', '{load_dt}', '{load_src}' )
        on conflict (hk_product_category_pk) do update
        set  
        h_product_pk  = excluded.h_product_pk,
        h_category_pk = excluded.h_category_pk,
        load_dt       = excluded.load_dt,
        load_src      = excluded.load_src;"""
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                
    def l_product_restaurant_merge(self, hk_product_restaurant_pk: str,
                                    h_product_pk: str, h_restaurant_pk: str,
                                    load_dt: datetime, load_src: str) -> None:
        # merge dds.l_product_restaurant
        sql = f""" insert into dds.l_product_restaurant
        (hk_product_restaurant_pk, h_product_pk, h_restaurant_pk, load_dt, load_src)
        values ( '{hk_product_restaurant_pk}'::uuid, '{h_product_pk}', '{h_restaurant_pk}', '{load_dt}', '{load_src}' )
        on conflict (hk_product_restaurant_pk) do update
        set
        h_product_pk = excluded.h_product_pk,
        h_restaurant_pk = excluded.h_restaurant_pk,
        load_dt = excluded.load_dt,
        load_src = excluded.load_src;"""
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                
    def l_order_product_merge(self, hk_order_product_pk: str,
                                    h_order_pk: str, h_product_pk: str,
                                    load_dt: datetime, load_src: str) -> None:
        # merge dds.l_order_product
        sql = f"""insert into dds.l_order_product  (hk_order_product_pk, h_order_pk, h_product_pk, load_dt, load_src)
        values ( '{hk_order_product_pk}'::uuid, '{h_order_pk}', '{h_product_pk}', '{load_dt}', '{load_src}' )
        on conflict (hk_order_product_pk) do update
        set 
        h_order_pk   = EXCLUDED.h_order_pk,
        h_product_pk = EXCLUDED.h_product_pk,
        load_dt      = EXCLUDED.load_dt,
        load_src     = EXCLUDED.load_src ;"""
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)

    def l_order_user_merge(self, hk_order_user_pk: str,
                            h_order_pk: str, h_user_pk: str,
                            load_dt: datetime, load_src: str) -> None:
        # merge dds.l_order_user
        sql = f"""
        insert into dds.l_order_user (hk_order_user_pk, h_order_pk, h_user_pk, load_dt, load_src)
        values (  '{hk_order_user_pk}'::uuid, '{h_order_pk}', '{h_user_pk}', '{load_dt}', '{load_src}' )
        on conflict (hk_order_user_pk) do update
        set
        h_order_pk = EXCLUDED.h_order_pk,
        h_user_pk  = EXCLUDED.h_user_pk,
        load_dt    = EXCLUDED.load_dt,
        load_src   = EXCLUDED.load_src;"""
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)

