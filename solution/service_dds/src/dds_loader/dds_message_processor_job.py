import json, hashlib, uuid
from logging import Logger
from typing import List, Dict
from datetime import datetime
from lib.kafka_connect import KafkaConsumer, KafkaProducer
from dds_loader.repository.dds_repository import DdsRepository

class DdsMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 dds_repository: DdsRepository,
                 batch_size: int,
                 logger: Logger) -> None:
        self._consumer = consumer
        self._producer = producer
        self._dds_repository = dds_repository
        self._logger = logger
        self._batch_size = 100

    # функция вызываемая по расписанию
    def run(self) -> None:
        # Пишем в лог, что джоб запустился
        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):
            dst_msg={}
            dst_products=[]
            msg = self._consumer.consume()
            if msg.get("object_type", "not_order") != "order":
                break
            load_dt = datetime.utcnow()
            load_src = 'orders_backend'

            self._logger.info(f"{datetime.utcnow()}:Message.received")

            # merge dds.h_order, dds.s_orders_cost, dds.s_order_status
            payload = msg['payload']
            order_id = msg['object_id']
            h_order_pk = uuid.UUID(hashlib.sha256(str(order_id).encode()).hexdigest()[::2])
            order_dt = payload['date']
            order_cost = payload['cost']
            order_payment = payload['payment']
            order_status = payload['status']

            self._dds_repository.orders_merge(
                h_order_pk, 
                order_id, 
                order_dt,
                order_cost, 
                order_payment,
                order_status,
                load_dt, 
                load_src
            )

            # merge dds.h_user, dds.s_user_names
            user_id = payload['user']['id']
            username = payload['user']['name']
            h_user_pk = uuid.UUID(hashlib.sha256(str(username).encode()).hexdigest()[::2])
            
            userlogin = username
            self._logger.info(f"warning - {h_user_pk}  -  {username}")
            self._dds_repository.users_merge(
                h_user_pk, 
                user_id, 
                username, 
                userlogin,
                load_dt, 
                load_src
            )

            # merge dds.h_restaurant, dds.s_restaurant_names
            restaurant_id = payload['restaurant']['id']
            restaurant_name = payload['restaurant']['name']
            h_restaurant_pk = uuid.UUID(hashlib.sha256(str(restaurant_id).encode()).hexdigest()[::2])            
            self._dds_repository.restaurants_merge(
                h_restaurant_pk, 
                restaurant_id, 
                restaurant_name,
                load_dt, 
                load_src
            )

            dst_msg['object_id'] = str(order_id)
            dst_msg['message_type'] = 'cdm_message'
            dst_msg['user_id'] = str(h_user_pk)

            # merge all tables
            products = payload['products']
            for product in products:
                s = product['category']
                h_category_pk=uuid.UUID(hashlib.sha256(s.encode()).hexdigest()[::2])
                category_name = product['category']
                self._dds_repository.categories_merge(
                    h_category_pk, 
                    category_name, 
                    load_dt, 
                    load_src)
                
                s = str(product['name'])
                h_product_pk=uuid.UUID(hashlib.sha256(s.encode()).hexdigest()[::2])
                product_id=product['id']
                product_name=product['name']
                self._dds_repository.products_merge(
                    h_product_pk,
                    product_id,
                    product_name,
                    load_dt,
                    load_src)
                
                s = product['category'] + product['name']
                hk_product_category_pk = uuid.UUID(hashlib.sha256(s.encode()).hexdigest()[::2])
                self._dds_repository.l_product_category_merge(
                    hk_product_category_pk,
                    h_product_pk,
                    h_category_pk,
                    load_dt,
                    load_src)
                
                s = product['name'] + restaurant_name
                hk_product_restaurant_pk = uuid.UUID(hashlib.sha256(s.encode()).hexdigest()[::2])
                self._dds_repository.l_product_restaurant_merge(
                    hk_product_restaurant_pk,
                    h_product_pk,
                    h_restaurant_pk,
                    load_dt,
                    load_src)
                
                s = str(msg['object_id']) + product_name
                hk_order_product_pk = uuid.UUID(hashlib.sha256(s.encode()).hexdigest()[::2])
                self._dds_repository.l_order_product_merge(
                    hk_order_product_pk,
                    h_order_pk,
                    h_product_pk,
                    load_dt,
                    load_src)
                
                s = str(msg['object_id']) + username
                hk_order_user_pk = uuid.UUID(hashlib.sha256(s.encode()).hexdigest()[::2])
                self._dds_repository.l_order_user_merge(
                    hk_order_user_pk,
                    h_order_pk,
                    h_user_pk,
                    load_dt,
                    load_src)
                
                dst_products.append(
                {
                "id": str(product['id']),
                "product_id": str(h_product_pk),
                "product_name": product_name,
                "category_id": str(h_category_pk),
                "category_name": category_name,
                "quantity": str(product['quantity'])
                })

            dst_msg['products'] = dst_products

            # Formation of CDM message and # Send message to Kafka topic
            if order_status == 'CLOSED':
                #self._logger.info(f"to cdm message {dst_msg}")
                self._producer.produce(dst_msg)


        self._logger.info(f"{datetime.utcnow()}: FINISH")
