from datetime import datetime
from logging import Logger
from uuid import UUID

from lib.kafka_connect import KafkaConsumer
from cdm_loader.repository.cdm_repository import CdmRepository

class CdmMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 cdm_repository: CdmRepository,
                 batch_size: int,
                 logger: Logger) -> None:
        self._consumer = consumer
        self._cdm_repository = cdm_repository
        self._logger = logger
        self._batch_size = 100

    # функция вызываемая по расписанию
    def run(self) -> None:
        # Пишем в лог, что джоб запустился
        self._logger.info(f"{datetime.utcnow()}: START")
        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            self._logger.info(f'!!! {msg}')
            self._logger.info(f'$$$ {type(msg)}')
            if msg.get("message_type", "not_cdm_message") != "cdm_message":
                break
            load_dt = datetime.utcnow()
            load_src = 'cdm_message'

            self._logger.info(f"{datetime.utcnow()}:Message.received")

            # merge cdm.user_product_counters, cdm.user_category_counters

            user_id = msg['user_id']

            products = msg['products']
            for product in products:
                product_id = products['product_id']
                product_name = products['product_name']
                category_id = products['category_id']
                category_name = products['category_name']
                order_cnt = products['order_cnt']

                self._cdm_repository.user_counters_merge(
                    user_id,
                    product_id,
                    product_name,
                    category_id,
                    category_name,
                    order_cnt
                )


        self._logger.info(f"{datetime.utcnow()}: FINISH")
