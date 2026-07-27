from datetime import datetime
import logging
import time
import json
from typing import Generator
import pytz

from kafka.structs import TopicPartition
from kafka import KafkaConsumer

class Consumer:
    def __init__(
            self,
            kafka_consumer: KafkaConsumer,
            topic: str = "iq_flow",  # Добавляем параметры
            partition: int = 0
    ):
            self.kafka_consumer = kafka_consumer
            self.topic = topic
            self.partition = partition
            self.logger = logging.getLogger(__name__)

    def consume_all_recent_messages(
            self,
            duration_seconds: int,
            poll_timeout_ms: int = 1000,
            max_poll_attempts: int = 30
    ) -> Generator[dict, None, None]:
        tp = TopicPartition(self.topic, self.partition)
        self.kafka_consumer.assign([tp])

        # Определение начальной точки
        end_time = int(time.time() * 1000)
        start_time = end_time - (duration_seconds * 1000)

        # Ищем первый offset, попадающий в наш диапазон
        start_offset = self.kafka_consumer.offsets_for_times({tp: start_time})[tp].offset
        self.kafka_consumer.seek(tp, start_offset)

        # Настройки для стабильного чтения
        self.kafka_consumer.config["max_poll_records"] = 500  # Читаем пачками по 500

        # Полный проход по всем сообщениям периода
        processed_count = 0
        poll_attempts = 0

        while poll_attempts < max_poll_attempts:
            records = self.kafka_consumer.poll(poll_timeout_ms)

            if not records:
                poll_attempts += 1
                continue

            poll_attempts = 0  # Сброс при получении данных

            for _, messages in records.items():
                for msg in messages:
                    try:
                        data = json.loads(msg.value)
                        data['offset_custom'] = str(msg.offset)  # Добавляем offset в словарь с данными

                        msg_time = data.get('timestamp')

                        if isinstance(msg_time, str):
                            msg_time = datetime.strptime(msg_time, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=pytz.UTC)
                            msg_time = int(msg_time.timestamp() * 1000)

                        # Фильтр по времени на случай "перелета"
                        if msg_time and msg_time < start_time:
                            continue

                        if msg_time and msg_time > end_time:
                            return  # Выход при выходе за границы

                        yield data
                        processed_count += 1

                    except json.JSONDecodeError:
                        self.logger.error(f"Ошибка в сообщении offset={msg.offset}")
                        continue

        self.logger.info(f"Обработано сообщений: {processed_count}")