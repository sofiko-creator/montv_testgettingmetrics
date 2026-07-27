import os
import allure

from src.Server.config.settings import SERVER_ADDRESS
from src.Server.config.settings import PORT_NUMBER
import pytest
import glob
from kafka import KafkaConsumer

# Фикстура для удаления старых файлов перед запуском тестов
@pytest.fixture(scope="session", autouse=True)
def delete_old_offset_files():
    for file_path in glob.glob('*_offsets_*.csv'):
        try:
            os.remove(file_path)
        except OSError:
            pass

@pytest.fixture(scope="session")
def kafka_consumer():
    # Создаем Consumer
    consumer = KafkaConsumer(
        bootstrap_servers=f'{SERVER_ADDRESS}:{PORT_NUMBER}',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='test_group_id'
    )

    yield consumer  # Возвращаем consumer для использования в тестах

    consumer.close()  # Закрываем consumer после завершения тестов
