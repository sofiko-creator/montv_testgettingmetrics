import allure
from src.Server.consumer.Consumer import Consumer
from src.Server.data_processor.DataProcessor import DataProcessor

@allure.epic("[MONTV-55] iq-config")
@allure.story("[MONTV-1496] Автотестирование получения сырых метрик в топики Kafka")
@allure.label("Requirement", '[FR.0024] Получение метрик от IQ анализаторов, преобразование и отправка в KAFKA')
@allure.tag("Интеграционное")
@allure.tag("Функциональное")
@allure.tag("IQ-съёмник")
class TestTimingServer:
    @allure.id("886949")
    @allure.title("Проверка временных интервалов между сообщениями для топика iq_flow")
    @allure.description(
        "Тест проверяет, что iq-config регулярно (раз в 10 секунд) опрашивает IQ анализаторы и публикует метрики в Kafka-топик iq_flow.")
    def test_time_intervals_iq_flow(self, kafka_consumer: Consumer):
        value = DataProcessor()
        df = value.get_values_from_kafka_topic(0, "iq_flow", kafka_consumer, 5000)
        result, msg = value.check_time_intervals(df)
        with allure.step("Проверка, что уникальных меток времени достаточно для проверки"):
            assert len(msg['unique_timestamps']) > 1, f"Уникальных меток времени недостаточно для проверки (count = {len(msg['unique_timestamps'])})"
        with allure.step("Проверка, что интервалы между сообщениями для различных анализаторов примерно равны 10 секунд"):
            assert result, msg['error_message']

    @allure.id("888635")
    @allure.title("Проверка временных интервалов между сообщениями для топика iq_pid")
    @allure.description(
        "Тест проверяет, что iq-config регулярно (раз в 10 секунд) опрашивает IQ анализаторы и публикует метрики в Kafka-топик iq_pid.")
    def test_time_intervals_iq_pid(self, kafka_consumer: Consumer):
        value = DataProcessor()
        df = value.get_values_from_kafka_topic(0, "iq_pid", kafka_consumer, 5000)
        result, msg = value.check_time_intervals(df)
        with allure.step("Проверка, что уникальных меток времени достаточно для проверки"):
            assert len(msg['unique_timestamps']) > 1, f"Уникальных меток времени недостаточно для проверки (count = {len(msg['unique_timestamps'])})"
        with allure.step(
                "Проверка, что интервалы между сообщениями для различных анализаторов примерно равны 10 секунд"):
            assert result, msg['error_message']

    @allure.id("888632")
    @allure.title("Проверка временных интервалов между сообщениями для топика iq_flow_etr")
    @allure.description(
        "Тест проверяет, что iq-config регулярно (раз в 10 секунд) опрашивает IQ анализаторы и публикует метрики в Kafka-топик iq_flow_etr.")
    def test_time_intervals_iq_flow_etr(self, kafka_consumer: Consumer):
        value = DataProcessor()
        df = value.get_values_from_kafka_topic(0, "iq_flow_etr", kafka_consumer, 5000)
        result, msg = value.check_time_intervals(df)
        with allure.step("Проверка, что уникальных меток времени достаточно для проверки"):
            assert len(msg['unique_timestamps']) > 1, f"Уникальных меток времени недостаточно для проверки (count = {len(msg['unique_timestamps'])})"
        with allure.step(
                "Проверка, что интервалы между сообщениями для различных анализаторов примерно равны 10 секунд"):
            assert result, msg['error_message']

    @allure.id("888633")
    @allure.title("Проверка временных интервалов между сообщениями для топика iq_program")
    @allure.description(
        "Тест проверяет, что iq-config регулярно (раз в 10 секунд) опрашивает IQ анализаторы и публикует метрики в Kafka-топик iq_program.")
    def test_time_intervals_iq_program(self, kafka_consumer: Consumer):
        value = DataProcessor()
        df = value.get_values_from_kafka_topic(0, "iq_program", kafka_consumer, 5000)
        result, msg = value.check_time_intervals(df)
        with allure.step("Проверка, что уникальных меток времени достаточно для проверки"):
            assert len(msg['unique_timestamps']) > 1, f"Уникальных меток времени недостаточно для проверки (count = {len(msg['unique_timestamps'])})"
        with allure.step(
                "Проверка, что интервалы между сообщениями для различных анализаторов примерно равны 10 секунд"):
            assert result, msg['error_message']

    @allure.id("888634")
    @allure.title("Проверка временных интервалов между сообщениями для топика iq_flow_structure")
    @allure.description(
        "Тест проверяет, что iq-config регулярно (раз в 10 секунд) опрашивает IQ анализаторы и публикует метрики в Kafka-топик iq_flow_structure.")
    def test_time_intervals_iq_flow_structure(self, kafka_consumer: Consumer):
        value = DataProcessor()
        df = value.get_values_from_kafka_topic(0, "iq_flow_structure", kafka_consumer, 5000)
        result, msg = value.check_time_intervals_iq_flow_structure(df)
        with allure.step("Проверка, что уникальных меток времени достаточно для проверки"):
            assert len(msg['originals']) > 1, f"Уникальных меток времени недостаточно для проверки (count = {len(msg['originals'])})"
        with allure.step(
                "Проверка, что интервалы между сообщениями для различных анализаторов примерно равны 10 секунд"):
            assert result, msg['error_message']