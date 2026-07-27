import allure
from src.Server.consumer.Consumer import Consumer
from src.Server.data_processor.DataProcessor import DataProcessor
from src.Server.config.settings import ID_DVB_C_ANALYZER_CRICKET
from src.Server.config.settings import ID_IPTV_ANALYZER_G2X
from src.Server.config.settings import ID_IPTV_ANALYZER_SURVEYOR
import pytest

@allure.epic("[MONTV-55] iq-config")
@allure.story("[MONTV-1496] Автотестирование получения сырых метрик в топики Kafka")
@allure.label("Requirement", '[FR.0024] Получение метрик от IQ анализаторов, преобразование и отправка в KAFKA')
@allure.tag("Интеграционное")
@allure.tag("Функциональное")
@allure.tag("IQ-съёмник")
class TestStructureServer:
     @allure.id("891927")
     @allure.title("Проверка структуры сообщений для топика iq_flow")
     @allure.description("Тест проверяет, что cтруктура сообщений в топике iq_flow соответствует описанной структуре модели данных метрик \"flowsStore\".")
     def test_message_structure_iq_flow(self, kafka_consumer: Consumer):
         required_fields: list[str] = ["probeID", "flowID", "alias", "tsID", "srcIp", "srcPort", "destIp",
                            "destPort", "flowFaultStatus", "protocol", "tos", "encapsulationString",
                            "headerSize", "payloadSize", "mtspSize", "currentPacketCount","currentLBR",
                            "currentMBR", "currentPBR", "currentVB", "currentDF", "currentML",
                            "lossPercent", "timestamp", "numChannels", "numScrambledChannels",
                            "serviceProvider", "serviceName", "igmpStatus", "qamchannel" ,"qamfrequency",
                            "modulationString", "signalStatus", "tuner", "currentSNR", "currentRSUC", "currentRSCO",
                            "currentBerPostFEC", "currentBerPreFEC", "numSamples", "berPreFECNumSamples",
                            "rxPowerNumSamples", "currentRxPower"]  # необходимые поля

         value = DataProcessor()
         df = value.get_values_from_kafka_topic(0, "iq_flow", kafka_consumer, 5000)
         result, msg = value.check_message_structure(required_fields, "probeID", "iq_flow", df)

         with allure.step("Проверка, что поле 'qamchannel' для Iq_Cricket имеет значение НЕ null"):
            assert not ID_DVB_C_ANALYZER_CRICKET in msg['none_probes_qamchannel'], "Поле 'qamchannel' для Iq_Cricket имеет значение null"
         with allure.step("Проверка, что поле 'srcIp' для Iq_G2X имеет значение НЕ null"):
            assert not ID_IPTV_ANALYZER_G2X in msg['none_probes_src_ip'], "Поле 'srcIp' для Iq_G2X имеет значение null"
         with allure.step("Проверка, что поле 'srcIp' для Iq_Surveyor имеет значение НЕ null"):
            assert not ID_IPTV_ANALYZER_SURVEYOR in msg['none_probes_src_ip'], "Поле 'srcIp' для Iq_Surveyor имеет значение null"
         with allure.step("Проверка, что поле 'destIp' для Iq_G2X имеет значение НЕ null"):
            assert not ID_IPTV_ANALYZER_G2X in msg['none_probes_dest_ip'], "Поле 'destIp' для Iq_G2X имеет значение null"
         with allure.step("Проверка, что поле 'destIp' для Iq_Surveyor имеет значение НЕ null"):
            assert not ID_IPTV_ANALYZER_SURVEYOR in msg['none_probes_dest_ip'], "Поле 'destIp' для Iq_Surveyor имеет значение null"
         with allure.step("Проверка, что структура сообщений содержит нужный состав полей"):
            assert result, msg['error_message']

     @allure.id("891928")
     @allure.title("Проверка структуры сообщений для топика iq_pid")
     @allure.description("Тест проверяет, что структура сообщений в топике iq_pid соответствует описанной структуре модели данных метрик \"pidState\".")
     def test_message_structure_iq_pid(self, kafka_consumer: Consumer):

         required_fields: list[str] = ["probeID", "flowID", "programID", "programChannel", "pidID",
                                       "currPCR", "currCC",
                                       "typeString", "timestamp", "lossRatio", "isScrambled", "isPcr",
                                       "isPmt", "srcIp", "destIp"]  # необходимые поля

         value = DataProcessor()
         df = value.get_values_from_kafka_topic(0, "iq_pid", kafka_consumer, 5000)
         result, msg = value.check_message_structure(required_fields, "probeID", "iq_pid", df)


         with allure.step("Проверка, что поле 'qamchannel' для Iq_Cricket имеет значение НЕ null"):
            assert not ID_DVB_C_ANALYZER_CRICKET in msg['none_probes_qamchannel'], "Поле 'qamchannel' для Iq_Cricket имеет значение null"
         with allure.step("Проверка, что поле 'srcIp' для Iq_G2X имеет значение НЕ null"):
            assert not ID_IPTV_ANALYZER_G2X in msg['none_probes_src_ip'], "Поле 'srcIp' для Iq_G2X имеет значение null"
         with allure.step("Проверка, что поле 'srcIp' для Iq_Surveyor имеет значение НЕ null"):
            assert not ID_IPTV_ANALYZER_SURVEYOR in msg['none_probes_src_ip'], "Поле 'srcIp' для Iq_Surveyor имеет значение null"
         with allure.step("Проверка, что поле 'destIp' для Iq_G2X имеет значение НЕ null"):
            assert not ID_IPTV_ANALYZER_G2X in msg['none_probes_dest_ip'], "Поле 'destIp' для Iq_G2X имеет значение null"
         with allure.step("Проверка, что поле 'destIp' для Iq_Surveyor имеет значение НЕ null"):
            assert not ID_IPTV_ANALYZER_SURVEYOR in msg['none_probes_dest_ip'], "Поле 'destIp' для Iq_Surveyor имеет значение null"
         with allure.step("Проверка, что структура сообщений содержит нужный состав полей"):
            assert result, msg['error_message']

     @allure.id("891929")
     @allure.title("Проверка структуры сообщений для топика iq_flow_etr")
     @allure.description("Тест проверяет, что структура сообщений в топике iq_flow_etr соответствует описанной структуре модели данных метрик \"flowEtrState\".")
     def test_message_structure_iq_flow_etr(self, kafka_consumer: Consumer):

         required_fields: list[str] = ["probeID", "flowID","timestamp", "section_1_4_status","section_1_3_status",
                                       "section_1_6_status", "section_1_5_status","section_1_2_status", "section_1_1_status",
                                       "section_2_6_status", "section_2_2_status", "section_2_3_part2_data.status",
                                       "section_2_3_part1_data.status", "section_2_4_data.status", "section_2_5_data.status",
                                       "section_2_1_status", "section_3_6_part1_data.status", "section_3_6_status",
                                       "section_3_6_part3_data.status", "section_3_1_status", "section_3_1_part4_data.status",
                                       "section_3_7_status", "section_3_5_status", "section_3_5_part4_data.status",
                                       "section_3_2_status", "section_3_4_status", "section_3_8_status"]  # необходимые поля

         value = DataProcessor()
         df = value.get_values_from_kafka_topic(0, "iq_flow_etr", kafka_consumer, 5000)
         result, msg = value.check_message_structure(required_fields, "probeID", "iq_flow_etr", df)

         with allure.step("Проверка, что поле 'qamchannel' для Iq_Cricket имеет значение НЕ null"):
            assert not ID_DVB_C_ANALYZER_CRICKET in msg['none_probes_qamchannel'], "Поле 'qamchannel' для Iq_Cricket имеет значение null"
         with allure.step("Проверка, что поле 'srcIp' для Iq_G2X имеет значение НЕ null"):
            assert not ID_IPTV_ANALYZER_G2X in msg['none_probes_src_ip'], "Поле 'srcIp' для Iq_G2X имеет значение null"
         with allure.step("Проверка, что поле 'srcIp' для Iq_Surveyor имеет значение НЕ null"):
            assert not ID_IPTV_ANALYZER_SURVEYOR in msg['none_probes_src_ip'], "Поле 'srcIp' для Iq_Surveyor имеет значение null"
         with allure.step("Проверка, что поле 'destIp' для Iq_G2X имеет значение НЕ null"):
            assert not ID_IPTV_ANALYZER_G2X in msg['none_probes_dest_ip'], "Поле 'destIp' для Iq_G2X имеет значение null"
         with allure.step("Проверка, что поле 'destIp' для Iq_Surveyor имеет значение НЕ null"):
            assert not ID_IPTV_ANALYZER_SURVEYOR in msg['none_probes_dest_ip'], "Поле 'destIp' для Iq_Surveyor имеет значение null"
         with allure.step("Проверка, что структура сообщений содержит нужный состав полей"):
            assert result, msg['error_message']

     @allure.id("891930")
     @allure.title("Проверка структуры сообщений для топика iq_program")
     @allure.description("Тест проверяет, что структура сообщений в топике iq_program соответствует описанной структуре модели данных метрик \"programState\".")
     def test_message_structure_iq_program(self, kafka_consumer: Consumer):
         required_fields: list[str] = ["probeID", "flowID", "programID", "alias",
                                       "channel","mlr","bitrate","timestamp",
                                       "monPeriod","scrambled","sdtName","sdtProvider",
                                       "numPids"]  # необходимые поля


         value = DataProcessor()
         df = value.get_values_from_kafka_topic(0, "iq_program", kafka_consumer, 5000)
         result, msg = value.check_message_structure(required_fields, "probeID", "iq_program", df)

         with allure.step("Проверка, что поле 'qamchannel' для Iq_Cricket имеет значение НЕ null"):
            assert not ID_DVB_C_ANALYZER_CRICKET in msg['none_probes_qamchannel'], "Поле 'qamchannel' для Iq_Cricket имеет значение null"
         with allure.step("Проверка, что поле 'srcIp' для Iq_G2X имеет значение НЕ null"):
            assert not ID_IPTV_ANALYZER_G2X in msg['none_probes_src_ip'], "Поле 'srcIp' для Iq_G2X имеет значение null"
         with allure.step("Проверка, что поле 'srcIp' для Iq_Surveyor имеет значение НЕ null"):
            assert not ID_IPTV_ANALYZER_SURVEYOR in msg['none_probes_src_ip'], "Поле 'srcIp' для Iq_Surveyor имеет значение null"
         with allure.step("Проверка, что поле 'destIp' для Iq_G2X имеет значение НЕ null"):
            assert not ID_IPTV_ANALYZER_G2X in msg['none_probes_dest_ip'], "Поле 'destIp' для Iq_G2X имеет значение null"
         with allure.step("Проверка, что поле 'destIp' для Iq_Surveyor имеет значение НЕ null"):
            assert not ID_IPTV_ANALYZER_SURVEYOR in msg['none_probes_dest_ip'], "Поле 'destIp' для Iq_Surveyor имеет значение null"
         with allure.step("Проверка, что структура сообщений содержит нужный состав полей"):
            assert result, msg['error_message']

     @allure.id("891931")
     @allure.title("Проверка структуры сообщений для топика iq_flow_structure")
     @allure.description("Тест проверяет, что структура сообщений в топике iq_flow_structure соответствует описанной структуре потока \"FLOW_STRUCTURE\".")
     def test_message_structure_iq_flow_structure(self, kafka_consumer: Consumer):

        required_fields: list[str] = ["flow.flow_properties.tsid", "probe_id", "flow.id", "flow.plps", "timestamp"]  # необходимые поля

        value = DataProcessor()
        df = value.get_values_from_kafka_topic(0, "iq_flow_structure", kafka_consumer, 5000)
        result, msg = value.check_message_structure(required_fields, "probe_id", "iq_flow_structure", df)

        with allure.step("Проверка, что структура сообщений содержит нужный состав полей"):
            assert result, msg['error_message']
