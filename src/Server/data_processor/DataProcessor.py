from datetime import datetime
import ipaddress
import logging
import pandas as pd
from src.Server.consumer.Consumer import Consumer
from typing import Tuple, List
from src.Server.config.settings import ID_IPTV_ANALYZER_G2X
from src.Server.config.settings import ID_IPTV_ANALYZER_SURVEYOR
from src.Server.config.settings import ID_DVB_C_ANALYZER_CRICKET


class DataProcessor:
    def __init__(self):
        self.TIME_INTERVAL = 10

    def get_values_from_kafka_topic(self, partition:int, topic:str, kafka_consumer, duration_seconds: int) -> pd.DataFrame:
        """Получаем все значения из списка на странице."""
        consumer = Consumer(
            kafka_consumer=kafka_consumer,
            topic=topic,
            partition=partition
        )
        messages = list(consumer.consume_all_recent_messages(duration_seconds))
        json_data_list = [item for item in messages]
        df = pd.json_normalize(json_data_list)
        return df

    def _check_time_intervals(
            self,
            data: pd.DataFrame,
            probe_id_name: str = 'probeID'
    ) -> Tuple[bool, List[int], List[str], List[Tuple[int, int, int]]]:
        all_valid = True
        all_timestamps = []
        all_pairs_timestamps_with_error = []
        all_probe_ids = []
        grouped = data.groupby(probe_id_name)

        for probe_id, group in grouped:
            unique_data = group.drop_duplicates('timestamp').sort_values('timestamp')
            timestamps = unique_data['timestamp'].tolist()

            if len(timestamps) > 1:
                diffs = pd.Series(timestamps).diff().dropna() / 1000
                for i, diff in enumerate(diffs):
                    if round(abs(diff)) != self.TIME_INTERVAL:
                        all_valid = False
                        all_pairs_timestamps_with_error.append((timestamps[i], timestamps[i + 1], probe_id))

            all_timestamps.extend(timestamps)
            all_probe_ids.extend([probe_id] * len(timestamps))

        return all_valid, all_timestamps, all_probe_ids, all_pairs_timestamps_with_error

    def _check_time_intervals_iq_flow_structure(
            self,
            data: List[Tuple[str, str, int]],
            probe_id_name: str = 'probe_id'
    ) -> Tuple[bool, List[int], List[str], List[str], List[Tuple[int, int, int, int, int]]]:
        df = pd.DataFrame(data, columns=[probe_id_name, 'original', 'timestamp'])
        grouped = df.groupby(probe_id_name)

        all_valid = True
        timestamps = []
        all_pairs_timestamps_with_error = []
        probe_ids = []
        originals = []

        for probe_id, group in grouped:
            unique_data = group.drop_duplicates('timestamp').sort_values('timestamp')
            group_ts = unique_data['timestamp'].tolist()
            group_orig = unique_data['original'].tolist()

            # Проверка условий
            if len(group_ts) > 1:
                diffs = pd.Series(group_ts).diff().dropna() / 1000
                for i, diff in enumerate(diffs):
                    if round(abs(diff)) != self.TIME_INTERVAL:
                        all_valid = False
                        all_pairs_timestamps_with_error.append((group_ts[i], group_ts[i+1], group_orig[i], group_orig[i+1], probe_id))

            timestamps.extend(group_ts)
            probe_ids.extend([probe_id] * len(group_ts))
            originals.extend(group_orig)

        return (all_valid, timestamps, probe_ids, originals, all_pairs_timestamps_with_error)

    def convert_to_formatted_output(self, unique_timestamps: list, probe_ids: list) -> list:
        formatted_output = []

        if len(unique_timestamps) != len(probe_ids):
            raise ValueError("Длины unique_timestamps и probe_ids не совпадают")

        for ts, pid in zip(unique_timestamps, probe_ids):
            formatted_str = f"ProbeID: {pid}, {ts} ({datetime.fromtimestamp(ts / 1000)})"
            formatted_output.append(formatted_str)

        return formatted_output

    def convert_to_formatted_output_for_pairs(self, unique_pairs_timestamp_with_error: list) -> list:
        formatted_output = []

        for ts, ts_other, pid in unique_pairs_timestamp_with_error:
            formatted_str = f"ProbeID: {pid}, {ts} ({datetime.fromtimestamp(ts / 1000)}) и {ts_other} ({datetime.fromtimestamp(ts_other / 1000)}), Diff: {abs(ts - ts_other)/1000}"
            formatted_output.append(formatted_str)

        return formatted_output

    def convert_to_formatted_output_for_ts_with_orig(self, probe_ids, originals, timestamps):
        formatted_pairs = [
            f"ProbeID: {pid}, Original: {orig}, UNIX (ms): {ts}"
            for pid, orig, ts in zip(probe_ids, originals, timestamps)
        ]
        return formatted_pairs

    def convert_to_formatted_output_for_ts_pairs_with_orig(self, info_about_errors):
        formatted_pairs = [
            f"ProbeID: {pid}, Original: {orig} и {orig_other}, UNIX (ms): {ts} и {ts_other}, Diff: {abs(ts - ts_other)/1000}"
            for ts, ts_other, orig, orig_other, pid in info_about_errors
        ]
        return formatted_pairs

    def convert_to_unix(self, df):
        df['timestamp'] = df['timestamp'].apply(
            lambda ts: int(datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S.%fZ").timestamp() * 1000)
        )
        return df

    def _check_message_structure(
            self,
            df: pd.DataFrame,
            required_fields: list[str], probe_id_name: str) -> tuple[
        bool, list[str], list[str], list[str], list[str], list[str], list[str], list[int]]:
        has_offset_custom = 'offset_custom' in df.columns

        missing_offsets = []
        extra_offsets = []
        none_offsets = []
        none_probes_qamchannel = []
        none_probes_src_ip = []
        none_probes_dest_ip = []
        none_fields_copy = []
        # Проверка структуры
        filtered_columns = [col for col in df.columns if col != 'offset_custom']
        missing_fields = [field for field in required_fields if field not in filtered_columns]
        extra_fields = [field for field in filtered_columns if field not in required_fields]
        none_fields = [col for col in filtered_columns if df[col].isnull().any()]
        # Собираем offset_custom для каждого типа ошибок
        if has_offset_custom:
            missing_offsets = df['offset_custom'].tolist() if missing_fields else []

            extra_offsets = df['offset_custom'].tolist() if extra_fields else []

            none_offsets = []
            # сделано для диапазона Source-Specific Multicast: от 232.0.0.0 до 232.255.255.255
            network = ipaddress.ip_network('232.0.0.0/8')
            none_fields_copy.extend(none_fields)
            # проходимся по всем полям, у которых значение null
            for col in none_fields_copy:
                logging.info(f"Current field: {col}")
                if col == "qamchannel":
                    # Находим id анализаторов, у которых qamchannel = null
                    none_probes_qamchannel.extend(df.loc[df[col].isnull(), probe_id_name].tolist())
                    # Если это DVBC анализатор (ID_DVB_C_ANALYZER_CRICKET), то записываем ошибку, когда qamchannel = null
                    if ID_DVB_C_ANALYZER_CRICKET in none_probes_qamchannel:
                        none_offsets.extend(df.loc[(df[col].isnull()) & (
                                df['probeID'] == ID_DVB_C_ANALYZER_CRICKET), 'offset_custom'].tolist())
                    # иначе не считаем это ошибкой, так как для IPTV анализаторов это допустимо
                    else:
                        # Удаляем только одно вхождение "qamchannel"
                        none_fields.remove("qamchannel")
                if col == "destIp":
                    # Находим id анализаторов, у которых destIp = null
                    none_probes_dest_ip.extend(df.loc[df[col].isnull(), probe_id_name].tolist())

                    # Если анализатор типа IPTV (ID_IPTV_ANALYZER_G2X или ID_IPTV_ANALYZER_SURVEYOR), то записываем ошибку
                    if ((ID_IPTV_ANALYZER_G2X in none_probes_dest_ip)
                            or (ID_IPTV_ANALYZER_SURVEYOR in none_probes_dest_ip)):
                        none_offsets.extend(df.loc[(df[col].isnull()) & ((df['probeID'] == ID_IPTV_ANALYZER_G2X) | (
                                df['probeID'] == ID_IPTV_ANALYZER_SURVEYOR)), 'offset_custom'].tolist())
                    # иначе не считаем это ошибкой, так как для DVB-C анализаторов это допустимо
                    else:
                        # Удаляем только одно вхождение "destIp"
                        none_fields.remove("destIp")
                if col == "srcIp":
                    # Находим индексы строк, у которых srcIp = null
                    null_src_ip_indexes = df[df[col].isnull()].index
                    # Находим id анализаторов, у которых srcIp = null
                    none_probes_src_ip.extend(df.loc[df[col].isnull(), probe_id_name].tolist())
                    flag = 0
                    for idx in null_src_ip_indexes:
                        # Находим dest_ip соответствующий найденному индексу строки, где srcIp = null
                        dest_ip = df.at[idx, 'destIp']
                        # Если dest_ip не равен null и находится в диапазоне Source-Specific Multicast,
                        # для которого обязательно наличие srcIp, записываем ошибку
                        if (dest_ip is not None) and (ipaddress.ip_address(dest_ip) in network):
                            none_offsets.append(df.at[idx, 'offset_custom'])
                            flag = 1
                        # Если dest_ip равен null и указан для анализатора типа IPTV, то это ошибка
                        # Для DVBC анализатора такой случай ошибкой не является
                        if ((dest_ip is None) and ((df.at[idx, 'probeID'] == ID_IPTV_ANALYZER_G2X)
                                                   or (df.at[idx, 'probeID'] == ID_IPTV_ANALYZER_SURVEYOR))):
                            none_offsets.append(df.at[idx, 'offset_custom'])
                            flag = 1
                    # Если ошибок не обнаружено, удаляем одно вхождение поля из списка проверяемых
                    if flag == 0:
                        none_fields.remove("srcIp")

            none_offsets = list(set(none_offsets))
            none_offsets.sort(reverse=True)

        result = not missing_fields and not extra_fields and not none_fields
        return (result, missing_fields, extra_fields, none_fields,
                missing_offsets, extra_offsets, none_offsets, none_probes_qamchannel, none_probes_src_ip,
                none_probes_dest_ip)

    def check_time_intervals(self, df: pd.DataFrame):
        result, unique_timestamps, probe_ids, unique_pairs_timestamp_with_error = self._check_time_intervals(df)

        formatted_output_pairs = self.convert_to_formatted_output_for_pairs(unique_pairs_timestamp_with_error)

        error_message = (
            "Временные интервалы между сообщениями не соответствуют ожидаемым.\n"
            f"Всего уникальных меток (для всех анализаторов): {len(unique_timestamps)}\n"
            f"Пары уникальных временных меток с ошибкой и id анализаторов соответственно:\n"
            f"{'\n'.join(formatted_output_pairs)}\n"
        )

        return result, {'unique_timestamps': unique_timestamps, 'error_message': error_message}

    def check_time_intervals_iq_flow_structure(self, df: pd.DataFrame):
        original_timestamps = df['timestamp'].copy()

        df = self.convert_to_unix(df)

        data_tuples = list(zip(df['probe_id'], original_timestamps, df['timestamp']))

        result, timestamps, probe_ids, originals, info_about_errors = self._check_time_intervals_iq_flow_structure(data_tuples)

        formatted_pairs_with_error = self.convert_to_formatted_output_for_ts_pairs_with_orig(info_about_errors)

        error_message = (
            "Временные интервалы между сообщениями не соответствуют ожидаемым.\n"
            f"Всего уникальных меток (для всех анализаторов): {len(timestamps)}\n"
            f"Пары уникальных временных меток с ошибкой и id анализаторов соответственно:\n"
            f"{'\n'.join(formatted_pairs_with_error)}\n"
        )

        return result, {'originals': originals, 'error_message': error_message}

    def check_message_structure(self, required_fields: list[str], probe_id_name: str, topic:str, df: pd.DataFrame):
        (result, missing_fields, extra_fields, none_fields, missing_offsets, extra_offsets,
         none_offsets, none_probes_qamchannel, none_probes_src_ip, none_probes_dest_ip) = self._check_message_structure(
            df, required_fields, probe_id_name)

        error_message = (
            "Структура сообщений не соответствует требованиям\n"
            "Поля, которые были пропущены (хотя бы один раз) в сообщениях:\n"
            f"{' ,'.join(missing_fields)}\n"
            "Поля, которые встречались как лишние (хотя бы один раз) в сообщениях:\n"
            f"{' ,'.join(extra_fields)}\n"
            "Поля, которые были инициализированы как null (хотя бы один раз) в сообщениях:\n"
            f"{' ,'.join(none_fields)}\n"
        )

        if not result and (len(missing_offsets) != 0 or len(extra_offsets) != 0 or len(none_offsets) != 0):
            filename = f'error_offsets_{topic}.csv' # удобнее смотреть, чем в одном файле
            with open(filename, 'w') as f:
                if len(missing_offsets) != 0:
                    f.write("Offset data of fields that were missing (at least once) in messages:\n"
                            f"{' '.join(missing_offsets)}\n\n")
                if len(extra_offsets) != 0:
                    f.write("Offset data of fields that appeared as extra (at least once) in messages:\n"
                                f"{' '.join(extra_offsets)}\n\n")
                if len(none_offsets) != 0:
                    f.write("Offset data of fields that were initialized as null (at least once) in messages:\n"
                            f"{' '.join(none_offsets)}\n\n")

        return result, {'none_probes_qamchannel': none_probes_qamchannel, 'none_probes_src_ip': none_probes_src_ip,
                        'none_probes_dest_ip': none_probes_dest_ip, 'error_message': error_message}


