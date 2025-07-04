import time
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from datetime import datetime as dt
import zipfile, os, csv, re, logging
from clickhouse_driver import Client
import csv
from collections import defaultdict


def get_os(os_full: str) -> str:
    if 'windows_mobile' in os_full:
        return os_full
    elif 'ios' in os_full:
        return 'ios'
    elif 'android' in os_full:
        return 'android'
    elif 'windows' in os_full:
        return 'windows'
    elif 'mac' in os_full:
        return 'mac_os'
    elif os_full in ['linux', 'ubuntu']:
        return os_full
    else:
        return 'other'


def split_params3(key: str) -> str:
    key_parts = key.split('::')
    if len(key_parts) == 3:
        return f'{key_parts[1]} {key_parts[2]}'
    else:
        return f'{" ".join(key_parts)}'


# Константы путей и хоста ClickHouse
ZIP_STAGING_DIR = "/home/iliushaaks/airflow/zips"
EXTRACTED_DIR = "/home/iliushaaks/airflow/extracted"
CLICKHOUSE_HOST = "localhost"

# Настройка логирования
log = logging.getLogger("airflow.task")


# Распаковка всех zip-архивов из каталога zips в каталог extracted
def extract_zip_files():
    os.makedirs(EXTRACTED_DIR, exist_ok=True)
    for fname in os.listdir(ZIP_STAGING_DIR):
        if fname.endswith(".zip"):
            with zipfile.ZipFile(os.path.join(ZIP_STAGING_DIR, fname), 'r') as zip_ref:
                zip_ref.extractall(EXTRACTED_DIR)
                log.info(f"Extracted {fname}")


# Определение префикса CSV-файла на основе имени и списка известных префиксов
def extract_prefix(filename: str, known_prefixes: list[str]) -> str | None:
    for prefix in known_prefixes:
        if re.match(rf"^{re.escape(prefix)}([_\W].*|\.csv$)", filename):
            return prefix
    return None


# Преобразование строки CSV в кортеж с типами, ожидаемыми ClickHouse
def cast_row_to_clickhouse(row: list[str]) -> tuple:
    try:
        client_id = int(row[0])
        browser_major = int(row[5])
        is_page_view = int(row[11])
        timestamp = dt.strptime(row[12], "%Y-%m-%d %H:%M:%S")

        # Оборачиваем в массивы, даже если значения пустые
        key1 = [row[6]] if row[6] else []
        key2 = [row[7]] if row[7] else []
        key3 = [row[8]] if row[8] else []
        key4 = [row[9]] if row[9] else []

    except (ValueError, TypeError, IndexError) as e:
        raise ValueError(f"Invalid data conversion: {e}")

    return (
        client_id,
        row[1],  # browser
        browser_major,
        timestamp,
        row[2],  # deviceCategory
        row[10],  # ipAddress
        is_page_view,
        row[14],  # lastTrafficSource
        row[3],  # operatingSystem
        key1,
        key2,
        key3,
        key4,
        row[4],  # regionCity
        row[13]  # URL
    )


# Загрузка строк из CSV-файла в указанную таблицу ClickHouse с батчами по 10k строк
def insert_csv_file(fname: str, table_name: str):
    client = Client(host=CLICKHOUSE_HOST, user='default', password='ghbdtn')
    full_path = os.path.join(EXTRACTED_DIR, fname)
    row_count = 0
    batch = []

    with open(full_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            try:
                parsed = cast_row_to_clickhouse(row)
                batch.append(parsed)
                row_count += 1
                if len(batch) >= 10000:
                    client.execute(f"INSERT INTO {table_name} VALUES", batch)
                    log.info(f"Inserted batch of 10000 into {table_name}")
                    batch = []
            except Exception as e:
                log.error(f"Error parsing row: {row} → {e}")

    if batch:
        client.execute(f"INSERT INTO {table_name} VALUES", batch)
        log.info(f"Inserted final batch ({len(batch)} rows) into {table_name}")
    log.info(f"Total rows inserted into {table_name} from {fname}: {row_count}")


def transform_csv(filename: str):
    with open(os.path.join(EXTRACTED_DIR, filename), newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        id_dict = dict()
        ready_to_csv = list()
        ready_to_csv.append(["clientID", "operatingSystem", "ipAddress",
                             "parsedParamsKey3", "parsedParamsKey4", "dateTimeFirst",
                             "dateTimeLast", "dateTimeConfirmed", "lastTrafficSource"])
        for i, row in enumerate(reader):
            if i == 0:
                continue
            if int(row[0]) not in id_dict.keys():
                id_dict[int(row[0])] = [get_os(row[3]), row[10], [split_params3(row[8])], [row[9].split('::')[0]],
                                        row[12],
                                        row[12], None, row[-1]]
            else:
                id_dict[int(row[0])][2].append(split_params3(row[8]))
                id_dict[int(row[0])][3].append(row[9].split('::')[0])
                id_dict[int(row[0])][5] = row[12]
                if row[8] == "passport_correct::/make-money/investments/":
                    id_dict[int(row[0])][6] = row[12]
        for i in id_dict.items():
            value = i[1]
            ready_to_csv.append(
                [i[0], value[0], value[1], '->'.join(value[2]).replace(',', ';'), '->'.join(value[3]).replace(',', ';'),
                 value[4], value[5], value[6], value[7]])
        done_smt = 0
        form_opened = 0
        filled_form_opened = 0
        sms_confirmed_id = 0
        passport_correct = 0
        parents = 0
        entered_site = len(id_dict)
        for row in id_dict.values():
            if len(set(row[2])) > 1:
                done_smt += 1
            for i in ["fullName_focus", "phone_focus", "birthday_focus"]:
                if i in row[3]:
                    form_opened += 1
                    break
            if 'sms-verification View' in row[2]:
                filled_form_opened += 1
            if 'sms-input_correct' in row[3]:
                sms_confirmed_id += 1
            if 'passport_correct' in row[3]:
                passport_correct += 1
            if 'teenager-plate-button_click' in row[3]:
                parents += 1
    with open(f"funnel_{filename}.csv", "w") as file:
        file.write(f"Action,Dev by Entered,Dev by prev\n")
        file.write(f"Done smth,{done_smt/entered_site},{done_smt/entered_site}\n")
        file.write(f"Went to form,{form_opened/entered_site},{form_opened/done_smt}\n")
        file.write(f"Filled form,{filled_form_opened/entered_site},{filled_form_opened/form_opened}\n")
        file.write(f"Sms confirmed,{sms_confirmed_id/entered_site},{sms_confirmed_id/filled_form_opened}\n")
        file.write(f"Passport correct,{passport_correct/entered_site},{passport_correct/sms_confirmed_id}\n")
        file.write(f"Parents link copied,{parents/entered_site},{parents/sms_confirmed_id}\n")

    with open(f"edited_{filename}.csv", "w") as file:
        for line in ready_to_csv:
            line = [str(item) for item in line]
            file.write((','.join(line) + '\n'))


# DAG
default_args = {"start_date": dt(2023, 1, 1)}
with DAG("upload_local_csv_to_clickhouse", schedule_interval=None, catchup=False, default_args=default_args) as dag:
    begin = EmptyOperator(task_id="begin")

    unzip_task = PythonOperator(
        task_id="unzip_files",
        python_callable=extract_zip_files
    )
    table_map = Variable.get("csv_table_map", deserialize_json=True)
    known_prefixes = list(table_map.keys())

    end = EmptyOperator(task_id="end")

    begin >> unzip_task

    

    for fname in os.listdir(EXTRACTED_DIR):
        if not fname.endswith(".csv"):
            continue

        prefix = extract_prefix(fname, known_prefixes)
        if prefix is None:
            log.warning(f"Unknown prefix in file {fname}")
            continue

        table_name = table_map[prefix]

        task = PythonOperator(
            task_id=f"insert_{prefix}_{fname.replace('.csv', '').replace('-', '_')}",
            python_callable=insert_csv_file,
            op_args=[fname, table_name]
        )

        transform = PythonOperator(task_id=f"transform_{prefix}_{fname.replace('.csv', '').replace('-', '_')}", python_callable=transform_csv, op_args=[fname])
        
        unzip_task >> transform >> task >> end
