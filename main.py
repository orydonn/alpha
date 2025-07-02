import matplotlib
import csv
import requests
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
    return f'{key_parts[1]} {key_parts[2]}'


def missed_id_to_city(city, ip_address):
    if city == '':
        city = ip_to_city(ip_address)
    return city


with open('investments.csv', newline='', encoding='utf-8') as f1, open('invest_03.csv', newline='', encoding='utf-8') as f2, open('invest_04.csv', newline='', encoding='utf-8') as f3, open('invest_05.csv', newline='', encoding='utf-8') as f4:
    reader = []
    reader_1 = csv.reader(f2)
    reader_2 = csv.reader(f3)
    reader_3 = csv.reader(f4)
    reader_4 = csv.reader(f1)
    for i in reader_1:
        reader.append(i)
    for i in reader_2:
        if i[0] == 'clientID':
            continue
        reader.append(i)
    for i in reader_3:
        if i[0] == 'clientID':
            continue
        reader.append(i)
    for i in reader_4:
        if i[0] == 'clientID':
            continue
        reader.append(i)

    id_dict = dict()
    for i, row in enumerate(reader):
        if i == 0:
            continue
        if int(row[0]) not in id_dict.keys():
            id_dict[int(row[0])] = [get_os(row[3]), row[10], [split_params3(row[8])], [row[9].split('::')[0]], row[12], row[12], None, row[-1]]
        else:
            id_dict[int(row[0])][2].append(split_params3(row[8]))
            id_dict[int(row[0])][3].append(row[9].split('::')[0])
            id_dict[int(row[0])][5] = row[12]
            if row[9] == "passport_correct::/make-money/investments/":
                id_dict[int(row[0])][6] = row[12]
    print(id_dict)

with open("reformatted_invest.txt", "w") as file:
    file.write(str(id_dict))
