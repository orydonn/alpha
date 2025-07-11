import matplotlib
import csv
import requests
from collections import defaultdict

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




    # entered_site = []
    # done_smt = []
    # sms_confirmed_id = []
    # form_opened = []
    # filled_form_opened = []
    # passport_correct = []
    # roditeli = []
    # for i, row in enumerate(reader):
    # passthrough = False
    # for op in ["windows", "mac", "ubuntu", "linux", "chrome"]:
    #     if op in row[3]:
    #         passthrough = True
    #         continue
    # if not passthrough:
    #     continue
    #     if i == 0:
    #         continue
    #     if int(row[0]) not in entered_site:
    #         entered_site.append(int(row[0]))
    #     if int(row[0]) not in form_opened and row[9] in ["fullName_focus::/make-money/investments/", "phone_focus::/make-money/investments/", "birthday_focus::/make-money/investments/"]:
    #         form_opened.append(int(row[0]))
    #     if int(row[0]) not in done_smt and row[8] != "Product::smev::View":
    #         done_smt.append(int(row[0]))
    #     if int(row[0]) not in filled_form_opened and row[8] == "Product::sms-verification::View":
    #         filled_form_opened.append(int(row[0]))
    #     if int(row[0]) not in sms_confirmed_id and row[9] == "sms-input_correct::/make-money/investments/":
    #         sms_confirmed_id.append(int(row[0]))
    #     if int(row[0]) not in passport_correct and row[9] == "passport_correct::/make-money/investments/":
    #         passport_correct.append(int(row[0]))
    #     if int(row[0]) not in roditeli and row[9] == "teenager-plate-button_click::/make-money/investments/":
    #         roditeli.append(int(row[0]))
    # print(f"Зашли на сайт - От предыдущего этапа: {len(entered_site)/len(entered_site)*100}% | От вошедших: {len(entered_site)/len(entered_site)*100}% | [{len(entered_site)} чел]")
    # print(f"Сделал что-то - От предыдущего этапа: {len(done_smt) / len(entered_site) * 100}% | От вошедших: {len(done_smt) / len(entered_site) * 100}% | [{len(done_smt)} чел]")
    # print(f"Начали заполнять форму - От предыдущего этапа: {len(form_opened)/len(done_smt)*100}% | От вошедших: {len(form_opened)/len(entered_site)*100}% | [{len(form_opened)} чел]")
    # print(f"Заполнили форму - От предыдущего этапа: {len(filled_form_opened)/len(form_opened)*100}% | От вошедших: {len(filled_form_opened)/len(entered_site)*100}% | [{len(filled_form_opened)} чел]")
    # print(f"Подтвердили смс - От предыдущего этапа: {len(sms_confirmed_id)/len(filled_form_opened)*100}% | От вошедших: {len(sms_confirmed_id)/len(entered_site)*100}% | [{len(sms_confirmed_id)} чел]")
    # print(f"Подтвердили паспорт - От предыдущего этапа: {len(passport_correct) / len(sms_confirmed_id) * 100}% | От вошедших: {len(passport_correct) / len(entered_site) * 100}% | [{len(passport_correct)} чел]")
    # print(f"Попросили родителей - От предыдущего этапа: {len(roditeli) / len(sms_confirmed_id) * 100}% | От вошедших: {len(roditeli) / len(entered_site) * 100}% | [{len(roditeli)} чел]")

    # user_lst_rec = defaultdict()
    # for i, row in enumerate(reader):
    #     if i == 0:
    #         continue
    #     user_lst_rec[int(row[0])] = row[8]
    # lst_req = defaultdict(lambda: 0)
    # for item in user_lst_rec.items():
    #     lst_req[item[1]] += 1
    # srtd_dct = sorted(lst_req.items(), key=lambda x:x[1])
    # for item in srtd_dct:
    #     print(f'{item[0]} - кол-во ушедших {item[1]}')

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


    def ip_to_city(ip_address):
        ip_address = ip_address.replace('xxx', '111')
        """
        Convert an IPv4 or IPv6 address to city location using ipinfo.io API.

        Args:
            ip_address (str): The IP address to look up.

        Returns:
            str: City name or error message.
        """
        try:
            response = requests.get(f"https://ipinfo.io/{ip_address}/json")
            data = response.json()

            if 'city' in data:
                return data['city']
            else:
                return f"City not found for IP: {ip_address}"
        except Exception as e:
            return f"Error: {e}"

    def missed_id_to_city(city, ip_address):
        if city == '':
            city = ip_to_city(ip_address)
        return city

    id_dict = dict()
    for i, row in enumerate(reader):
        if i == 0:
            continue
        if int(row[0]) not in id_dict.keys():
            id_dict[int(row[0])] = [get_os(row[3]), row[10], [split_params3(row[8])], [row[9].split('::')[0]], [row[12], row[12]], None, row[-1]]
        else:
            id_dict[int(row[0])][2].append(split_params3(row[8]))
            id_dict[int(row[0])][3].append(row[9].split('::')[0])
            id_dict[int(row[0])][4][1] = row[12]
            if row[9] == "passport_correct::/make-money/investments/":
                id_dict[int(row[0])][5] = row[12]
    print(id_dict)
