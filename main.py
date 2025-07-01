import matplotlib
import csv

with open('investments.csv', newline='', encoding='utf-8') as f:
    reader = csv.reader(f)
    entered_site = []
    done_smt = []
    sms_confirmed_id = []
    form_opened = []
    filled_form_opened = []
    passport_correct = []
    roditeli = []
    for i, row in enumerate(reader):
        # passthrough = False
        # for op in ["windows", "mac", "ubuntu", "linux", "chrome"]:
        #     if op in row[3]:
        #         passthrough = True
        #         continue
        # if not passthrough:
        #     continue
        if i == 0:
            continue
        if int(row[0]) not in entered_site:
            entered_site.append(int(row[0]))
        if int(row[0]) not in form_opened and row[9] in ["fullName_focus::/make-money/investments/", "phone_focus::/make-money/investments/", "birthday_focus::/make-money/investments/"]:
            form_opened.append(int(row[0]))
        if int(row[0]) not in done_smt and row[8] != "Product::smev::View":
            done_smt.append(int(row[0]))
        if int(row[0]) not in filled_form_opened and row[8] == "Product::sms-verification::View":
            filled_form_opened.append(int(row[0]))
        if int(row[0]) not in sms_confirmed_id and row[9] == "sms-input_correct::/make-money/investments/":
            sms_confirmed_id.append(int(row[0]))
        if int(row[0]) not in passport_correct and row[9] == "passport_correct::/make-money/investments/":
            passport_correct.append(int(row[0]))
        if int(row[0]) not in roditeli and row[9] == "teenager-plate-button_click::/make-money/investments/":
            roditeli.append(int(row[0]))
    print(f"Зашли на сайт - От предыдущего этапа: {len(entered_site)/len(entered_site)*100}% | От вошедших: {len(entered_site)/len(entered_site)*100}% | [{len(entered_site)} чел]")
    print(f"Сделал что-то - От предыдущего этапа: {len(done_smt) / len(entered_site) * 100}% | От вошедших: {len(done_smt) / len(entered_site) * 100}% | [{len(done_smt)} чел]")
    print(f"Начали заполнять форму - От предыдущего этапа: {len(form_opened)/len(done_smt)*100}% | От вошедших: {len(form_opened)/len(entered_site)*100}% | [{len(form_opened)} чел]")
    print(f"Заполнили форму - От предыдущего этапа: {len(filled_form_opened)/len(form_opened)*100}% | От вошедших: {len(filled_form_opened)/len(entered_site)*100}% | [{len(filled_form_opened)} чел]")
    print(f"Подтвердили смс - От предыдущего этапа: {len(sms_confirmed_id)/len(filled_form_opened)*100}% | От вошедших: {len(sms_confirmed_id)/len(entered_site)*100}% | [{len(sms_confirmed_id)} чел]")
    print(f"Подтвердили паспорт - От предыдущего этапа: {len(passport_correct) / len(sms_confirmed_id) * 100}% | От вошедших: {len(passport_correct) / len(entered_site) * 100}% | [{len(passport_correct)} чел]")
    print(f"Попросили родителей - От предыдущего этапа: {len(roditeli) / len(sms_confirmed_id) * 100}% | От вошедших: {len(roditeli) / len(entered_site) * 100}% | [{len(roditeli)} чел]")