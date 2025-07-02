
with open('reformatted_invest.txt', 'r', encoding='utf-8') as f:
    done_smt = 0
    form_opened = 0
    filled_form_opened = 0
    sms_confirmed_id = 0
    passport_correct = 0
    parents = 0
    id_dict = eval(f.read())
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

    print(f"Зашли на сайт - От предыдущего этапа: {entered_site/entered_site*100}% | От вошедших: {(entered_site)/(entered_site)*100}% | [{(entered_site)} чел]")
    print(f"Сделал что-то - От предыдущего этапа: {done_smt / entered_site * 100}% | От вошедших: {(done_smt) / (entered_site) * 100}% | [{(done_smt)} чел]")
    print(f"Начали заполнять форму - От предыдущего этапа: {form_opened/done_smt*100}% | От вошедших: {(form_opened)/(entered_site)*100}% | [{(form_opened)} чел]")
    print(f"Заполнили форму - От предыдущего этапа: {(filled_form_opened)/(form_opened)*100}% | От вошедших: {(filled_form_opened)/(entered_site)*100}% | [{(filled_form_opened)} чел]")
    print(f"Подтвердили смс - От предыдущего этапа: {(sms_confirmed_id)/(filled_form_opened)*100}% | От вошедших: {(sms_confirmed_id)/(entered_site)*100}% | [{(sms_confirmed_id)} чел]")
    print(f"Подтвердили паспорт - От предыдущего этапа: {(passport_correct) / (sms_confirmed_id) * 100}% | От вошедших: {(passport_correct) / (entered_site) * 100}% | [{(passport_correct)} чел]")
    print(f"Попросили родителей - От предыдущего этапа: {(parents) / (sms_confirmed_id) * 100}% | От вошедших: {(parents) / (entered_site) * 100}% | [{(parents)} чел]")