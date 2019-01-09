import json
from os.path import join
from datetime import datetime
from ets.ets_mysql_lib import MysqlConnection as Mc
from ets.ets_excel_creator import Excel
from queries import *
from templates import *
from config import *
from ets.ets_ds_lib import OPERATION_GET_COMMISSION, __OPERATIONS
from ets.ets_email_lib import Report

url = __OPERATIONS[OPERATION_GET_COMMISSION]['url']
token = __OPERATIONS[OPERATION_GET_COMMISSION]['headers']['Postman-Token']

date_str = datetime.now().strftime('%d.%m.%Y')

cn = Mc(connection=Mc.MS_44_1_CONNECT)
cn.connect()
cn_edo = Mc(connection=Mc.MS_EDO_CONNECT)
cn_edo.connect()


def commission_worker(procedure_bd_info):
    excel_data_with_account = []
    excel_data_no_account = []
    for request_info in procedure_bd_info:

        if not request_info['registration_number']:
            continue

        if request_info['additional_data']:
            request_info['additional_data'] = json.loads(request_info['additional_data'])
            request_info['account_id'] = None if type(request_info['additional_data']) is list else \
                request_info['additional_data']['specialAccount'].get('id', None)
        else:
            request_info['account_id'] = None

        # получаем сведения о банке и спецсчете
        if request_info['account_id']:
            request_info['bank_code'], request_info['account_number'] = \
                cn_edo.execute_query(get_edo_info_query % request_info['account_id'])[0]
        else:
            request_info['bank_code'] = None
            request_info['account_number'] = None

        # высчитываем величину комиссии для списания
        request_info['max_sum_1_percent'] = request_info['max_sum'] / 100
        request_info['max_sum_nds'] = request_info['max_sum_1_percent'] * nds_param_DEC

        if request_info['is_smp'] == 'Да':
            request_info['take_commission'] = False if \
                request_info['max_sum_1_percent'] < (100 + (nds_param_DEC * 100)) else True
            if request_info['max_sum_1_percent'] >= smp_max_block_DEC:
                request_info['nds'] = smp_max_block_DEC * (nds_param_DEC * 100) / (100 + (nds_param_DEC * 100))
                request_info['to_block'] = smp_max_block_DEC
            else:
                request_info['nds'] = request_info['max_sum_1_percent'] * (nds_param_DEC * 100) / \
                                      (100 + (nds_param_DEC * 100))
                request_info['to_block'] = request_info['max_sum_1_percent']
        else:
            request_info['take_commission'] = False if request_info['max_sum_1_percent'] < 100 else True
            if request_info['max_sum_1_percent'] >= no_smp_max_block_DEC:
                request_info['nds'] = no_smp_max_block_DEC * nds_param_DEC
                request_info['to_block'] = no_smp_max_block_DEC + no_smp_max_block_DEC * nds_param_DEC
            else:
                request_info['nds'] = request_info['max_sum_nds']
                request_info['to_block'] = request_info['max_sum_1_percent'] + request_info['max_sum_nds']

        request_info['nds'] = round(request_info['nds'], 2)
        request_info['to_block'] = round(request_info['to_block'], 2)

        # формируем subject
        request_info['subject_full'] = ' '.join(('Списание платы за участие в закупке',
                                                 request_info['registration_number'],
                                                 request_info['supplier_full_name'],
                                                 'на сумму',
                                                 str(request_info['to_block']),
                                                 'в т.ч. НДС (20%)',
                                                 str(request_info['nds'])))

        request_info['subject_short'] = ' '.join(('Списание платы за участие в закупке',
                                                  request_info['registration_number'],
                                                  request_info['supplier_short_name'] if request_info['supplier_short_name']
                                                  else '',
                                                  'на сумму',
                                                  str(request_info['to_block']),
                                                  'в т.ч. НДС (20%)',
                                                  str(request_info['nds'])))

        request_info['subject'] = request_info['subject_short'] if len(request_info['subject_full']) > 210 \
            else request_info['subject_full']

        # получаем данные о статусе списания комиссии
        commission_status = cn_edo.execute_query(get_commission_status_info_query % request_info, dicted=True)[0]
        request_info.update(commission_status)
        request_info['real_operation_status_text'] = request_info['real_operation_status_text'] \
            if request_info['real_operation_status_text'] else 'Комиссия не списывалась'
        request_info['real_amount'] = request_info['real_amount'] \
            if request_info['real_amount'] else '0.00'

        # получаем action по комиссии
        if request_info['take_commission']:
            if request_info['account_number']:
                request_info['action'] = ''
            else:
                request_info['action'] = 'Списание комиссии со счета площадки'
        else:
            request_info['action'] = 'Размер платы менее 100 рублей, плата не взымается'

        # формируем curl
        request_info.update({'url': url, 'token': token})
        request_info['curl'] = curl_template % request_info

        # формируем json
        request_info['json'] = json.dumps({'account': request_info['account_number'],
                                           'bankId': request_info['bank_code'],
                                           'amount': str(request_info['to_block']),
                                           'appId': str(request_info['registration_number']) + '-' + str(
                                                   request_info['request_id']
                                           ),
                                           'ground': request_info['subject']})

        # формируем уведомление
        request_info['date_str'] = date_str

        if request_info['provision_amount']:
            request_info['notification'] = provision_notification_template % request_info
        else:
            request_info['notification'] = no_provision_notification_template % request_info

        # формируем наборы данных для файлов со спецсчетом и без
        if request_info['account_number']:
            excel_data_with_account.append([request_info['registration_number'],
                                            request_info['request_id'],
                                            request_info['supplier_full_name'],
                                            request_info['supplier_inn'],
                                            request_info['supplier_kpp'],
                                            request_info['supplier_address'],
                                            request_info['contract_datetime'],
                                            request_info['is_smp'],
                                            decimal.Decimal(request_info['provision_amount']),
                                            request_info['max_sum'],
                                            request_info['to_block'],
                                            request_info['real_amount'],  # Списано (с НДС) (фактические данные)
                                            request_info['bank_code'],
                                            request_info['account_number'],
                                            request_info['json'] if request_info['take_commission'] else '',
                                            request_info['curl'] if request_info['take_commission'] else '',
                                            request_info['real_status'],  # Идентификатор ответа
                                            '\n'.join([request_info['action'], request_info['real_operation_status_text']]),
                                            request_info['real_description'],  # Комментарий
                                            request_info['notification']])
        else:
            excel_data_no_account.append([request_info['registration_number'],
                                          request_info['request_id'],
                                          request_info['supplier_full_name'],
                                          request_info['supplier_inn'],
                                          request_info['supplier_kpp'],
                                          request_info['supplier_address'],
                                          request_info['contract_datetime'],
                                          request_info['is_smp'],
                                          decimal.Decimal(request_info['provision_amount']),
                                          request_info['max_sum'],
                                          request_info['to_block'],
                                          request_info['notification']])

    return excel_data_with_account, excel_data_no_account

# получаем данные по всем процедурам за указанный период
procedure_bd_info = cn.execute_query(get_procedure_request_info_query, dicted=True)
excel_data_with_acc, excel_data_no_acc = commission_worker(procedure_bd_info)


# формируем excel файл
excel = Excel()
excel_list_with_account = excel.create_list('Списанные комиссии')
excel_list_with_account.write_data_from_iter(excel_data_with_acc, top_line=['Процедура',
                                                                            'Id заявки',
                                                                            'Наименование организации',
                                                                            'ИНН',
                                                                            'КПП',
                                                                            'Адрес',
                                                                            'Дата заключения контракта',
                                                                            'СМП',
                                                                            'Обеспечение заявки',
                                                                            'НМЦК',
                                                                            'Сумма к списанию (с НДС)',
                                                                            'Фактически списано',
                                                                            'Банк',
                                                                            'Аккаунт',
                                                                            'Json',
                                                                            'Curl',
                                                                            'Идентификатор ответа',
                                                                            'Статус списания',
                                                                            'Комментарий',
                                                                            'Уведомление'])


excel_list_with_account.set_column_width(150, 70, 250, 100, 100, 250, 80, 50, 100, 100,
                                         100, 100, 70, 150, 550, 550, 250, 100, 150, 550, 550)


excel_list_excel_data_no_account = excel.create_list('Комиссии для ручной обработки')
excel_list_excel_data_no_account.write_data_from_iter(excel_data_no_acc, top_line=['Процедура',
                                                                                   'Id заявки',
                                                                                   'Наименование организации',
                                                                                   'ИНН',
                                                                                   'КПП',
                                                                                   'Адрес',
                                                                                   'Дата заключения контракта',
                                                                                   'СМП',
                                                                                   'Обеспечение заявки',
                                                                                   'НМЦК',
                                                                                   'Сумма к списанию (с НДС)',
                                                                                   'Уведомление'])

excel_list_excel_data_no_account.set_column_width(150, 70, 250, 100, 100, 250, 80, 50, 100, 100, 100, 550)
excel_file = excel.save_file(excel_dir, file_name=excel_file_name)

good_commission_info = cn.execute_query(get_good_commission_info_query, dicted=True)
good_commission_excel_data_with_acc, good_commission_excel_data_no_acc = commission_worker(good_commission_info)

excel_2 = Excel()
excel_list_good_commission_with_acc = excel_2.create_list('К списанию со спецсчета')
excel_list_good_commission_with_acc.write_data_from_iter(good_commission_excel_data_with_acc, top_line=['Процедура',
                                                                                                        'Id заявки',
                                                                                                        'Наименование организации',
                                                                                                        'ИНН',
                                                                                                        'КПП',
                                                                                                        'Адрес',
                                                                                                        'Дата подписания протокола',
                                                                                                        'СМП',
                                                                                                        'Обеспечение заявки',
                                                                                                        'НМЦК',
                                                                                                        'Сумма к списанию (с НДС)',
                                                                                                        'Фактически списано',
                                                                                                        'Банк',
                                                                                                        'Аккаунт',
                                                                                                        'Json',
                                                                                                        'Curl',
                                                                                                        'Идентификатор ответа',
                                                                                                        'Статус списания',
                                                                                                        'Комментарий',
                                                                                                        'Уведомление'])

excel_list_good_commission_with_acc.set_column_width(150, 70, 250, 100, 100, 250, 80, 50, 100, 100,
                                                     100, 100, 70, 150, 550, 550, 250, 100, 150, 550, 550)

excel_list_good_commission_no_acc = excel_2.create_list('К списанию вручную')
excel_list_good_commission_no_acc.write_data_from_iter(good_commission_excel_data_no_acc, top_line=['Процедура',
                                                                                                    'Id заявки',
                                                                                                    'Наименование организации',
                                                                                                    'ИНН',
                                                                                                    'КПП',
                                                                                                    'Адрес',
                                                                                                    'Дата подписания протокола',
                                                                                                    'СМП',
                                                                                                    'Обеспечение заявки',
                                                                                                    'НМЦК',
                                                                                                    'Сумма к списанию (с НДС)',
                                                                                                    'Уведомление'])

excel_list_good_commission_no_acc.set_column_width(150, 70, 250, 100, 100, 250, 80, 50, 100, 100, 100, 550)


excel_list_error_commission = excel_2.create_list('К возврату')
error_commission_info = cn.execute_query(get_error_commission_info_query)
excel_list_error_commission.write_data_from_iter(error_commission_info, top_line=['Номер закупки',
                                                                                  'Дата публикации протокола',
                                                                                  'Участник с которого некорректно списана комиссия',
                                                                                  'Дата подписания контракта',
                                                                                  'Участник с которого надлежит списать комиссию'])

excel_list_error_commission.set_column_width(150, 120, 150, 120, 150)
excel_file_2 = excel_2.save_file(excel_dir, file_name=excel_2_file_name)

cn.disconnect()
cn_edo.disconnect()

# отправляем сообщение
report = Report(subject, recipients=recipients)
report.add_file(join(excel_dir, excel_file))
report.add_file(join(excel_dir, excel_file_2))
report.update_message(message)
report.send_letter()


