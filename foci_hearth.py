import math
import os
import time
import json
from builtins import print

from django.utils import timezone

import django

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'rudez.settings')
django.setup()

from codewrite.models import CollisionRange, HearthCollision, HearthDtp, AllDtpCard, HearthTmpDtp, HearthDtpDis, HearthDtpTmp, HearthCollisionTmp


# Вывод спика цепочек очагов
def turn_hearth():
    # Получение всех записей ДТП которые сравнивали по дистанции
    col_range = CollisionRange.objects.all().order_by('id_collision_1')
    list_collision_hearth = []
    # Сериализация списка очагов и запись во временную таблицу
    for item_first in col_range:
        tmp_sec = []
        for item_second in col_range:
            if item_first.id_collision_1 == item_second.id_collision_1 and \
                    arr_collistion_type(item_first.icon_type) == arr_collistion_type(item_second.icon_type):
                tmp_sec.append({
                    'id': item_second.id_collision_2
                })
        # Добавление первого элемента к остальному списку
        tmp_sec.append({
            'id': item_first.id_collision_1
        })
        # Переписвание вложенного списка без дубликатов
        col_by_dict = {i['id']: i for i in tmp_sec}
        new_items_list = list(col_by_dict.values())
        # Сортировка по возрастанию
        sort_list = sorted(new_items_list, key=lambda x: x['id'])
        # Если ДТП в очаге больше 2
        if len(sort_list) > 2:
            # Формируем строку из элементов списка
            tmp = '||'.join(str(x['id']) for x in sort_list)
            # Создаем объект типа HearthTmpDtp
            list_collision_hearth.append(HearthTmpDtp(year=item_first.dtp_year, month=item_first.dtp_month,
                                                      quarter=item_first.dtp_quarter, icon_type=item_first.icon_type,
                                                      num_dtp=tmp))
    if rewrite_not_duplicate(list_collision_hearth, False):
        return True
    else:
        return False


# Функция получения значения по iconType
def arr_collistion_type(id_item):
    tmp = {
        0: '',
        1: "Столкновение",
        2: "Столкновение",
        3: "Опрокидывание",
        4: "Опрокидывание",
        5: "Наезд на препятствие",
        6: "Наезд на препятствие",
        7: "Наезд на велосипедиста",
        8: "Наезд на велосипедиста",
        9: "Наезд на пассажира",
        10: "Наезд на пассажира",
        11: "Съезд с дороги",
        12: "Съезд с дороги",
        13: "Наезд на стоящее ТС",
        14: "Наезд на стоящее ТС",
        15: "Наезд на пешехода",
        16: "Наезд на пешехода",
        17: "Иной вид ДТП",
        18: "Иной вид ДТП",
        19: "Очаг с погибими",
        20: "Очаг с погибими"
    }
    return tmp[id_item]


# Перезапись очагов без дубликатов
def rewrite_not_duplicate(list_request, first_id_if):
    # Очищаем прошлые записи
    HearthTmpDtp.objects.all().delete()
    # Добавляем объекты в таблицу
    HearthTmpDtp.objects.bulk_create(list_request)
    # Получаем элементы из временной таблицы без дубликатов
    tmp_list_hearth = HearthTmpDtp.objects.all().distinct('num_dtp').values()
    list_dis = []
    # Если нужно добавить id первого элемента ДТП в поле (для сортировки)
    if first_id_if:
        for item_hearth in tmp_list_hearth:
            # Получение первого id из списка ДТП очагов
            first_id = item_hearth['num_dtp'].split("||")[0]
            list_dis.append(HearthDtpDis(year=item_hearth['year'], month=item_hearth['month'],
                                         quarter=item_hearth['quarter'], icon_type=item_hearth['icon_type'],
                                         num_dtp=item_hearth['num_dtp'], id_first_dtp=first_id))
    else:
        for item_hearth in tmp_list_hearth:
            list_dis.append(HearthDtpDis(year=item_hearth['year'], month=item_hearth['month'],
                                         quarter=item_hearth['quarter'], icon_type=item_hearth['icon_type'],
                                         num_dtp=item_hearth['num_dtp']))
    # Очистка таблицы в которую произайдет запись
    HearthDtpDis.objects.all().delete()
    # Запись в другую таблицу
    HearthDtpDis.objects.bulk_create(list_dis)
    # Очистка временной таблицы
    HearthTmpDtp.objects.all().delete()
    return True


# Склеиавние ДПТ очагов
def concat_hearth(foci_list):
    list_t = []
    for item_foci_1 in foci_list:
        # Получение первого списка ДТП
        tmp_1 = item_foci_1['list']
        for item_foci_2 in foci_list:
            # Получение второго списка ДТП
            tmp_2 = item_foci_2['list']
            # Из списка сделать строку добавив разделитель
            tmp = '||'.join(str(x) for x in tmp_1)
            # Если id очага разный и есть хоть один повторяющийся элемент, однаковый год|месяц|тип нарушения
            if item_foci_1['id'] != item_foci_2['id'] and len(find_intersection_not_param(tmp_1, tmp_2)) > 0 and \
                    item_foci_1['year'] == item_foci_2['year'] and \
                    item_foci_1['quarter'] == item_foci_2['quarter'] and \
                    arr_collistion_type(item_foci_1['icon_type']) == arr_collistion_type(item_foci_2['icon_type']):
                # Склеивание ДТП
                tmp = tmp_1 + tmp_2
                # Удаление дубликатов
                url_by_dict = {i: i for i in tmp}
                new_items_list_dtp = list(url_by_dict.values())
                # Сортировка по возрастанию
                sort_list = sorted(new_items_list_dtp, key=lambda x: x)
                # Из списка сделать строку с добавлением разделителя
                tmp = '||'.join(str(x) for x in sort_list)
            # Создание обхектов типа HearthTmpDtp для добавление в таблицу
            list_t.append(HearthTmpDtp(year=item_foci_1['year'], month=item_foci_1['month'],
                                       quarter=item_foci_1['quarter'],
                                       icon_type=item_foci_1['icon_type'],
                                       num_dtp=tmp))
    # Удаление дубликатов
    if rewrite_not_duplicate(list_t, True):
        return True
    else:
        return False


# Удаление дубликатов -- Добавление очагов и их ДТП
def create_hearth():
    # Получение временных очагов и их ДТП
    list_dtp_tmp = HearthDtpDis.objects.all().order_by('id_first_dtp').values()
    list_foci = list(list_dtp_tmp)
    # Удаляем копии очагов с меньшим кол-вом ДТП
    for i in range(len(list_foci)):
        if i+1 < len(list_foci):
            if list_foci[i]['id_first_dtp'] == list_foci[i+1]['id_first_dtp']:
                count_list_1 = len(list_foci[i]['num_dtp'].split("||"))
                count_list_2 = len(list_foci[i+1]['num_dtp'].split("||"))
                if count_list_1 < count_list_2:
                    del list_foci[i]
                if count_list_1 > count_list_2:
                    del list_foci[i+1]
    # Запись в таблицу HearthDtp
    list_collision_hearth = []
    for item_foci in list_foci:
        now = timezone.now()
        # Создание объекта
        list_dtp = item_foci['num_dtp'].split("||")
        hearth = HearthDtpTmp.objects.create(year=item_foci['year'], month=item_foci['month'],
                                             quarter=item_foci['quarter'], create=now,
                                             count_dtp=len(list_dtp), type=item_foci['icon_type'])
        hearth.save()
        # Добавление связки очага и его ДТП
        for id_nested_dtp in list_dtp:
            # Есть нет ДТП по id
            if AllDtpCard.objects.filter(id=id_nested_dtp):
                cr_obj = AllDtpCard.objects.get(id=id_nested_dtp)
                list_collision_hearth.append(HearthCollisionTmp(hid=hearth, cid=cr_obj))
            else:
                print('error')
                print(id_nested_dtp)
                exit()
    # Добавление связки очаг-дтп
    HearthCollisionTmp.objects.bulk_create(list_collision_hearth)
    return True


# Поиск по вхождению по параметру
def find_intersection(l1, l2, param):
    out = []
    i1 = 0
    i2 = 0
    while (i1 < len(l1)) and (i2 < len(l2)):
        if l1[i1][param] > l2[i2][param]:
            i2 += 1
        elif l1[i1][param] < l2[i2][param]:
            i1 += 1
        else:  # l1[i1] == l2[i2]
            out.append(l1[i1])
            i1 += 1
            i2 += 1
    return out


# Поиск по вхождению без параметра
def find_intersection_not_param(l1, l2):
    out = []
    i1 = 0
    i2 = 0
    while (i1 < len(l1)) and (i2 < len(l2)):
        if l1[i1] > l2[i2]:
            i2 += 1
        elif l1[i1] < l2[i2]:
            i1 += 1
        else:  # l1[i1] == l2[i2]
            out.append(l1[i1])
            i1 += 1
            i2 += 1
    return out


# Главная функция скрипта
if __name__ == '__main__':
    print('start')
    # Фиксация времени начал скрипта
    start_time = time.time()
    # Очистка таблицы HearthCollision
    HearthCollision.objects.all().delete()
    # Очистка таблицы HearthDtp
    HearthDtp.objects.all().delete()
    # Записываем временные очаги во временную таблицу
    if turn_hearth():
        tmp_list = []
        # Получаем очаги без дубликатов
        list_dtp_foci = HearthDtpDis.objects.all().distinct('num_dtp').values()
        # Сериализация объектов очагов
        for item in list_dtp_foci:
            # Из одной строки делаем список по разделителю
            mylist = [int(x) for x in item['num_dtp'].split('||')]
            tmp_list.append({
                'id': item['id'],
                'list': mylist,
                'year': item['year'],
                'month': item['month'],
                'quarter': item['quarter'],
                'icon_type': item['icon_type'],
            })
        # Слеивание ДТП очагов
        if concat_hearth(tmp_list):
            # Из временной таблицы записываем очаги
            if create_hearth():
                print("--- %s seconds ---" % (time.time() - start_time))
                print('end')
                exit()
            else:
                print('Ошибка при записи очагов')
        else:
            print('Ошибка при склеивании очагов')
    else:
        print('Ошибка при создании предполагаемых очагов')
