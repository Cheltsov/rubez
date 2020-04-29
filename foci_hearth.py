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


# Функция получения значения по iconType
def arr_collistion_type(item):
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
    return tmp[item]


def concat_hearth(foci_list):
    foci_list_new = []
    list_t = []
    for item_foci_1 in foci_list:
        # Получение первого списка ДТП
        tmp_1 = item_foci_1['list']
        for item_foci_2 in foci_list:
            # Получение второго списка ДТП
            tmp_2 = item_foci_2['list']
            tmp = '||'.join(str(x) for x in tmp_1)
            if item_foci_1['id'] != item_foci_2['id'] and len(find_intersection_not_param(tmp_1, tmp_2)) > 0 and \
                    item_foci_1['year'] == item_foci_2['year'] and \
                    item_foci_1['quarter'] == item_foci_2['quarter'] and \
                    arr_collistion_type(item_foci_1['icon_type']) == arr_collistion_type(item_foci_2['icon_type']):

                # Объединение массивов
                tmp = tmp_1 + tmp_2
                # Удаление дубликатов
                url_by_dict = {i: i for i in tmp}
                new_items_list_dtp = list(url_by_dict.values())
                sort_list = sorted(new_items_list_dtp, key=lambda x: x)
                tmp = '||'.join(str(x) for x in sort_list)

            list_t.append(HearthTmpDtp(year=item_foci_1['year'], month=item_foci_1['month'],
                                       quarter=item_foci_1['quarter'],
                                       icon_type=item_foci_1['icon_type'],
                                       num_dtp=tmp))

    HearthTmpDtp.objects.all().delete()
    HearthTmpDtp.objects.bulk_create(list_t)
    tmp_list = HearthTmpDtp.objects.all().distinct('num_dtp').values()
    list_dis = []
    for item in tmp_list:
        first_id = item['num_dtp'].split("||")[0]
        list_dis.append(HearthDtpDis(year=item['year'], month=item['month'],
                                     quarter=item['quarter'], icon_type=item['icon_type'], num_dtp=item['num_dtp'],
                                     id_first_dtp=first_id))
    HearthDtpDis.objects.all().delete()
    HearthDtpDis.objects.bulk_create(list_dis)
    HearthTmpDtp.objects.all().delete()
    return True


# Вывод спика цепочек очагов
def turn_hearth():
    # Получение всех записей дпт которые сравнивали по дистанции
    col_range = CollisionRange.objects.all().order_by('id_collision_1')
    foci_list = []
    list_collision_hearth = []
    # Сериализация списка очагов
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
        if len(sort_list) > 2:
            tmp = '||'.join(str(x['id']) for x in sort_list)
            list_collision_hearth.append(HearthTmpDtp(year=item_first.dtp_year, month=item_first.dtp_month,
                                                      quarter=item_first.dtp_quarter, icon_type=item_first.icon_type,
                                                      num_dtp=tmp))

    HearthTmpDtp.objects.all().delete()
    HearthTmpDtp.objects.bulk_create(list_collision_hearth)
    tmp_list = HearthTmpDtp.objects.all().distinct('num_dtp')
    list_dis = []
    for item in tmp_list:
        list_dis.append(HearthDtpDis(year=item.year, month=item.month,
                                     quarter=item.quarter, icon_type=item.icon_type, num_dtp=item.num_dtp))
    HearthDtpDis.objects.all().delete()
    HearthDtpDis.objects.bulk_create(list_dis)
    HearthTmpDtp.objects.all().delete()
    return True


def last_interation_duplicate():
    list_dtp_foci = HearthDtpDis.objects.all().order_by('id_first_dtp').values()
    list_dtp_foci = list(list_dtp_foci)
    for i in range(len(list_dtp_foci)):
        if i+1 < len(list_dtp_foci):
            if list_dtp_foci[i]['id_first_dtp'] == list_dtp_foci[i+1]['id_first_dtp']:
                count_list_1 = len(list_dtp_foci[i]['num_dtp'].split("||"))
                count_list_2 = len(list_dtp_foci[i+1]['num_dtp'].split("||"))
                if count_list_1 < count_list_2:
                    del list_dtp_foci[i]
                if count_list_1 > count_list_2:
                    del list_dtp_foci[i+1]

    list_collision_hearth = []
    for item_foci in list_dtp_foci:
        now = timezone.now()
        # Создание объекта
        list_dtp = item_foci['num_dtp'].split("||")
        hearth = HearthDtpTmp.objects.create(year=item_foci['year'], month=item_foci['month'],
                                             quarter=item_foci['quarter'], create=now,
                                             count_dtp=len(list_dtp), type=item_foci['icon_type'])
        hearth.save()
        for id_nested_dtp in list_dtp:
            if AllDtpCard.objects.filter(id=id_nested_dtp):
                cr_obj = AllDtpCard.objects.get(id=id_nested_dtp)
                list_collision_hearth.append(HearthCollisionTmp(hid=hearth, cid=cr_obj))
            else:
                print('error')
                print(id_nested_dtp)
                exit()
    HearthCollisionTmp.objects.bulk_create(list_collision_hearth)
    print("Готово")
    exit()


# Создание записи Очага и получение списка связи Hearth и Dtp
def get_list_heath(list_dtp):
    list_collision_hearth = []
    log_error = []
    for item_foci in list_dtp:
        now = timezone.now()
        # Если вложенных ДПТ больше 2 (начало учитывается)
        if len(item_foci['list']) > 2:
            # Создание объекта
            hearth = HearthDtp.objects.create(year=item_foci['year'], month=item_foci['month'],
                                              quarter=item_foci['quarter'], created=now,
                                              count_dtp=len(item_foci['list']), type=item_foci['icon_type'])
            hearth.save()
            for item_nested_dtp in item_foci['list']:
                if AllDtpCard.objects.filter(id=item_nested_dtp['id']):
                    cr_obj = AllDtpCard.objects.get(id=item_nested_dtp['id'])
                    list_collision_hearth.append(HearthCollision(hid=hearth, cid=cr_obj))
                else:
                    log_error.append({'id_dtp_all': item_nested_dtp['id']})
    with open('media/json_error_log.json', 'w') as outfile:
        json.dump(list(log_error), outfile)
    return list_collision_hearth


# Главная функция скрипта
if __name__ == '__main__':
    print('start')

    last_interation_duplicate()
    exit()

    if turn_hearth():
        tmp_list = []
        list_dtp_foci = HearthDtpDis.objects.all().distinct('num_dtp').values()
        for item in list_dtp_foci:
            mylist = [int(x) for x in item['num_dtp'].split('||')]
            tmp_list.append({
                'id': item['id'],
                'list': mylist,
                'year': item['year'],
                'month': item['month'],
                'quarter': item['quarter'],
                'icon_type': item['icon_type'],
            })
        # Слеивание дпт очага
        if concat_hearth(tmp_list):
            print('000')
            exit()



    # tmp = HearthTmpDtp.objects.all().distinct('num_dtp').order_by('num_dtp').values()
    # for i in tmp:
    #     print(i)

    # Фиксация времени начал скрипта
    start_time = time.time()
    # Очистка таблицы HearthCollision
    HearthCollision.objects.all().delete()
    # Очистка таблицы HearthDtp
    HearthDtp.objects.all().delete()
    # Вывод Дпт Очагов
    new_foci = turn_hearth()
    # Получение списка связи Hearth и Dtp и создание Hearth
    list_collision_hearth = get_list_heath(new_foci)
    # Создание связи очага и ДПТ
    HearthCollision.objects.bulk_create(list_collision_hearth)
    print("--- %s seconds ---" % (time.time() - start_time))
    print('end')
