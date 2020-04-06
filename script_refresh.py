import math
import os
import time
from datetime import datetime

import django
import json

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'rudez.settings')
django.setup()

from codewrite.models import SkpdiDtpCard, AllDtpCardHistory, AllDtpLastIndex, StatGibddDtpCard, AllDtpCard, AllDtpCollisionType
from django.db import transaction

from django.db import connection


# Функция создания списка SKPDI и STAT_GIPDD
def create_skpdi_stat():
    # Если есть записи в таблице all_dtp_last_index
    if AllDtpLastIndex.objects.count() > 0:
        # Получение последней записи в таблице
        last_index_history = AllDtpLastIndex.objects.latest('id')
        # Получение списка записей SKPDI
        arr_skpdi = SkpdiDtpCard.objects.filter(id__lte=last_index_history.skpdi_id).order_by(
            'id')
        if not arr_skpdi:
            arr_skpdi = SkpdiDtpCard.objects.filter(coordinates__isnull=False).order_by('id')
        # Получение списка записей STAT_GIPDD
        arr_stat_gpdd = StatGibddDtpCard.objects.filter(sid__lte=last_index_history.stat_gibdd_id).order_by('sid')
        if not arr_stat_gpdd:
            arr_stat_gpdd = StatGibddDtpCard.objects.filter(lat__isnull=False).order_by('sid')
    else:
        arr_skpdi = SkpdiDtpCard.objects.filter(coordinates__isnull=False).order_by('id')
        arr_stat_gpdd = StatGibddDtpCard.objects.filter(lat__isnull=False).order_by('sid')
    return arr_skpdi, arr_stat_gpdd


# Функция для явного сказания массива collision_dtp_array, arr_skpdi, arr_stat_gpdd
def my_same(arr_skpdi, arr_stat_gpdd):
    arr_skpdi_list = []
    arr_stat_list = []
    collision_dtp_array = []
    for sk in arr_skpdi:
        arr_skpdi_list.append(sk.id)
    for st in arr_stat_gpdd:
        arr_stat_list.append(st.sid)

    #print(arr_skpdi_list)
    #print(arr_stat_list)
    return collision_dtp_array, arr_skpdi_list, arr_stat_list


# Функиця для получения одинаковых записей по полю даты и типа нарушения
def same_date_type(arr_skpdi, arr_stat_gpdd):
    collision_dtp_array = []
    arr_skpdi_list = []
    arr_stat_list = []
    i = 0
    for sk in arr_skpdi:
        arr_skpdi_list.append(sk.id)
        sk_date = datetime.date(sk.lastchanged)
        for st in arr_stat_gpdd:
            arr_stat_list.append(st.sid)
            st_date = datetime.date(st.dtp_date)
            if sk_date == st_date:
                for sk_col_type in sk.skpdicollisiontypecollision_set.all():
                    if sk.lat and sk.lon and st.lat and st.lon:
                        # Если точки не в одном радиусе
                        if not check_coords(sk.lat, sk.lon, st.lat, st.lon):
                            collision_dtp_array.append({
                                'skpdi': {
                                    'id': sk.id,
                                    'type': sk_col_type.skpdi_collision_type.name
                                },
                                'stat': {
                                    'id': st.sid,
                                    'type': st.dtvp
                                }
                            })
            i += 1
    arr_skpdi_list = list(set(arr_skpdi_list))
    arr_stat_list = list(set(arr_stat_list))
    print(collision_dtp_array)
    return collision_dtp_array, arr_skpdi_list, arr_stat_list


# Функция проверики по нарушению
def check_type(sk_type, st_type):
    if AllDtpCollisionType.objects.filter(skpdi_type=sk_type, stat_type=st_type):
        return True
    else:
        return False


# Функция создания лога скрипта
def create_last_index(max_id_skpdi, max_id_stat):
    now = datetime.now()
    last_id = AllDtpLastIndex.objects.create(update_time_field=now, skpdi_id=max_id_skpdi, stat_gibdd_id=max_id_stat)
    last_id.save()


# Функция очиски массивов от лишних элементов
def delete_same_index(collision_dtp_array, arr_skpdi_list, arr_stat_list):
    i = 0
    for item_dtp in collision_dtp_array:
        if check_type(item_dtp['skpdi']['type'], item_dtp['stat']['type']):
            collision_dtp_array.pop(i)
        i += 1
    # Удаление лишних элементов из массива arr_skpdi_list и arr_stat_list
    for item_dtp in collision_dtp_array:
        if item_dtp['skpdi']['id'] in arr_skpdi_list:
            arr_skpdi_list.remove(item_dtp['skpdi']['id'])
        if item_dtp['stat']['id'] in arr_stat_list:
            arr_stat_list.remove(item_dtp['stat']['id'])
    # Исключение дубликатов
    arr_skpdi_list = set(arr_skpdi_list)
    arr_stat_list = set(arr_stat_list)
    return collision_dtp_array, arr_skpdi_list, arr_stat_list


# Проверка на одинаковость по координатам
def check_coords(mylat_skpdi, mylon_skpdi, mylat_stat, mylot_stat):
    if mylat_skpdi and mylon_skpdi and mylat_stat and mylot_stat:
        dist = 0.2  # дистанция 200 м
        mylon = float(mylon_skpdi)  # долгота центра
        mylat = float(mylat_skpdi)  # широта
        lon1 = mylon - dist / abs(math.cos(math.radians(mylat)) * 111.0)  # 1 градус широты = 111 км
        lon2 = mylon + dist / abs(math.cos(math.radians(mylat)) * 111.0)
        lat1 = mylat - (dist / 111.0)
        lat2 = mylat + (dist / 111.0)
        if lat1 <= float(mylat_stat) <= lat2 and lon1 <= float(mylot_stat) <= lon2:
            return True
        else:
            return False
    else:
        return False


# Открытие транзакции и вставка в таблицу all_dtp_card
@transaction.atomic
def insert_all_dtp(collision_dtp_array, arr_skpdi_list, arr_stat_list):
    with transaction.atomic():
        with connection.cursor() as cursor:
            for collision in collision_dtp_array:
                cursor.execute(
                    "insert into dtp_global_stat.all_dtp_card (skpdi_id, stat_gibdd_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                    [collision['skpdi']['id'], collision['stat']['id']])
                print(str(collision['skpdi']['id']) + "---" + str(collision['stat']['id']))
            for sk_index in arr_skpdi_list:
                cursor.execute(
                    "insert into dtp_global_stat.all_dtp_card (skpdi_id) VALUES (%s) ON CONFLICT DO NOTHING",
                    [sk_index])
                print(str(sk_index) + "--- null")
            for st_index in arr_stat_list:
                cursor.execute(
                    "insert into dtp_global_stat.all_dtp_card (stat_gibdd_id) VALUES (%s) ON CONFLICT DO NOTHING",
                    [st_index])
                print("null ---" + str(st_index))


# Функция создания записи очереди
def create_history():
    if AllDtpCardHistory.objects.count() > 0:
        last_history = AllDtpCardHistory.objects.latest('id')
        if last_history.finish == 0:
            exit()
    now = datetime.now()
    new_last_history = AllDtpCardHistory.objects.create(start_time=now, finish=0)
    new_last_history.save()


# Функция обновления записи очереди
def update_history():
    last_history = AllDtpCardHistory.objects.latest('id')
    now = datetime.now()
    last_history.finish_time = now
    last_history.finish = 1
    last_history.save()


# Функция для поиск IconType SKPDI
def find_icon_type(obj_sk):
    icon_type = 0
    dead_type = obj_sk.skpdiuch_set.all().values().first()
    if dead_type:
        dead_type = dead_type['collision_party_cond_id']
        collition_type = obj_sk.skpdicollisiontypecollision_set.all().first().skpdi_collision_type.name
        if dead_type == 216599770:
            icon_type = death_skpdi(collition_type)
        else:
            icon_type = no_death_skpdi(collition_type)
    return icon_type


# Функиця создержащая словарь для IconType для SKPDI - убит
def death_skpdi(collition_type):
    tmp = {
        'Столкновение': 1,
        'Опрокидывание': 1,
        'Наезд на препятствие': 1,
        'Наезд на велосипедиста': 5,
        'Падение пассажира': 1,
        'Съезд с дороги': 1,
        'Наезд на стоящее ТС': 1,
        'Наезд на пешехода на пешеходном переходе': 3,
        'Наезд на пешехода вне пешеходного перехода': 1,
        'Иные виды ДТП': 7,
        'Наезд на животное': 7,
        'Наезд на гужевой транспорт': 7,
    }
    return tmp.get(collition_type)


# Функиця создержащая словарь для IconType для SKPDI - ранен
def no_death_skpdi(collition_type):
    tmp = {
        'Столкновение': 2,
        'Опрокидывание': 2,
        'Наезд на препятствие': 2,
        'Наезд на велосипедиста': 6,
        'Падение пассажира': 2,
        'Съезд с дороги': 2,
        'Наезд на стоящее ТС': 2,
        'Наезд на пешехода на пешеходном переходе': 4,
        'Наезд на пешехода вне пешеходного перехода': 4,
        'Иные виды ДТП': 8,
        'Наезд на животное': 8,
        'Наезд на гужевой транспорт': 8,
    }
    return tmp.get(collition_type)


# Функиця создержащая словарь для IconType для STAT_GIPDD - убит
def death_stat(collition_type):
    tmp = {
        'Столкновение': 1,
        'Опрокидывание': 1,
        'Наезд на препятствие': 1,
        'Наезд на велосипедиста': 5,
        'Падение пассажира': 1,
        'Съезд с дороги': 1,
        'Наезд на стоящее ТС': 1,
        'Наезд на пешехода': 3,
        'Наезд на лицо, не являющееся участником движения, осуществляющее какую-либо друую деятельность': 3,
        'Иные виды ДТП': 7,
        'Наезд на лицо, не являющееся участником движения, осуществляющее производство работ': 7,
        'Падение груза': 7,
        'Отбрасывание предмета': 7,
        'Наезд на внезапно возникшее препятствие': 7,
    }
    return tmp[collition_type]


# Функиця создержащая словарь для IconType для STAT_GIPDD - ранен
def no_death_stat(collition_type):
    tmp = {
        'Столкновение': 2,
        'Опрокидывание': 2,
        'Наезд на препятствие': 2,
        'Наезд на велосипедиста': 6,
        'Падение пассажира': 2,
        'Съезд с дороги': 2,
        'Наезд на стоящее ТС': 2,
        'Наезд на пешехода': 4,
        'Наезд на лицо, не являющееся участником движения, осуществляющее какую-либо друую деятельность': 4,
        'Иные виды ДТП': 8,
        'Наезд на лицо, не являющееся участником движения, осуществляющее производство работ': 8,
        'Падение груза': 8,
        'Отбрасывание предмета': 8,
        'Наезд на внезапно возникшее препятствие': 8,
    }
    return tmp[collition_type]


# Создание файла json для SKPDI
def create_json_skpdi():
    list_skpdi = []
    arr_skpdi = SkpdiDtpCard.objects.filter(coordinates__isnull=False)
    for item_sk in arr_skpdi:
        icon_type = find_icon_type(item_sk)
        list_skpdi.append({
            'id': item_sk.id,
            'lat': item_sk.lat,
            'lon': item_sk.lon,
            'iconType': icon_type,
        })
    with open('media/json_skpdi.json', 'w') as outfile:
        json.dump(list(list_skpdi), outfile)


# Создание файла json для STAT_GIPDD
def create_json_stat():
    arr_stat = StatGibddDtpCard.objects.filter(lat__isnull=False, lon__isnull=False).values('sid', 'lat', 'lon')
    with open('media/json_stat.json', 'w') as outfile:
        json.dump(list(arr_stat), outfile)


# Создание файла json коллизий SKPDI и STAT_GIPDD
def create_json_col():
    list_not_col_skpdi = AllDtpCard.objects.filter(stat_gibdd_id__isnull=True).values_list('skpdi_id', flat=True)
    list_not_col_stat = AllDtpCard.objects.filter(skpdi_id__isnull=True).values_list('stat_gibdd_id', flat=True)
    list_col = AllDtpCard.objects.filter(skpdi_id__isnull=False, stat_gibdd_id__isnull=False).values('skpdi_id',
                                                                                                     'stat_gibdd_id')
    json_sk = []
    for sk in list_not_col_skpdi:
        tmp_sk = SkpdiDtpCard.objects.filter(id=sk).first()
        icon_type = find_icon_type(tmp_sk)
        json_sk.append({
            'id': tmp_sk.id,
            'lat': tmp_sk.lat,
            'lon': tmp_sk.lon,
            'iconType': icon_type,
        })

    json_st = []
    for st in list_not_col_stat:
        tmp_st = StatGibddDtpCard.objects.filter(sid=st).values('sid', 'lat', 'lon').first()
        json_st.append(tmp_st)

    json_col = []
    for col in list_col:
        tmp_sk = SkpdiDtpCard.objects.filter(id=col['skpdi_id']).values('id', 'lat', 'lon').first()
        tmp_st = StatGibddDtpCard.objects.filter(sid=col['stat_gibdd_id']).values('sid', 'lat', 'lon').first()
        json_col.append({
            'skpdi': tmp_sk,
            'stat': tmp_st
        })

    all_json = [{
        'list_skpdi': json_sk,
        'list_stat': json_st,
        'list_col': json_col
    }]
    with open('media/json_col.json', 'w') as outfile:
        json.dump(list(all_json), outfile)


# Главная функция скрипта
if __name__ == '__main__':
    # Фиксация времени начал скрипта
    start_time = time.time()
    # Установка начале очереди
    create_history()
    # Получение списков skpdi и stat_gipdd
    arr_skpdi, arr_stat_gpdd = create_skpdi_stat()
    # Проверка списков по дате и нарушению
    collision_dtp_array, arr_skpdi_list, arr_stat_list = same_date_type(arr_skpdi, arr_stat_gpdd)
    # Создания лога скрипта
    create_last_index(max(arr_skpdi_list), max(arr_stat_list))
    # Исключение дублей массивов
    collision_dtp_array, arr_skpdi_list, arr_stat_list = delete_same_index(collision_dtp_array, arr_skpdi_list, arr_stat_list)
    # Вставка в базу новых пар ДТП
    insert_all_dtp(collision_dtp_array, arr_skpdi_list, arr_stat_list)
    # Вывод в консоль количества секунд за которое выполнялся скрипт
    print("--- %s seconds ---" % (time.time() - start_time))
    # Установка окончания очереди
    update_history()
    # Запись в файл JSON Выборки SKPDI
    create_json_skpdi()
    # Запись в файл JSON Выборки STAT_GIPDD
    create_json_stat()
    # Запись в файл JSON Выборки коллизий SKPDI и STAT_GIPDD
    create_json_col()
