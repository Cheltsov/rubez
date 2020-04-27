import math
import os
import time
from datetime import datetime
from django.utils import timezone

import django
import json

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'rudez.settings')
django.setup()

from codewrite.models import SkpdiDtpCard, AllDtpCardHistory, AllDtpLastIndex, StatGibddDtpCard, AllDtpCard, \
    AllDtpCollisionType
from django.db import transaction, connection


# Функция создания списка SKPDI и STAT_GIPDD
def create_skpdi_stat():
    # Если есть записи в таблице all_dtp_last_index
    if AllDtpLastIndex.objects.count() > 0:
        # Получение последней записи в таблице
        last_index_history = AllDtpLastIndex.objects.latest('id')
        # Получение списка записей SKPDI
        arr_skpdi = SkpdiDtpCard.objects.filter(id__lte=last_index_history.skpdi_id, lat__isnull=False).order_by(
            'id')
        if not arr_skpdi:
            arr_skpdi = SkpdiDtpCard.objects.filter(lat__isnull=False).order_by('id')
        # Получение списка записей STAT_GIPDD
        arr_stat_gpdd = StatGibddDtpCard.objects.filter(sid__lte=last_index_history.stat_gibdd_id, lat__isnull=False).order_by('sid')
        if not arr_stat_gpdd:
            arr_stat_gpdd = StatGibddDtpCard.objects.filter(lat__isnull=False).order_by('sid')
    else:
        arr_skpdi = SkpdiDtpCard.objects.filter(lat__isnull=False).order_by('id')
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
    return collision_dtp_array, arr_skpdi_list, arr_stat_list


# Функиця для получения одинаковых записей по полю даты и типа нарушения
def same_date_type(arr_skpdi, arr_stat_gpdd):
    collision_dtp_array = []
    arr_skpdi_list = []
    arr_stat_list = []
    i = 0
    for sk in arr_skpdi:
        arr_skpdi_list.append(sk.id)
        sk_date = datetime.date(sk.collision_date)
        for st in arr_stat_gpdd:
            arr_stat_list.append(st.sid)
            st_date = datetime.date(st.dtp_date)
            if sk_date == st_date:
                for sk_col_type in sk.skpdicollisiontypecollision_set.all():
                    if sk.lat and sk.lon and st.lat and st.lon:
                        # Если точки в одном радиусе
                        if check_coords(sk.lat, sk.lon, st.lat, st.lon):
                            collision_dtp_array.append({
                                'skpdi': {
                                    'id': sk.id,
                                    'type': sk_col_type.skpdi_collision_type.name.strip()
                                },
                                'stat': {
                                    'id': st.sid,
                                    'type': st.dtvp.strip()
                                }
                            })
            i += 1
    arr_skpdi_list = list(set(arr_skpdi_list))
    arr_stat_list = list(set(arr_stat_list))
    return collision_dtp_array, arr_skpdi_list, arr_stat_list


# Функция проверики по нарушению
def check_type(sk_type, st_type):
    if AllDtpCollisionType.objects.filter(skpdi_type=sk_type, stat_type=st_type):
        return True
    else:
        return False


# Функция создания лога скрипта
def create_last_index(max_id_skpdi, max_id_stat):
    now = timezone.now()
    last_id = AllDtpLastIndex.objects.create(update_time=now, skpdi_id=max_id_skpdi, stat_gibdd_id=max_id_stat)
    last_id.save()


# Функция очиски массивов от лишних элементов
def delete_same_index(collision_dtp_array, arr_skpdi_list, arr_stat_list):
    i = 0
    for item_dtp in collision_dtp_array:
        if not check_type(item_dtp['skpdi']['type'], item_dtp['stat']['type']):
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
        dist = 250  # дистанция 200 м
        mylon = float(mylon_skpdi)  # долгота центра
        mylat = float(mylat_skpdi)  # широта
        lon1 = mylon - dist / abs(math.cos(math.radians(mylat)) * 111000)  # 1 градус широты = 111 км
        lon2 = mylon + dist / abs(math.cos(math.radians(mylat)) * 111000)
        lat1 = mylat - (dist / 111000)
        lat2 = mylat + (dist / 111000)
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
                # print(str(collision['skpdi']['id']) + "---" + str(collision['stat']['id']))
            for sk_index in arr_skpdi_list:
                cursor.execute(
                    "insert into dtp_global_stat.all_dtp_card (skpdi_id) VALUES (%s) ON CONFLICT DO NOTHING",
                    [sk_index])
                # print(str(sk_index) + "--- null")
            for st_index in arr_stat_list:
                cursor.execute(
                    "insert into dtp_global_stat.all_dtp_card (stat_gibdd_id) VALUES (%s) ON CONFLICT DO NOTHING",
                    [st_index])
                # print("null ---" + str(st_index))


# Функция создания записи очереди
def create_history():
    if AllDtpCardHistory.objects.count() > 0:
        last_history = AllDtpCardHistory.objects.latest('id')
        if last_history.finish == 0:
            exit()
    now = timezone.now()
    new_last_history = AllDtpCardHistory.objects.create(start_time=now, finish=0)
    new_last_history.save()


# Функция обновления записи очереди
def update_history():
    last_history = AllDtpCardHistory.objects.latest('id')
    now = timezone.now()
    last_history.finish_time = now
    last_history.finish = 1
    last_history.save()


# Функция для поиск IconType SKPDI
def find_icon_type_skpdi(obj_sk):
    icon_type = 0
    dead_type = obj_sk.skpdiuch_set.all().values().first()
    if dead_type and obj_sk.skpdicollisiontypecollision_set.count() > 0:
        dead_type = dead_type['collision_party_cond_id']
        collition_type = obj_sk.skpdicollisiontypecollision_set.first().skpdi_collision_type.name
        if dead_type == 216599770:
            # Если ранен
            icon_type = no_death_skpdi(collition_type)
        else:
            icon_type = death_skpdi(collition_type)
    return icon_type


# Функиця для поиск IconType STAT_GIPDD
def find_icon_type_stat(obj_st):
    if obj_st.pog:
        icon_type = death_stat(obj_st.dtvp.strip())
    else:
        icon_type = no_death_stat(obj_st.dtvp.strip())
    return icon_type


# Функиця создержащая словарь для IconType для SKPDI - убит
def death_skpdi(collition_type):
    tmp = {
        'Столкновение': 2,
        'Опрокидывание': 4,
        'Наезд на препятствие': 6,
        'Наезд на велосипедиста': 8,
        'Падение пассажира': 10,
        'Съезд с дороги': 12,
        'Наезд на стоящее ТС': 14,
        'Наезд на пешехода на пешеходном переходе': 16,
        'Наезд на пешехода вне пешеходного перехода': 16,
        'Иные виды ДТП': 18,
        'Наезд на животное': 18,
        'Наезд на гужевой транспорт': 18,
    }
    return tmp[collition_type]


# Функиця создержащая словарь для IconType для SKPDI - ранен
def no_death_skpdi(collition_type):
    tmp = {
        'Столкновение': 1,
        'Опрокидывание': 3,
        'Наезд на препятствие': 5,
        'Наезд на велосипедиста': 7,
        'Падение пассажира': 9,
        'Съезд с дороги': 11,
        'Наезд на стоящее ТС': 13,
        'Наезд на пешехода на пешеходном переходе': 15,
        'Наезд на пешехода вне пешеходного перехода': 15,
        'Иные виды ДТП': 17,
        'Наезд на животное': 17,
        'Наезд на гужевой транспорт': 17,
    }
    return tmp[collition_type]


# Функиця создержащая словарь для IconType для STAT_GIPDD - убит
def death_stat(collition_type):
    tmp = {
        'Столкновение': 2,
        'Опрокидывание': 4,
        'Наезд на препятствие': 6,
        'Наезд на велосипедиста': 8,
        'Падение пассажира': 10,
        'Съезд с дороги': 12,
        'Наезд на стоящее ТС': 14,
        'Наезд на пешехода': 16,
        'Наезд на лицо, не являющееся участником дорожного движения, осуществляющее какую-либо другую деятельность': 16,
        'Иной вид ДТП': 18,
        'Наезд на лицо, не являющееся участником дорожного движения, осуществляющее производство работ': 18,
        'Падение груза': 18,
        'Отбрасывание предмета': 18,
        'Наезд на внезапно возникшее препятствие': 18,
    }
    return tmp[collition_type]


# Функиця создержащая словарь для IconType для STAT_GIPDD - ранен
def no_death_stat(collition_type):
    tmp = {
        'Столкновение': 1,
        'Опрокидывание': 3,
        'Наезд на препятствие': 5,
        'Наезд на велосипедиста': 7,
        'Падение пассажира': 9,
        'Съезд с дороги': 11,
        'Наезд на стоящее ТС': 13,
        'Наезд на пешехода': 15,
        'Наезд на лицо, не являющееся участником дорожного движения, осуществляющее какую-либо другую деятельность': 15,
        'Иной вид ДТП': 17,
        'Наезд на лицо, не являющееся участником дорожного движения, осуществляющее производство работ': 17,
        'Падение груза': 17,
        'Отбрасывание предмета': 17,
        'Наезд на внезапно возникшее препятствие': 17,
    }
    return tmp[collition_type]


# Создание файла json для SKPDI
def create_json_skpdi():
    list_skpdi = []
    arr_skpdi = SkpdiDtpCard.objects.filter(coordinates__isnull=False, lat__isnull=False, lon__isnull=False)
    for item_sk in arr_skpdi:
        icon_type = find_icon_type_skpdi(item_sk)
        if item_sk.skpdicollisiontypecollision_set.count() > 0:
            list_skpdi.append({
                'id': item_sk.id,
                'lat': item_sk.lat,
                'lon': item_sk.lon,
                'iconType': icon_type,
                'collision_type': item_sk.skpdicollisiontypecollision_set.first().skpdi_collision_type.name,
                'date': time.mktime(item_sk.collision_date.timetuple())
            })
    with open('media/json_skpdi.json', 'w') as outfile:
        json.dump(list(list_skpdi), outfile)


# Создание файла json для STAT_GIPDD
def create_json_stat():
    list_stat = []
    arr_stat = StatGibddDtpCard.objects.filter(lat__isnull=False, lon__isnull=False)
    for item_st in arr_stat:
        icon_type = find_icon_type_stat(item_st)
        list_stat.append({
            'id': item_st.sid,
            'lat': item_st.lat,
            'lon': item_st.lon,
            'iconType': icon_type,
            'collision_type': item_st.dtvp,
            'date': time.mktime(item_st.dtp_date.timetuple())
        })
    with open('media/json_stat.json', 'w') as outfile:
        json.dump(list(list_stat), outfile)



# Функция создания JSON для третьего слоя карты
def create_json_col_for_map():
    list_collision = AllDtpCard.objects.all().values()
    tmp_item = []
    for col in list_collision:
        if not col['stat_gibdd_id']:
            obj_skpdi = SkpdiDtpCard.objects.filter(id=col['skpdi_id']).first()
            iconType = find_icon_type_skpdi(obj_skpdi)
            if obj_skpdi.skpdicollisiontypecollision_set.count() > 0:
                tmp_item.append({
                    'id_ver': col['id'],
                    'id_stat': '',
                    'id_skpdi': col['skpdi_id'],
                    'iconType': iconType,
                    'lat': obj_skpdi.lat,
                    'lon': obj_skpdi.lon,
                    'collision_type': obj_skpdi.skpdicollisiontypecollision_set.first().skpdi_collision_type.name,
                    'date': time.mktime(obj_skpdi.collision_date.timetuple())
                })
        else:
            obj_stat = StatGibddDtpCard.objects.filter(sid=col['stat_gibdd_id']).first()
            iconType = find_icon_type_stat(obj_stat)
            tmp_item.append({
                'id_ver': col['id'],
                'id_stat': col['stat_gibdd_id'],
                'id_skpdi': col['skpdi_id'],
                'iconType': iconType,
                'lat': obj_stat.lat,
                'lon': obj_stat.lon,
                'collision_type': obj_stat.dtvp,
                'date': time.mktime(obj_stat.dtp_date.timetuple())
            })

    with open('media/json_col.json', 'w') as outfile:
        json.dump(list(tmp_item), outfile)


# Главная функция скрипта
if __name__ == '__main__':
    print('start')
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
    collision_dtp_array, arr_skpdi_list, arr_stat_list = delete_same_index(collision_dtp_array, arr_skpdi_list,
                                                                           arr_stat_list)
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
    create_json_col_for_map()
    print("--- %s seconds ---" % (time.time() - start_time))
