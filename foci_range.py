import math
import os
import time
from datetime import datetime

import django
import json

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'rudez.settings')
django.setup()

from codewrite.models import SkpdiDtpCard, StatGibddDtpCard, CollisionRange

# Задание стартового года
year_start = '2019'
# Задание расстояния между точками
dtp_range = 500
# Задание кварталов
quarter = [
    [1, 3],
    [4, 6],
    [7, 9],
    [10, 12]
]


# Функция получения списка доступных годов
def get_list_years():
    list_year_tmp = []
    year_range = int(datetime.now().year) - int(year_start) + 1
    for i in range(year_range):
        list_year_tmp.append(int(year_start) + i)
    return list_year_tmp


# Функция получения списка ДТП из json файла коллизий
def get_data(year, qua1, qua2):
    with open('media/json_col.json') as json_file:
        data = json.load(json_file)
        new_dtp_list = []
        for item in data:
            year_dtp = datetime.fromtimestamp(item['date']).year
            month_dtp = datetime.fromtimestamp(item['date']).month
            if year_dtp >= year and qua1 <= month_dtp <= qua2:
                if item['id_skpdi']:
                    new_dtp_list.append({
                        'id': item['id_skpdi'],
                        'lat': item['lat'],
                        'lon': item['lon'],
                        'iconType': item['iconType'],
                        'skpdi': 1,
                        'id_collision': item['id_ver'],
                        'month': month_dtp,
                        'year': year_dtp
                    })
                if item['id_stat']:
                    new_dtp_list.append({
                        'id': item['id_stat'],
                        'lat': item['lat'],
                        'lon': item['lon'],
                        'iconType': item['iconType'],
                        'skpdi': 0,
                        'id_collision': item['id_ver'],
                        'month': month_dtp,
                        'year': year_dtp
                    })
        return new_dtp_list


# Функция расчета расстояния между точками
def calculateTheDistance(lat_p1, lon_p1, lat_p2, lon_p2):
    earth_radius = 6372795
    # Перевод координат в радианы
    lat1 = lat_p1 * math.pi / 180
    lat2 = lat_p2 * math.pi / 180
    long1 = lon_p1 * math.pi / 180
    long2 = lon_p2 * math.pi / 180
    # косинусы и синусы широт и разницы долгот
    cl1 = math.cos(lat1)
    cl2 = math.cos(lat2)
    sl1 = math.sin(lat1)
    sl2 = math.sin(lat2)
    delta = long2 - long1
    cdelta = math.cos(delta)
    sdelta = math.sin(delta)
    # вычисления длины большого круга
    y = math.sqrt(math.pow(cl2 * sdelta, 2) + math.pow(cl1 * sl2 - sl1 * cl2 * cdelta, 2))
    x = sl1 * sl2 + cl1 * cl2 * cdelta
    ad = math.atan2(y, x)
    dist = ad * earth_radius
    return dist


# Функция формирования записей для добавления в таблицу
def generation_collision_range(list_dtp, cur_year, cur_quarte):
    arr_item_range = []
    # Цикл нахождения расстояния между точками - каждая с каждой
    for item_dtp_1 in list_dtp:
        for item_dtp_2 in list_dtp:
            # Поиск расстояния
            dist = calculateTheDistance(item_dtp_1['lat'], item_dtp_1['lon'], item_dtp_2['lat'],
                                        item_dtp_2['lon'])
            # Дополнительне проверки
            if dist <= dtp_range and item_dtp_1['id'] != item_dtp_2['id'] and \
                    item_dtp_1['iconType'] == item_dtp_2['iconType']:
                # Создание объекта для записи в таблицу CollisionRange
                obj_range = CollisionRange(cid_1=item_dtp_1['id'],
                                           cid_2=item_dtp_2['id'],
                                           range=int(dist),
                                           cid_skpdi_1=item_dtp_1['skpdi'],
                                           cid_skpdi_2=item_dtp_2['skpdi'],
                                           dtp_year=cur_year,
                                           dtp_quarter=int(quarter.index(cur_quarte) + 1),
                                           dtp_month=item_dtp_1['month'],
                                           id_collision_1=item_dtp_1['id_collision'],
                                           id_collision_2=item_dtp_2['id_collision'])
                arr_item_range.append(obj_range)
    return arr_item_range


def create_collision_range():
    # Получение списков ДТП
    list_year = get_list_years()
    # Перебор по годам
    for item_year in list_year:
        # Перебор по кварталам
        for item_quarter in quarter:
            qu1 = item_quarter[0]
            qu2 = item_quarter[1]
            # Получение списка ДПТ по квартально
            # list_dtp = get_dtp_quarter(item_year, qu1, qu2)
            list_dtp = get_data(item_year, qu1, qu2)
            # Функция генерирования записей collision_range
            arr_item_range = generation_collision_range(list_dtp, item_year, item_quarter)
            # Добавление в БД таблица Collision_range
            CollisionRange.objects.bulk_create(arr_item_range)
            # Вывод сообщения о добавленных элементах
            print("Год: " + str(item_year) + ". Квартал: " + str(qu1) + " - " + str(qu2) + " Добавил: " + str(
                len(arr_item_range)))
    return True


# Главная функция скрипта
if __name__ == '__main__':
    print('start')
    # Фиксация времени начал скрипта
    start_time = time.time()
    # Очистка таблицы
    CollisionRange.objects.all().delete()
    # Создание таблицы collision_range
    create_collision_range()
    print("--- %s seconds ---" % (time.time() - start_time))
    print('end')
