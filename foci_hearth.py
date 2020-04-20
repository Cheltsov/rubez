import math
import os
import time
from django.utils import timezone

import django

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'rudez.settings')
django.setup()

from codewrite.models import CollisionRange, HearthCollision, HearthDtp, AllDtpCard

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


# Вывод спика цепочек очагов
def turn_hearth():
    # Получение всех записей дпт которые сравнивали по дистанции
    col_range = CollisionRange.objects.all().order_by('cid_1')
    foci_list = []
    # Сериализация списка очагов
    for item_first in col_range:
        tmp_sec = []
        for item_second in col_range:
            if item_first.cid_1 == item_second.cid_1:
                tmp_sec.append({
                    'id': item_second.id_collision_2
                })
        # Переписвание вложенного списка без дубликатов
        col_by_dict = {i['id']: i for i in tmp_sec}
        new_items_list = list(col_by_dict.values())
        # Формируем список
        foci_list.append({
            'id': item_first.id_collision_1,
            'list': new_items_list,
            'year': item_first.dtp_year,
            'month': item_first.dtp_month,
            'quarter': item_first.dtp_quarter,
            'icon_type': item_first.icon_type
        })
    # Переписывание списка без дубликатов
    url_by_dict = {i['id']: i for i in foci_list}
    new_items = list(url_by_dict.values())
    new_foci = []
    for item_foci in new_items:
        # Если вложенных ДПТ меньше 2 (начало учитывается)
        if len(item_foci['list']) > 1:
            new_foci.append(item_foci)
    return new_foci


# Создание записи Очага и получение списка связи Hearth и Dtp
def get_list_heath(list_dtp):
    list_collision_hearth = []
    for item_foci in list_dtp:
        now = timezone.now()
        hearth = HearthDtp.objects.create(year=item_foci['year'], month=item_foci['month'],
                                          quarter=item_foci['quarter'], created=now,
                                          count_dtp=len(item_foci['list'])+1, type=item_foci['icon_type'])
        hearth.save()
        cr_obj = AllDtpCard.objects.get(id=item_foci['id'])
        list_collision_hearth.append(HearthCollision(hid=hearth, cid=cr_obj))
        for item_nested_dtp in item_foci['list']:
            cr_obj = AllDtpCard.objects.get(id=item_nested_dtp['id'])
            list_collision_hearth.append(HearthCollision(hid=hearth, cid=cr_obj))
    return list_collision_hearth


# Главная функция скрипта
if __name__ == '__main__':
    print('start')
    # Фиксация времени начал скрипта
    start_time = time.time()
    # Очистка таблицы HearthDtp
    HearthDtp.objects.all().delete()
    # Очистка таблицы HearthCollision
    HearthCollision.objects.all().delete()
    # Вывод Дпт Очагов
    new_foci = turn_hearth()
    # Получение списка связи Hearth и Dtp и создание Hearth
    list_collision_hearth = get_list_heath(new_foci)
    # Создание связи очага и ДПТ
    HearthCollision.objects.bulk_create(list_collision_hearth)
    print("--- %s seconds ---" % (time.time() - start_time))
    print('end')
