import math
import os
import time
from datetime import datetime

import django

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'rudez.settings')
django.setup()

from codewrite.models import SkpdiDtpCard, AllDtpCardHistory, AllDtpLastIndex, StatGibddDtpCard, AllDtpCard
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
            arr_skpdi = SkpdiDtpCard.objects.all().order_by('id')
        # Получение списка записей STAT_GIPDD
        arr_stat_gpdd = StatGibddDtpCard.objects.filter(sid__lte=last_index_history.stat_gibdd_id).order_by('sid')
        if not arr_stat_gpdd:
            arr_stat_gpdd = StatGibddDtpCard.objects.all().order_by('sid')
    else:
        arr_skpdi = SkpdiDtpCard.objects.all().order_by('id')
        arr_stat_gpdd = StatGibddDtpCard.objects.all().order_by('sid')
    return arr_skpdi, arr_stat_gpdd


# Функция для явного сказания массива collision_dtp_array, arr_skpdi, arr_stat_gpdd
def my_same(arr_skpdi, arr_stat_gpdd):
    arr_skpdi_list = []
    arr_stat_list = []
    collision_dtp_array = [{'skpdi': {'id': 229404014, 'lat': '54.913635', 'lon': '37.412744'}, 'stat': {'id': 221008628, 'lat': 55.775101, 'lon': 38.429071}}, {'skpdi': {'id': 229404014, 'lat': '54.913635', 'lon': '37.412744'}, 'stat': {'id': 221008663, 'lat': 56.3125, 'lon': 37.4936}}, {'skpdi': {'id': 229404014, 'lat': '54.913635', 'lon': '37.412744'}, 'stat': {'id': 221008706, 'lat': 55.569089, 'lon': 38.192382}}, {'skpdi': {'id': 229404014, 'lat': '54.913635', 'lon': '37.412744'}, 'stat': {'id': 221008804, 'lat': 55.131164, 'lon': 37.418211}}, {'skpdi': {'id': 229404014, 'lat': '54.913635', 'lon': '37.412744'}, 'stat': {'id': 221009153, 'lat': 55.567141, 'lon': 36.518222}}, {'skpdi': {'id': 229404014, 'lat': '54.913635', 'lon': '37.412744'}, 'stat': {'id': 221009804, 'lat': 55.9928, 'lon': 37.5356}}, {'skpdi': {'id': 229404014, 'lat': '54.913635', 'lon': '37.412744'}, 'stat': {'id': 221545043, 'lat': 55.839187, 'lon': 37.252321}}, {'skpdi': {'id': 305102858, 'lat': '54.82446', 'lon': '38.143405'}, 'stat': {'id': 221010123, 'lat': 55.867811, 'lon': 38.207531}}, {'skpdi': {'id': 305102858, 'lat': '54.82446', 'lon': '38.143405'}, 'stat': {'id': 221010178, 'lat': 55.855853, 'lon': 38.543901}}, {'skpdi': {'id': 305102858, 'lat': '54.82446', 'lon': '38.143405'}, 'stat': {'id': 221010221, 'lat': 55.663505, 'lon': 39.860426}}, {'skpdi': {'id': 305102858, 'lat': '54.82446', 'lon': '38.143405'}, 'stat': {'id': 221010315, 'lat': 55.994439, 'lon': 37.337894}}, {'skpdi': {'id': 305206978, 'lat': '55.57177', 'lon': '38.174679'}, 'stat': {'id': 221010123, 'lat': 55.867811, 'lon': 38.207531}}, {'skpdi': {'id': 305206978, 'lat': '55.57177', 'lon': '38.174679'}, 'stat': {'id': 221010178, 'lat': 55.855853, 'lon': 38.543901}}, {'skpdi': {'id': 305206978, 'lat': '55.57177', 'lon': '38.174679'}, 'stat': {'id': 221010221, 'lat': 55.663505, 'lon': 39.860426}}, {'skpdi': {'id': 305206978, 'lat': '55.57177', 'lon': '38.174679'}, 'stat': {'id': 221010315, 'lat': 55.994439, 'lon': 37.337894}}, {'skpdi': {'id': 305633498, 'lat': '55.971163', 'lon': '37.928334'}, 'stat': {'id': 221010123, 'lat': 55.867811, 'lon': 38.207531}}, {'skpdi': {'id': 305633498, 'lat': '55.971163', 'lon': '37.928334'}, 'stat': {'id': 221010178, 'lat': 55.855853, 'lon': 38.543901}}, {'skpdi': {'id': 305633498, 'lat': '55.971163', 'lon': '37.928334'}, 'stat': {'id': 221010221, 'lat': 55.663505, 'lon': 39.860426}}, {'skpdi': {'id': 305633498, 'lat': '55.971163', 'lon': '37.928334'}, 'stat': {'id': 221010315, 'lat': 55.994439, 'lon': 37.337894}}, {'skpdi': {'id': 305645338, 'lat': '56.242565', 'lon': '38.034003'}, 'stat': {'id': 221010123, 'lat': 55.867811, 'lon': 38.207531}}, {'skpdi': {'id': 305645338, 'lat': '56.242565', 'lon': '38.034003'}, 'stat': {'id': 221010178, 'lat': 55.855853, 'lon': 38.543901}}, {'skpdi': {'id': 305645338, 'lat': '56.242565', 'lon': '38.034003'}, 'stat': {'id': 221010221, 'lat': 55.663505, 'lon': 39.860426}}, {'skpdi': {'id': 305645338, 'lat': '56.242565', 'lon': '38.034003'}, 'stat': {'id': 221010315, 'lat': 55.994439, 'lon': 37.337894}}, {'skpdi': {'id': 306215494, 'lat': '55.209022', 'lon': '38.774437'}, 'stat': {'id': 221127730, 'lat': 56.238294, 'lon': 35.679967}}, {'skpdi': {'id': 306217473, 'lat': '55.007235', 'lon': '39.294475'}, 'stat': {'id': 221333179, 'lat': 56.439675, 'lon': 37.907081}}, {'skpdi': {'id': 306217589, 'lat': '55.893317', 'lon': '36.307948'}, 'stat': {'id': 221437169, 'lat': 55.513617, 'lon': 36.990849}}, {'skpdi': {'id': 306217589, 'lat': '55.893317', 'lon': '36.307948'}, 'stat': {'id': 221437618, 'lat': 55.553332, 'lon': 37.724926}}, {'skpdi': {'id': 306217589, 'lat': '55.893317', 'lon': '36.307948'}, 'stat': {'id': 221437728, 'lat': 55.535528, 'lon': 37.067775}}, {'skpdi': {'id': 306217589, 'lat': '55.893317', 'lon': '36.307948'}, 'stat': {'id': 221473589, 'lat': 55.829577, 'lon': 36.869752}}, {'skpdi': {'id': 306217589, 'lat': '55.893317', 'lon': '36.307948'}, 'stat': {'id': 221474224, 'lat': 55.44367, 'lon': 37.753884}}, {'skpdi': {'id': 306483338, 'lat': '55.87306', 'lon': '38.206018'}, 'stat': {'id': 221010418, 'lat': 55.779687, 'lon': 37.911222}}, {'skpdi': {'id': 306483338, 'lat': '55.87306', 'lon': '38.206018'}, 'stat': {'id': 221010468, 'lat': 55.3328, 'lon': 37.528428}}, {'skpdi': {'id': 306483338, 'lat': '55.87306', 'lon': '38.206018'}, 'stat': {'id': 221024575, 'lat': 55.6489, 'lon': 38.131571}}, {'skpdi': {'id': 306483338, 'lat': '55.87306', 'lon': '38.206018'}, 'stat': {'id': 221603987, 'lat': 55.317815, 'lon': 37.556591}}, {'skpdi': {'id': 307640218, 'lat': '55.909747', 'lon': '36.811731'}, 'stat': {'id': 221025360, 'lat': 55.827859, 'lon': 37.309334}}, {'skpdi': {'id': 307640218, 'lat': '55.909747', 'lon': '36.811731'}, 'stat': {'id': 221029597, 'lat': 55.914167, 'lon': 36.819448}}, {'skpdi': {'id': 307640218, 'lat': '55.909747', 'lon': '36.811731'}, 'stat': {'id': 221067958, 'lat': 56.328911, 'lon': 38.152127}}, {'skpdi': {'id': 307640218, 'lat': '55.909747', 'lon': '36.811731'}, 'stat': {'id': 221112149, 'lat': 54.77247, 'lon': 38.95292}}, {'skpdi': {'id': 308869958, 'lat': '55.82041', 'lon': '36.416715'}, 'stat': {'id': 221068235, 'lat': 54.995638, 'lon': 39.212844}}, {'skpdi': {'id': 308869958, 'lat': '55.82041', 'lon': '36.416715'}, 'stat': {'id': 221082294, 'lat': 55.604251, 'lon': 37.006888}}, {'skpdi': {'id': 308869958, 'lat': '55.82041', 'lon': '36.416715'}, 'stat': {'id': 221090202, 'lat': 55.411428, 'lon': 37.699585}}, {'skpdi': {'id': 309958798, 'lat': '55.829285', 'lon': '37.307155'}, 'stat': {'id': 221068876, 'lat': 55.679514, 'lon': 37.285913}}, {'skpdi': {'id': 309958798, 'lat': '55.829285', 'lon': '37.307155'}, 'stat': {'id': 221068880, 'lat': 55.430645, 'lon': 37.941456}}, {'skpdi': {'id': 309958798, 'lat': '55.829285', 'lon': '37.307155'}, 'stat': {'id': 221068924, 'lat': 55.820067, 'lon': 38.17219}}, {'skpdi': {'id': 309958798, 'lat': '55.829285', 'lon': '37.307155'}, 'stat': {'id': 221068937, 'lat': 55.777297, 'lon': 38.694491}}, {'skpdi': {'id': 309958798, 'lat': '55.829285', 'lon': '37.307155'}, 'stat': {'id': 221069605, 'lat': 56.138323, 'lon': 37.489514}}, {'skpdi': {'id': 309958798, 'lat': '55.829285', 'lon': '37.307155'}, 'stat': {'id': 221069688, 'lat': 56.372, 'lon': 37.343742}}, {'skpdi': {'id': 309958798, 'lat': '55.829285', 'lon': '37.307155'}, 'stat': {'id': 221070811, 'lat': 55.704042, 'lon': 37.385831}}, {'skpdi': {'id': 309958798, 'lat': '55.829285', 'lon': '37.307155'}, 'stat': {'id': 221070900, 'lat': 55.227138, 'lon': 37.503247}}, {'skpdi': {'id': 309958798, 'lat': '55.829285', 'lon': '37.307155'}, 'stat': {'id': 221070902, 'lat': 55.649239, 'lon': 36.309943}}, {'skpdi': {'id': 309958798, 'lat': '55.829285', 'lon': '37.307155'}, 'stat': {'id': 221070904, 'lat': 55.79993, 'lon': 37.282856}}, {'skpdi': {'id': 309958798, 'lat': '55.829285', 'lon': '37.307155'}, 'stat': {'id': 221071082, 'lat': 55.998801, 'lon': 37.423178}}, {'skpdi': {'id': 309958798, 'lat': '55.829285', 'lon': '37.307155'}, 'stat': {'id': 221071090, 'lat': 54.812656, 'lon': 38.61895}}, {'skpdi': {'id': 309958798, 'lat': '55.829285', 'lon': '37.307155'}, 'stat': {'id': 221071279, 'lat': 55.85218, 'lon': 37.971175}}, {'skpdi': {'id': 309958798, 'lat': '55.829285', 'lon': '37.307155'}, 'stat': {'id': 221071299, 'lat': 55.368478, 'lon': 38.394213}}, {'skpdi': {'id': 309958798, 'lat': '55.829285', 'lon': '37.307155'}, 'stat': {'id': 221110836, 'lat': 55.392275, 'lon': 39.044412}}, {'skpdi': {'id': 309958798, 'lat': '55.829285', 'lon': '37.307155'}, 'stat': {'id': 221265878, 'lat': 55.735302, 'lon': 38.790429}}, {'skpdi': {'id': 309958798, 'lat': '55.829285', 'lon': '37.307155'}, 'stat': {'id': 221265899, 'lat': 56.375191, 'lon': 38.109823}}, {'skpdi': {'id': 309958798, 'lat': '55.829285', 'lon': '37.307155'}, 'stat': {'id': 221339004, 'lat': 55.614789, 'lon': 36.101418}}, {'skpdi': {'id': 309958798, 'lat': '55.829285', 'lon': '37.307155'}, 'stat': {'id': 221434068, 'lat': 55.985371, 'lon': 37.06285}}, {'skpdi': {'id': 310000678, 'lat': '54.767812', 'lon': '39.065047'}, 'stat': {'id': 221068878, 'lat': 55.388886, 'lon': 37.849274}}, {'skpdi': {'id': 310000678, 'lat': '54.767812', 'lon': '39.065047'}, 'stat': {'id': 221068926, 'lat': 55.316442, 'lon': 39.32451}}, {'skpdi': {'id': 310000678, 'lat': '54.767812', 'lon': '39.065047'}, 'stat': {'id': 221069601, 'lat': 56.160752, 'lon': 36.90239}}, {'skpdi': {'id': 310000678, 'lat': '54.767812', 'lon': '39.065047'}, 'stat': {'id': 221069692, 'lat': 55.647846, 'lon': 36.822374}}, {'skpdi': {'id': 310022038, 'lat': '55.79677', 'lon': '39.019639'}, 'stat': {'id': 221068876, 'lat': 55.679514, 'lon': 37.285913}}, {'skpdi': {'id': 310022038, 'lat': '55.79677', 'lon': '39.019639'}, 'stat': {'id': 221068880, 'lat': 55.430645, 'lon': 37.941456}}, {'skpdi': {'id': 310022038, 'lat': '55.79677', 'lon': '39.019639'}, 'stat': {'id': 221068924, 'lat': 55.820067, 'lon': 38.17219}}, {'skpdi': {'id': 310022038, 'lat': '55.79677', 'lon': '39.019639'}, 'stat': {'id': 221068937, 'lat': 55.777297, 'lon': 38.694491}}, {'skpdi': {'id': 310022038, 'lat': '55.79677', 'lon': '39.019639'}, 'stat': {'id': 221069605, 'lat': 56.138323, 'lon': 37.489514}}, {'skpdi': {'id': 310022038, 'lat': '55.79677', 'lon': '39.019639'}, 'stat': {'id': 221069688, 'lat': 56.372, 'lon': 37.343742}}, {'skpdi': {'id': 310022038, 'lat': '55.79677', 'lon': '39.019639'}, 'stat': {'id': 221070811, 'lat': 55.704042, 'lon': 37.385831}}, {'skpdi': {'id': 310022038, 'lat': '55.79677', 'lon': '39.019639'}, 'stat': {'id': 221070900, 'lat': 55.227138, 'lon': 37.503247}}, {'skpdi': {'id': 310022038, 'lat': '55.79677', 'lon': '39.019639'}, 'stat': {'id': 221070902, 'lat': 55.649239, 'lon': 36.309943}}, {'skpdi': {'id': 310022038, 'lat': '55.79677', 'lon': '39.019639'}, 'stat': {'id': 221070904, 'lat': 55.79993, 'lon': 37.282856}}, {'skpdi': {'id': 310022038, 'lat': '55.79677', 'lon': '39.019639'}, 'stat': {'id': 221071082, 'lat': 55.998801, 'lon': 37.423178}}, {'skpdi': {'id': 310022038, 'lat': '55.79677', 'lon': '39.019639'}, 'stat': {'id': 221071090, 'lat': 54.812656, 'lon': 38.61895}}, {'skpdi': {'id': 310022038, 'lat': '55.79677', 'lon': '39.019639'}, 'stat': {'id': 221071279, 'lat': 55.85218, 'lon': 37.971175}}, {'skpdi': {'id': 310022038, 'lat': '55.79677', 'lon': '39.019639'}, 'stat': {'id': 221071299, 'lat': 55.368478, 'lon': 38.394213}}, {'skpdi': {'id': 310022038, 'lat': '55.79677', 'lon': '39.019639'}, 'stat': {'id': 221110836, 'lat': 55.392275, 'lon': 39.044412}}, {'skpdi': {'id': 310022038, 'lat': '55.79677', 'lon': '39.019639'}, 'stat': {'id': 221265878, 'lat': 55.735302, 'lon': 38.790429}}, {'skpdi': {'id': 310022038, 'lat': '55.79677', 'lon': '39.019639'}, 'stat': {'id': 221265899, 'lat': 56.375191, 'lon': 38.109823}}, {'skpdi': {'id': 310022038, 'lat': '55.79677', 'lon': '39.019639'}, 'stat': {'id': 221339004, 'lat': 55.614789, 'lon': 36.101418}}, {'skpdi': {'id': 310022038, 'lat': '55.79677', 'lon': '39.019639'}, 'stat': {'id': 221434068, 'lat': 55.985371, 'lon': 37.06285}}, {'skpdi': {'id': 310105578, 'lat': '55.863385', 'lon': '37.1536'}, 'stat': {'id': 221082296, 'lat': 55.904936, 'lon': 37.6683}}, {'skpdi': {'id': 310105578, 'lat': '55.863385', 'lon': '37.1536'}, 'stat': {'id': 221085551, 'lat': 55.368527, 'lon': 39.010413}}, {'skpdi': {'id': 310105578, 'lat': '55.863385', 'lon': '37.1536'}, 'stat': {'id': 221090292, 'lat': 55.862573, 'lon': 37.157307}}, {'skpdi': {'id': 310293878, 'lat': '55.381887', 'lon': '36.554672'}, 'stat': {'id': 221069599, 'lat': 55.381896, 'lon': 36.555043}}, {'skpdi': {'id': 310293878, 'lat': '55.381887', 'lon': '36.554672'}, 'stat': {'id': 221090237, 'lat': 55.285617, 'lon': 37.775116}}, {'skpdi': {'id': 310293878, 'lat': '55.381887', 'lon': '36.554672'}, 'stat': {'id': 221090341, 'lat': 54.503243, 'lon': 38.623896}}, {'skpdi': {'id': 310293878, 'lat': '55.381887', 'lon': '36.554672'}, 'stat': {'id': 221434065, 'lat': 55.248867, 'lon': 37.938666}}, {'skpdi': {'id': 310334738, 'lat': '56.330362', 'lon': '36.57841'}, 'stat': {'id': 221068928, 'lat': 55.360039, 'lon': 38.115005}}, {'skpdi': {'id': 310334738, 'lat': '56.330362', 'lon': '36.57841'}, 'stat': {'id': 221068950, 'lat': 55.870688, 'lon': 35.326602}}, {'skpdi': {'id': 310334738, 'lat': '56.330362', 'lon': '36.57841'}, 'stat': {'id': 221070809, 'lat': 56.331314, 'lon': 36.570783}}, {'skpdi': {'id': 310492118, 'lat': '55.658287', 'lon': '36.811749'}, 'stat': {'id': 221068878, 'lat': 55.388886, 'lon': 37.849274}}, {'skpdi': {'id': 310492118, 'lat': '55.658287', 'lon': '36.811749'}, 'stat': {'id': 221068926, 'lat': 55.316442, 'lon': 39.32451}}, {'skpdi': {'id': 310492118, 'lat': '55.658287', 'lon': '36.811749'}, 'stat': {'id': 221069601, 'lat': 56.160752, 'lon': 36.90239}}, {'skpdi': {'id': 310492118, 'lat': '55.658287', 'lon': '36.811749'}, 'stat': {'id': 221069692, 'lat': 55.647846, 'lon': 36.822374}}, {'skpdi': {'id': 311090258, 'lat': '54.76382', 'lon': '38.27615'}, 'stat': {'id': 221083454, 'lat': 55.818982, 'lon': 38.161397}}, {'skpdi': {'id': 312085758, 'lat': '55.421542', 'lon': '37.546723'}, 'stat': {'id': 221091319, 'lat': 54.893146, 'lon': 39.149888}}, {'skpdi': {'id': 312085758, 'lat': '55.421542', 'lon': '37.546723'}, 'stat': {'id': 221111565, 'lat': 55.368954, 'lon': 39.066653}}, {'skpdi': {'id': 312085758, 'lat': '55.421542', 'lon': '37.546723'}, 'stat': {'id': 221111567, 'lat': 55.574973, 'lon': 38.238473}}, {'skpdi': {'id': 312085758, 'lat': '55.421542', 'lon': '37.546723'}, 'stat': {'id': 221434685, 'lat': 55.394992, 'lon': 37.789707}}, {'skpdi': {'id': 312408158, 'lat': '55.50358', 'lon': '36.046392'}, 'stat': {'id': 221266049, 'lat': 56.137862, 'lon': 37.467198}}, {'skpdi': {'id': 312408158, 'lat': '55.50358', 'lon': '36.046392'}, 'stat': {'id': 221266053, 'lat': 55.6237, 'lon': 38.0294}}, {'skpdi': {'id': 312408158, 'lat': '55.50358', 'lon': '36.046392'}, 'stat': {'id': 221286466, 'lat': 56.006666, 'lon': 38.373646}}, {'skpdi': {'id': 312408158, 'lat': '55.50358', 'lon': '36.046392'}, 'stat': {'id': 221343512, 'lat': 55.853832, 'lon': 37.178184}}, {'skpdi': {'id': 312408158, 'lat': '55.50358', 'lon': '36.046392'}, 'stat': {'id': 221371715, 'lat': 55.486787, 'lon': 37.601183}}, {'skpdi': {'id': 312411258, 'lat': '56.238612', 'lon': '35.679732'}, 'stat': {'id': 221127730, 'lat': 56.238294, 'lon': 35.679967}}, {'skpdi': {'id': 312430758, 'lat': '55.36879', 'lon': '39.06674'}, 'stat': {'id': 221091319, 'lat': 54.893146, 'lon': 39.149888}}, {'skpdi': {'id': 312430758, 'lat': '55.36879', 'lon': '39.06674'}, 'stat': {'id': 221111565, 'lat': 55.368954, 'lon': 39.066653}}, {'skpdi': {'id': 312430758, 'lat': '55.36879', 'lon': '39.06674'}, 'stat': {'id': 221111567, 'lat': 55.574973, 'lon': 38.238473}}, {'skpdi': {'id': 312430758, 'lat': '55.36879', 'lon': '39.06674'}, 'stat': {'id': 221434685, 'lat': 55.394992, 'lon': 37.789707}}, {'skpdi': {'id': 313145898, 'lat': '55.612605', 'lon': '38.066921'}, 'stat': {'id': 221266049, 'lat': 56.137862, 'lon': 37.467198}}, {'skpdi': {'id': 313145898, 'lat': '55.612605', 'lon': '38.066921'}, 'stat': {'id': 221266053, 'lat': 55.6237, 'lon': 38.0294}}, {'skpdi': {'id': 313145898, 'lat': '55.612605', 'lon': '38.066921'}, 'stat': {'id': 221286466, 'lat': 56.006666, 'lon': 38.373646}}, {'skpdi': {'id': 313145898, 'lat': '55.612605', 'lon': '38.066921'}, 'stat': {'id': 221343512, 'lat': 55.853832, 'lon': 37.178184}}, {'skpdi': {'id': 313145898, 'lat': '55.612605', 'lon': '38.066921'}, 'stat': {'id': 221371715, 'lat': 55.486787, 'lon': 37.601183}}, {'skpdi': {'id': 313638918, 'lat': '56.313358', 'lon': '37.905609'}, 'stat': {'id': 221266308, 'lat': 56.315287, 'lon': 37.859595}}, {'skpdi': {'id': 313638918, 'lat': '56.313358', 'lon': '37.905609'}, 'stat': {'id': 221266677, 'lat': 55.722204, 'lon': 38.418074}}, {'skpdi': {'id': 313638918, 'lat': '56.313358', 'lon': '37.905609'}, 'stat': {'id': 221266684, 'lat': 55.042065, 'lon': 37.685916}}, {'skpdi': {'id': 314569398, 'lat': '55.728683', 'lon': '38.834238'}, 'stat': {'id': 221289564, 'lat': 55.720246, 'lon': 37.199042}}, {'skpdi': {'id': 314569398, 'lat': '55.728683', 'lon': '38.834238'}, 'stat': {'id': 221330935, 'lat': 55.975071, 'lon': 37.30824}}, {'skpdi': {'id': 314569398, 'lat': '55.728683', 'lon': '38.834238'}, 'stat': {'id': 221335346, 'lat': 55.560911, 'lon': 37.780523}}, {'skpdi': {'id': 314569398, 'lat': '55.728683', 'lon': '38.834238'}, 'stat': {'id': 221437147, 'lat': 54.965149, 'lon': 39.026077}}, {'skpdi': {'id': 314888598, 'lat': '55.92148', 'lon': '37.761257'}, 'stat': {'id': 221289564, 'lat': 55.720246, 'lon': 37.199042}}, {'skpdi': {'id': 314888598, 'lat': '55.92148', 'lon': '37.761257'}, 'stat': {'id': 221330935, 'lat': 55.975071, 'lon': 37.30824}}, {'skpdi': {'id': 314888598, 'lat': '55.92148', 'lon': '37.761257'}, 'stat': {'id': 221335346, 'lat': 55.560911, 'lon': 37.780523}}, {'skpdi': {'id': 314888598, 'lat': '55.92148', 'lon': '37.761257'}, 'stat': {'id': 221437147, 'lat': 54.965149, 'lon': 39.026077}}, {'skpdi': {'id': 314927758, 'lat': '55.932039', 'lon': '37.493076'}, 'stat': {'id': 221267689, 'lat': 55.919332, 'lon': 37.498323}}, {'skpdi': {'id': 314927758, 'lat': '55.932039', 'lon': '37.493076'}, 'stat': {'id': 221267693, 'lat': 55.686893, 'lon': 37.867094}}, {'skpdi': {'id': 314927758, 'lat': '55.932039', 'lon': '37.493076'}, 'stat': {'id': 221267857, 'lat': 55.817113, 'lon': 37.849038}}, {'skpdi': {'id': 314927758, 'lat': '55.932039', 'lon': '37.493076'}, 'stat': {'id': 221333486, 'lat': 55.998579, 'lon': 37.422974}}, {'skpdi': {'id': 314927758, 'lat': '55.932039', 'lon': '37.493076'}, 'stat': {'id': 221334040, 'lat': 56.032049, 'lon': 37.87071}}, {'skpdi': {'id': 314927758, 'lat': '55.932039', 'lon': '37.493076'}, 'stat': {'id': 221374523, 'lat': 55.924478, 'lon': 37.492926}}, {'skpdi': {'id': 314927758, 'lat': '55.932039', 'lon': '37.493076'}, 'stat': {'id': 221435534, 'lat': 55.252414, 'lon': 37.505693}}, {'skpdi': {'id': 315055878, 'lat': '55.925537', 'lon': '37.49312'}, 'stat': {'id': 221267689, 'lat': 55.919332, 'lon': 37.498323}}, {'skpdi': {'id': 315055878, 'lat': '55.925537', 'lon': '37.49312'}, 'stat': {'id': 221267693, 'lat': 55.686893, 'lon': 37.867094}}, {'skpdi': {'id': 315055878, 'lat': '55.925537', 'lon': '37.49312'}, 'stat': {'id': 221267857, 'lat': 55.817113, 'lon': 37.849038}}, {'skpdi': {'id': 315055878, 'lat': '55.925537', 'lon': '37.49312'}, 'stat': {'id': 221333486, 'lat': 55.998579, 'lon': 37.422974}}, {'skpdi': {'id': 315055878, 'lat': '55.925537', 'lon': '37.49312'}, 'stat': {'id': 221334040, 'lat': 56.032049, 'lon': 37.87071}}, {'skpdi': {'id': 315055878, 'lat': '55.925537', 'lon': '37.49312'}, 'stat': {'id': 221374523, 'lat': 55.924478, 'lon': 37.492926}}, {'skpdi': {'id': 315055878, 'lat': '55.925537', 'lon': '37.49312'}, 'stat': {'id': 221435534, 'lat': 55.252414, 'lon': 37.505693}}, {'skpdi': {'id': 315138618, 'lat': '54.468925', 'lon': '38.78699'}, 'stat': {'id': 221439131, 'lat': 55.270645, 'lon': 38.499506}}, {'skpdi': {'id': 315138618, 'lat': '54.468925', 'lon': '38.78699'}, 'stat': {'id': 221439533, 'lat': 56.25582, 'lon': 38.171096}}, {'skpdi': {'id': 315138618, 'lat': '54.468925', 'lon': '38.78699'}, 'stat': {'id': 221440167, 'lat': 56.743769, 'lon': 37.187659}}, {'skpdi': {'id': 315261398, 'lat': '55.357095', 'lon': '38.222294'}, 'stat': {'id': 221435584, 'lat': 55.341483, 'lon': 38.222702}}, {'skpdi': {'id': 316624398, 'lat': '56.133518', 'lon': '37.308565'}, 'stat': {'id': 221437169, 'lat': 55.513617, 'lon': 36.990849}}, {'skpdi': {'id': 316624398, 'lat': '56.133518', 'lon': '37.308565'}, 'stat': {'id': 221437618, 'lat': 55.553332, 'lon': 37.724926}}, {'skpdi': {'id': 316624398, 'lat': '56.133518', 'lon': '37.308565'}, 'stat': {'id': 221437728, 'lat': 55.535528, 'lon': 37.067775}}, {'skpdi': {'id': 316624398, 'lat': '56.133518', 'lon': '37.308565'}, 'stat': {'id': 221473589, 'lat': 55.829577, 'lon': 36.869752}}, {'skpdi': {'id': 316624398, 'lat': '56.133518', 'lon': '37.308565'}, 'stat': {'id': 221474224, 'lat': 55.44367, 'lon': 37.753884}}, {'skpdi': {'id': 316920578, 'lat': '55.914606', 'lon': '37.892725'}, 'stat': {'id': 221286069, 'lat': 55.668146, 'lon': 38.39138}}, {'skpdi': {'id': 316920578, 'lat': '55.914606', 'lon': '37.892725'}, 'stat': {'id': 221333177, 'lat': 55.892232, 'lon': 37.744539}}, {'skpdi': {'id': 316920578, 'lat': '55.914606', 'lon': '37.892725'}, 'stat': {'id': 221334881, 'lat': 56.133606, 'lon': 37.30854}}, {'skpdi': {'id': 316920578, 'lat': '55.914606', 'lon': '37.892725'}, 'stat': {'id': 221335348, 'lat': 55.588678, 'lon': 37.733274}}, {'skpdi': {'id': 316920578, 'lat': '55.914606', 'lon': '37.892725'}, 'stat': {'id': 221340402, 'lat': 55.807106, 'lon': 38.979578}}, {'skpdi': {'id': 316920578, 'lat': '55.914606', 'lon': '37.892725'}, 'stat': {'id': 221437153, 'lat': 55.914636, 'lon': 37.892801}}]
    for sk in arr_skpdi:
        arr_skpdi_list.append(sk.id)
    for st in arr_stat_gpdd:
        arr_stat_list.append(st.sid)

    print(arr_skpdi_list)
    print(arr_stat_list)
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
                    if sk_col_type.skpdi_collision_type.name == st.dtvp:
                        if sk.lat and sk.lon and st.lat and st.lon:
                            # print('Добавлен элемент ' + str(i))
                            collision_dtp_array.append({
                                'skpdi': {
                                    'id': sk.id,
                                    'lat': sk.lat,
                                    'lon': sk.lon,
                                },
                                'stat': {
                                    'id': st.sid,
                                    'lat': st.lat,
                                    'lon': st.lon,
                                }
                            })
            i += 1
    arr_skpdi_list = list(set(arr_skpdi_list))
    arr_stat_list = list(set(arr_stat_list))
    return collision_dtp_array, arr_skpdi_list, arr_stat_list


# Функция создания лога скрипта
def create_last_index(max_id_skpdi, max_id_stat):
    now = datetime.now()
    last_id = AllDtpLastIndex.objects.create(update_time_field=now, skpdi_id=max_id_skpdi, stat_gibdd_id=max_id_stat)
    last_id.save()
    return True


# Функция очиски массивов от лишних элементов
def delete_same_index(collision_dtp_array, arr_skpdi_list, arr_stat_list):
    i = 0
    for item_dtp in collision_dtp_array:
        # Если точки не в одном радиусе
        if not check_coords(item_dtp['skpdi']['lat'], item_dtp['skpdi']['lon'], item_dtp['stat']['lat'],
                            item_dtp['stat']['lon']):
            collision_dtp_array.pop(i)
        i += 1
    # Удаление лишних элемеов из массива arr_skpdi_list и arr_stat_list
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
