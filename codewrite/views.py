import math, json, time
from itertools import count

from django.shortcuts import render
from django.http import HttpResponse, JsonResponse, Http404
from codewrite.models import SkpdiDtpCard, StatGibddDtpCard, HearthCollision, HearthDtp, AllDtpCard

from django.core.serializers import serialize



# Create your views here.
def index(request):
    context = {
        'list': []
    }
    return render(request, 'codewrite/index.html', context)


# Получение JSON SKPDI из файла json
def get_skpdi(request):
    f = open('media/json_skpdi.json', 'r')
    return HttpResponse(f)


# Получение JSON STAT_GIPDD из файла json
def get_stat_gipdd(request):
    f = open('media/json_stat.json', 'r')
    return HttpResponse(f)


# Получение верифицированных ДПТ из файла json
def get_collision(request):
    f = open('media/json_col.json', 'r')
    return HttpResponse(f)


# Получение ДПТ по id и типу
def get_dtp_id(request):
    if request.GET:
        if 'id' in request.GET and 'type' in request.GET:
            id_dtp = request.GET['id']
            type_dtp = request.GET['type']
            if type_dtp == 'skpdi':
                dtp = SkpdiDtpCard.objects.filter(id=id_dtp).first()
                list_collision = []
                if hasattr(dtp, 'skpdicollisiontypecollision_set'):
                    type_collision = dtp.skpdicollisiontypecollision_set.all()
                    if type_collision:
                        for tc in type_collision:
                            list_collision.append({
                                'collisionType': tc.skpdi_collision_type.name
                            })
                list_uch = []
                if hasattr(dtp, 'skpdiuch_set'):
                    uch_skpdi = dtp.skpdiuch_set.all()
                    if uch_skpdi:
                        for item in uch_skpdi:
                            category_name = ''
                            category = item.collision_party_category
                            if category:
                                category_name = category.name
                            list_uch.append({
                                'collisionPartyCond': item.collision_party_cond.name,
                                'collisionPartyCategory': category_name,
                            })

                list_file = []
                if hasattr(dtp, 'skpdifile_set'):
                    file_skpdi = dtp.skpdifile_set.all()
                    if file_skpdi:
                        for file in file_skpdi:
                            list_file.append({
                                'guid': file.guid,
                                'kind': file.kind.name,
                                'file': file.file,
                                'url': file.url
                            })

                json_list = {
                    'id': dtp.id,
                    'collision_date': dtp.collision_date,
                    'coordinates': dtp.coordinates,
                    'collision_context': dtp.collision_context,
                    'damage': dtp.damage,
                    'vehicle_quantity': dtp.vehicle_quantity,
                    'related_road_conditions': dtp.related_road_conditions,
                    'lastchanged': dtp.lastchanged,
                    'name': dtp.name,
                    'lat': dtp.lat,
                    'lon': dtp.lon,
                    'collisionTypes': list_collision,
                    'uchs': list_uch,
                    'files': list_file
                }
                dtp = json_list
            else:
                dtp = StatGibddDtpCard.objects.filter(sid=id_dtp).first()

                ts_info_list = []
                if hasattr(dtp, 'statgibddtsinfo_set'):
                    ts_info_arr = dtp.statgibddtsinfo_set.all()
                    if ts_info_arr:
                        for ts in ts_info_arr:
                            ts_uch_list = []
                            if hasattr(ts, 'statgibddtsuch_set'):
                                ts_uch_arr = ts.statgibddtsuch_set.all()
                                if ts_uch_arr:
                                    for ts2 in ts_uch_arr:
                                        ts_uch_list.append({
                                            'npdd': ts2.npdd,
                                            'sop_npdd': ts2.sop_npdd,
                                            'k_uch': ts2.k_uch
                                        })
                            ts_info_list.append({
                                'ts': ts.ts_s,
                                'ts_uch': ts_uch_list
                            })

                uch_list = []
                if hasattr(dtp, 'statgibdduchinfo_set'):
                    uch_info_arr = dtp.statgibdduchinfo_set.all()
                    if uch_info_arr:
                        for uch in uch_info_arr:
                            uch_list.append({
                                'npdd': uch.npdd,
                                'sop_npdd': uch.sop_npdd,
                                'k_uch': uch.k_uch
                            })

                json_list = {
                    'sid': dtp.sid,
                    'created_date': dtp.created_date,
                    'dtp_date': dtp.dtp_date,
                    'dtvp': dtp.dtvp,
                    'district': dtp.district,
                    'dor': dtp.dor,
                    'km': dtp.km,
                    'm': dtp.m,
                    'np': dtp.np,
                    'lon': dtp.lon,
                    'lat': dtp.lat,
                    'pog': dtp.pog,
                    'ran': dtp.ran,
                    'spog': dtp.spog,
                    's_pch': dtp.s_pch,
                    'osv': dtp.osv,
                    'ndu': dtp.ndu,
                    'file_name': dtp.file_name,
                    'row_num': dtp.row_num,
                    'tsInfo': ts_info_list,
                    'uchInfo': uch_list,
                }
                dtp = json_list
            if not dtp:
                dtp = []
            return JsonResponse(dtp, safe=False)
        else:
            return HttpResponse('false')
    else:
        return HttpResponse('false')


def get_hearth(request):
    start_time = time.time()
    if request.GET:
        if request.GET.get('year') and request.GET.get('quarter'):
            year_page = request.GET['year']
            quarter_page = request.GET['quarter']
            hearth_list = HearthDtp.objects.filter(year=year_page, quarter=quarter_page)[:10]
            response = create_list_heath(hearth_list)
            print("--- %s seconds ---" % (time.time() - start_time))
            return HttpResponse(response)
        elif request.GET.get('year'):
            year_page = request.GET['year']
            hearth_list = HearthDtp.objects.filter(year=year_page)[:10]
            response = create_list_heath(hearth_list)
            print("--- %s seconds ---" % (time.time() - start_time))
            return HttpResponse(response)
        else:
            raise Http404('Invalid parameters')
    else:
        raise Http404('Object not found')


def create_list_heath(hearth_list):
    list_hearth_json = []
    for item in hearth_list:
        list_dtp = []
        count_stricken = 0
        count_lost = 0
        list_dtp_collision = item.hearthcollision_set.all()
        for item_col in list_dtp_collision:
            dtp = item_col.cid
            tmp = dtp.stat_gibdd
            if dtp.skpdi_id and dtp.stat_gibdd_id:
                if dtp.stat_gibdd.pog == 1:
                    count_lost += 1
                elif dtp.stat_gibdd.ran == 1:
                    count_stricken += 1
                list_dtp.append({
                    'id': dtp.id,
                    'id_skpdi': dtp.skpdi_id,
                    'id_stat': dtp.stat_gibdd_id,
                    'date': str(dtp.stat_gibdd.dtp_date),
                    'coords': {
                        'lat': dtp.stat_gibdd.lat,
                        'lon': dtp.stat_gibdd.lon
                    }
                })
            elif dtp.skpdi_id:
                dead_type = dtp.skpdi.skpdiuch_set.first().collision_party_cond_id
                if dead_type == 216599770:
                    count_stricken += 1
                else:
                    count_lost += 1
                list_dtp.append({
                    'id': dtp.id,
                    'id_skpdi': dtp.skpdi_id,
                    'id_stat': '',
                    'date': str(dtp.skpdi.collision_date),
                    'coords': {
                        'lat': dtp.skpdi.lat,
                        'lon': dtp.skpdi.lon
                    }
                })
            else:
                if dtp.stat_gibdd.pog == 1:
                    count_lost += 1
                elif dtp.stat_gibdd.ran == 1:
                    count_stricken += 1
                list_dtp.append({
                    'id': dtp.id,
                    'id_skpdi': '',
                    'id_stat': dtp.stat_gibdd_id,
                    'date': str(dtp.stat_gibdd.dtp_date),
                    'coords': {
                        'lat': dtp.stat_gibdd.lat,
                        'lon': dtp.stat_gibdd.lon
                    }
                })

        list_hearth_json.append({
            'id': item.id,
            'count_dtp': item.count_dtp,
            'quarter': item.quarter,
            'icon_type': item.type,
            'count_stricken': count_stricken,
            'count_lost': count_lost,
            'list_dtp': list_dtp
        })
    response = json.dumps(list_hearth_json)
    return response
