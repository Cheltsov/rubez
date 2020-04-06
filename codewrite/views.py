import math
from itertools import count

from django.shortcuts import render
from django.http import HttpResponse, JsonResponse
from codewrite.models import SkpdiDtpCard, StatGibddDtpCard


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
                dtp = SkpdiDtpCard.objects.filter(id=id_dtp).values().first()
            else:
                dtp = StatGibddDtpCard.objects.filter(sid=id_dtp).values().first()
            if not dtp:
                dtp = []
            return JsonResponse(dtp, safe=False)
        else:
            return HttpResponse('false')
    else:
        return HttpResponse('false')
