# This is an auto-generated Django model module.
# You'll have to do the following manually to clean this up:
#   * Rearrange models' order
#   * Make sure each model has one field with primary_key=True
#   * Make sure each ForeignKey and OneToOneField has `on_delete` set to the desired behavior
#   * Remove `managed = False` lines if you wish to allow Django to create, modify, and delete the table
# Feel free to rename the models, but don't rename db_table values or field names.
from django.db import models


class AllDtpCard(models.Model):
    skpdi = models.OneToOneField('SkpdiDtpCard', models.DO_NOTHING, blank=True, null=True)
    stat_gibdd = models.OneToOneField('StatGibddDtpCard', models.DO_NOTHING, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'all_dtp_card'


class AllDtpCardHistory(models.Model):
    start_time = models.DateTimeField(blank=True, null=True)
    finish_time = models.DateTimeField(blank=True, null=True)
    finish = models.IntegerField()

    class Meta:
        managed = False
        db_table = 'all_dtp_card_history'


class AllDtpCollisionType(models.Model):
    skpdi_type = models.CharField(max_length=150)
    stat_type = models.CharField(max_length=150)

    class Meta:
        managed = False
        db_table = 'all_dtp_collision_type'


class AllDtpLastIndex(models.Model):
    skpdi_id = models.IntegerField(blank=True, null=True)
    stat_gibdd_id = models.IntegerField()
    update_time = models.DateTimeField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'all_dtp_last_index'


class CollisionRange(models.Model):
    cid_1 = models.IntegerField(blank=True, null=True)
    cid_2 = models.IntegerField(blank=True, null=True)
    range = models.IntegerField(blank=True, null=True)
    cid_skpdi_1 = models.IntegerField(blank=True, null=True)
    cid_skpdi_2 = models.IntegerField(blank=True, null=True)
    dtp_year = models.IntegerField(blank=True, null=True)
    dtp_month = models.IntegerField(blank=True, null=True)
    dtp_quarter = models.IntegerField(blank=True, null=True)
    id_collision_1 = models.IntegerField(blank=True, null=True)
    id_collision_2 = models.IntegerField(blank=True, null=True)
    icon_type = models.IntegerField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'collision_range'


class HearthCollision(models.Model):
    cid = models.ForeignKey(AllDtpCard, models.DO_NOTHING, db_column='cid', blank=True, null=True)
    hid = models.ForeignKey('HearthDtp', models.DO_NOTHING, db_column='hid', blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'hearth_collision'


class HearthDtp(models.Model):
    created = models.DateTimeField(blank=True, null=True)
    year = models.IntegerField(blank=True, null=True)
    month = models.IntegerField(blank=True, null=True)
    quarter = models.IntegerField(blank=True, null=True)
    count_dtp = models.IntegerField(blank=True, null=True)
    type = models.IntegerField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'hearth_dtp'


class Line(models.Model):
    coords = models.TextField(blank=True, null=True)  # This field type is a guess.
    lon = models.FloatField(blank=True, null=True)
    lat = models.FloatField(blank=True, null=True)
    line_type_sid = models.CharField(max_length=50, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'line'


class MapRoadSection(models.Model):
    geo = models.TextField(blank=True, null=True)  # This field type is a guess.
    car_speed_limit = models.IntegerField(blank=True, null=True)
    truck_speed_limit = models.IntegerField(blank=True, null=True)
    risk_institute_number = models.IntegerField(blank=True, null=True)
    risk_institute_category = models.CharField(max_length=1, blank=True, null=True)
    risk_institute_traffic_intensity = models.DecimalField(max_digits=65535, decimal_places=65535, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'map_road_section'


class SkpdiAttachmentKind(models.Model):
    id = models.BigIntegerField(primary_key=True)
    code = models.CharField(max_length=255, blank=True, null=True)
    name = models.CharField(max_length=255, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'skpdi_attachment_kind'


class SkpdiCollisionPartyCategory(models.Model):
    id = models.BigIntegerField(primary_key=True)
    code = models.CharField(max_length=255, blank=True, null=True)
    name = models.CharField(max_length=255, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'skpdi_collision_party_category'


class SkpdiCollisionPartyCond(models.Model):
    id = models.BigIntegerField(primary_key=True)
    code = models.CharField(max_length=255, blank=True, null=True)
    name = models.CharField(max_length=255, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'skpdi_collision_party_cond'


class SkpdiCollisionType(models.Model):
    id = models.BigIntegerField(primary_key=True)
    code = models.CharField(max_length=255, blank=True, null=True)
    name = models.CharField(max_length=255, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'skpdi_collision_type'


class SkpdiCollisionTypeCollision(models.Model):
    id = models.BigIntegerField(primary_key=True)
    skpdi_collision_type = models.ForeignKey(SkpdiCollisionType, models.DO_NOTHING)
    skpdi_dtp_card = models.ForeignKey('SkpdiDtpCard', models.DO_NOTHING)

    class Meta:
        managed = False
        db_table = 'skpdi_collision_type_collision'


class SkpdiDtpCard(models.Model):
    id = models.BigIntegerField(primary_key=True)
    collision_date = models.DateTimeField(blank=True, null=True)
    coordinates = models.TextField(blank=True, null=True)  # This field type is a guess.
    collision_context = models.TextField(blank=True, null=True)
    damage = models.TextField(blank=True, null=True)
    vehicle_quantity = models.SmallIntegerField(blank=True, null=True)
    related_road_conditions = models.TextField(blank=True, null=True)
    lastchanged = models.DateTimeField(blank=True, null=True)
    name = models.CharField(max_length=255, blank=True, null=True)
    skpdi_odh = models.ForeignKey('SkpdiOdh', models.DO_NOTHING, blank=True, null=True)
    lat = models.FloatField(blank=True, null=True)
    lon = models.FloatField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'skpdi_dtp_card'


class SkpdiFile(models.Model):
    id = models.BigIntegerField(primary_key=True)
    skpdi_dtp_card = models.ForeignKey(SkpdiDtpCard, models.DO_NOTHING)
    guid = models.CharField(max_length=64, blank=True, null=True)
    kind = models.ForeignKey(SkpdiAttachmentKind, models.DO_NOTHING, db_column='kind', blank=True, null=True)
    file = models.BinaryField(blank=True, null=True)
    url = models.TextField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'skpdi_file'


class SkpdiOdh(models.Model):
    id = models.BigIntegerField(primary_key=True)
    start_coordinate = models.TextField(blank=True, null=True)  # This field type is a guess.
    end_coordinate = models.TextField(blank=True, null=True)  # This field type is a guess.
    geometry = models.TextField(blank=True, null=True)  # This field type is a guess.
    name = models.TextField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'skpdi_odh'


class SkpdiUch(models.Model):
    id = models.BigIntegerField(primary_key=True)
    skpdi_dtp_card = models.ForeignKey(SkpdiDtpCard, models.DO_NOTHING)
    collision_party_category = models.ForeignKey(SkpdiCollisionPartyCategory, models.DO_NOTHING, blank=True, null=True)
    collision_party_cond = models.ForeignKey(SkpdiCollisionPartyCond, models.DO_NOTHING, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'skpdi_uch'


class StatGibddDtpCard(models.Model):
    sid = models.IntegerField(primary_key=True)
    created_date = models.DateTimeField(blank=True, null=True)
    dtp_date = models.DateTimeField()
    dtvp = models.TextField(blank=True, null=True)
    district = models.TextField(blank=True, null=True)
    dor = models.TextField(blank=True, null=True)
    km = models.IntegerField(blank=True, null=True)
    m = models.IntegerField(blank=True, null=True)
    np = models.TextField(blank=True, null=True)
    lon = models.FloatField(blank=True, null=True)
    lat = models.FloatField(blank=True, null=True)
    pog = models.IntegerField(blank=True, null=True)
    ran = models.IntegerField(blank=True, null=True)
    spog = models.TextField(blank=True, null=True)
    s_pch = models.TextField(blank=True, null=True)
    osv = models.TextField(blank=True, null=True)
    ndu = models.TextField(blank=True, null=True)
    file_name = models.CharField(max_length=255, blank=True, null=True)
    row_num = models.IntegerField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'stat_gibdd_dtp_card'


class StatGibddTsInfo(models.Model):
    sid = models.IntegerField(primary_key=True)
    dtp_card_sid = models.ForeignKey(StatGibddDtpCard, models.DO_NOTHING, db_column='dtp_card_sid')
    ts_s = models.TextField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'stat_gibdd_ts_info'


class StatGibddTsUch(models.Model):
    sid = models.IntegerField(primary_key=True)
    ts_info_sid = models.ForeignKey(StatGibddTsInfo, models.DO_NOTHING, db_column='ts_info_sid')
    npdd = models.TextField(blank=True, null=True)
    sop_npdd = models.TextField(blank=True, null=True)
    k_uch = models.TextField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'stat_gibdd_ts_uch'


class StatGibddUchInfo(models.Model):
    sid = models.IntegerField(primary_key=True)
    dtp_card_sid = models.ForeignKey(StatGibddDtpCard, models.DO_NOTHING, db_column='dtp_card_sid')
    npdd = models.TextField(blank=True, null=True)
    sop_npdd = models.TextField(blank=True, null=True)
    k_uch = models.TextField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'stat_gibdd_uch_info'
