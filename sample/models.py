from django.db import models


class BaseInfo(models.Model):
    name = models.CharField(max_length=40)
    ifa_date = models.DateField()
    product_type = models.CharField(max_length=40)
    dir_type = models.CharField(max_length=20)
    is_cnc = models.BooleanField()
    is_paired = models.BooleanField()
    path = models.CharField(max_length=255)

    def __str__(self):
        return self.name


class QC(models.Model):
    c_lib = models.CharField(max_length=80)
    c_read_len = models.IntegerField()
    c_avg_seq_dep = models.IntegerField()
    c_total_bases = models.IntegerField()
    c_raw_reads = models.IntegerField()
    n_lib = models.CharField(max_length=80)
    n_read_len = models.IntegerField()
    n_avg_seq_dep = models.IntegerField()
    n_total_bases = models.IntegerField()
    n_raw_reads = models.IntegerField()
    sample = models.ForeignKey(BaseInfo, on_delete=models.CASCADE)
