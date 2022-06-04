from django.db import models


class BaseInfo(models.Model):
    name = models.CharField(max_length=40)
    ifa_date = models.DateField()
    product_type = models.CharField(max_length=40)
    pipe_type = models.CharField(max_length=20, null=True)
    dir_type = models.CharField(max_length=20)
    is_cnc = models.BooleanField()
    is_paired = models.BooleanField()
    path = models.CharField(max_length=255, unique=True, db_index=True)

    def __str__(self):
        return self.path


class QC(models.Model):
    sample_type1 = models.CharField(max_length=20, null=True)
    lib = models.CharField(max_length=80, null=True)
    read_len = models.IntegerField(null=True)
    avg_seq_dep = models.IntegerField(null=True)
    total_reads = models.IntegerField(null=True)
    total_bases = models.BigIntegerField(null=True)
    sample = models.ForeignKey(BaseInfo, on_delete=models.CASCADE)
