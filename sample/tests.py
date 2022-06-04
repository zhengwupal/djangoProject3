from django.test import TestCase
from sample.tasks import save_sample_test, save_sample_to_now
from sample.models import BaseInfo


class MyTestCase(TestCase):
    # save_sample_test('20210404')
    save_sample_to_now()
    # a = BaseInfo.objects.filter(product_type__contains='OncoD', qc__sample_type1='cancer',
    #                             qc__avg_seq_dep__range=(100, 1000)).all()[:20]
    # print(a.values('path'))
    # for i in a:
    #     print(i.qc_set.all().filter(sample_type1='cancer').values('avg_seq_dep'))
