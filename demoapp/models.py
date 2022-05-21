# -*- coding:utf-8 -*-
# @Time: 2022/5/21 13:14
# @Author: Zhengwu Cai
# @Email: zhengwupal@163.com
from django.db import models


class Widget(models.Model):
    name = models.CharField(max_length=140)
