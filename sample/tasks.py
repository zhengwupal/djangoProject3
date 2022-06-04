# -*- coding:utf-8 -*-
# @Time: 2022/5/28 14:20
# @Author: Zhengwu Cai
# @Email: zhengwupal@163.com
import logging
import subprocess
from collections.abc import Iterable
from datetime import datetime
import os.path
import pandas as pd
import re
from sample.models import BaseInfo, QC
from celery import shared_task
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger('djangoProject3.sample.tasks')


def get_stdout(cmd):
    p = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if p.returncode == 0 and p.stdout.decode() != '':
        return [i for i in p.stdout.decode().split('\n') if i != '']


def flatten(items, ignore_type=(str, bytes)):
    for x in items:
        if isinstance(x, Iterable) and not isinstance(x, ignore_type):
            yield from flatten(x)
        else:
            yield x


class SamplePath:
    def __init__(self, ifa_date):
        self.ifa_date = self.set_ifa_date(ifa_date)
        self.sample_path = self.set_sample_path()

    @staticmethod
    def set_ifa_date(ifa_date):
        return ifa_date.date().strftime('%Y%m%d')

    def get_success_cmd(self):
        if self.ifa_date < datetime.strptime('20210601', '%Y%m%d').date().strftime('%Y%m%d'):
            return [f'ls /mnt/GenePlus00{disk_num}/prod/workspace/IFA{self.ifa_date}*/*/output/*/SUCCESS | ' \
                    f'grep -v log | grep -v MergeLaneFq | grep -v ReshapeFqDir'
                    for disk_num in range(1, 5)]
        elif self.ifa_date >= datetime.strptime('20210901', '%Y%m%d').date().strftime('%Y%m%d'):
            return [f'ls /mnt/GenePlus003/lims_workspace/prod/'
                    f'{self.ifa_date[0:4]}/{self.ifa_date[4:6]}/IFA{self.ifa_date}*/*/*/SUCCESS']
        else:
            bnc = [f'ls /mnt/GenePlus00{disk_num}/prod/workspace/IFA{self.ifa_date}*/*/output/*/SUCCESS | ' \
                   f'grep -v log | grep -v MergeLaneFq | grep -v ReshapeFqDir'
                   for disk_num in range(1, 5)]
            cnc = [f'ls /mnt/GenePlus003/lims_workspace/prod/'
                   f'{self.ifa_date[0:4]}/{self.ifa_date[4:6]}/IFA{self.ifa_date}*/*/*/SUCCESS']
            bnc.extend(cnc)
            return bnc

    @staticmethod
    def get_sample(success_cmd):
        return flatten([get_stdout(i) for i in success_cmd if get_stdout(i) is not None])

    def set_sample_path(self):
        success_cmd = self.get_success_cmd()
        sample_path = self.get_sample(success_cmd)
        for i in sample_path:
            yield os.path.dirname(i)


class SampleBaseInfo:
    def __init__(self, path):
        self.path = path
        self.is_cnc = self.set_is_cnc()
        self.product_type = self.set_product_type()
        self.name = self.set_name()
        self.ifa_date = self.set_ifa_date()
        self.is_paired = self.set_is_paired()
        self.pipe_type = self.set_pipe_type()
        self.dir_type = self.set_dir_type()

    def set_is_cnc(self):
        if 'lims_workspace' in self.path.split('/'):
            return True
        else:
            return False

    def set_product_type(self):
        if self.is_cnc:
            return self.path.split('/')[-2]
        else:
            return self.path.split('/')[-3]

    def set_name(self):
        return os.path.basename(self.path)

    def set_ifa_date(self):
        if self.is_cnc:
            return datetime.strptime(self.path.split('/')[-3][3:11], '%Y%m%d').date().strftime('%Y-%m-%d')
        else:
            return datetime.strptime(self.path.split('/')[-4][3:11], '%Y%m%d').date().strftime('%Y-%m-%d')

    def set_is_paired(self):
        if '_' in self.name:
            return True
        else:
            return False

    def set_pipe_type(self):
        if self.is_cnc:
            if any([(i in self.product_type) for i in ['OncoH', 'OncoTop', 'OncoDsingle']]):
                return 'PanelSingle'
            elif any([(i in self.product_type) for i in ['OncoD', 'DxPlasma', 'OncoET', 'OncoS']]):
                return 'PanelPair'
            elif any([(i in self.product_type) for i in ['OncoFusion']]):
                return 'WTSSingle'
            elif any([(i in self.product_type) for i in ['OncoHRD']]):
                return 'OncoHRD'
            elif any([(i in self.product_type) for i in ['OncoWES']]):
                return 'WESPair'
            elif any([(i in self.product_type) for i in ['OncoIR']]):
                return 'IRPair'
            elif any([(i in self.product_type) for i in ['DxIR']]):
                return 'IRSingle'
            else:
                return 'Others'
        else:
            return self.product_type.split('_')[0]

    def set_dir_type(self):
        if self.is_cnc:
            return re.findall(r'[A-Za-z-]+', self.product_type)[0]
        else:
            return self.product_type.split('_')[0]


# class SampleLib:
#     def __init__(self, path, is_cnc, dir_type):
#         self.path = path
#         self.is_cnc = is_cnc
#         self.dir_type = dir_type
#         self.lib = self.set_lib()
#
#     def set_lib(self):
#         if self.is_cnc:
#             if self.dir_type in ['OncoH', 'IRPair', 'IRSingle']:
#                 normal_lib_path = get_stdout(f'ls {self.path}/input/*L00*_N_* -d | head -n 1')
#                 return os.path.basename(normal_lib_path[0])
#             else:
#                 cancer_lib_path = get_stdout(f'ls {self.path}/input/*L00*_C_* -d | head -n 1')
#                 return os.path.basename(cancer_lib_path[0])
#         else:
#             return 'BNC'


class SampleQC:
    def __init__(self, path, is_cnc, dir_type):
        self.path = path
        self.is_cnc = is_cnc
        self.dir_type = dir_type
        self.qc = self.set_qc()

    def set_qc(self):
        if self.dir_type in ['OncoD', 'OncoS-BLung', 'OncoS-BColon', 'OncoS-BBreast', 'OncoS-B', 'OncoS-BLiver',
                             'OncoS-T', 'OncoD-Elung', 'MRD-B', 'MRD-T', 'OncoMD', 'OncoTNBC', 'DxPlasma', 'OncoET',
                             'OncoWES', 'OncoWES2', 'OncoHRD']:
            qc = get_stdout(f'cat {self.path}/report/qc/*.bam_qc.csv | cut -d , -f 2,3')
            if qc is not None:
                cancer_qc = [i.split(',')[0] for i in qc]
                normal_qc = [i.split(',')[1] for i in qc]
                return cancer_qc, normal_qc
            else:
                return None
        elif self.dir_type in ['OncoTop', 'OncoDsingle']:
            qc = get_stdout(f'cat {self.path}/report/qc/*.bam_qc.csv | cut -d , -f 2')
            if qc is not None:
                cancer_qc = [i.split(',')[0] for i in qc]
                return (cancer_qc,)
            else:
                return None
        elif self.dir_type in ['OncoFusion', 'OncoCUP']:
            qc = get_stdout(f'cat {self.path}/report/reports/*_QC.tsv | cut -f 2')
            if qc is not None:
                lib = '_'.join(
                    os.path.basename(get_stdout(f'ls {self.path}/input/*L00* -d | head -n 1')[0]).split('_')[4:])
                qc.append(lib)
                return (qc,)
            else:
                return None
        elif self.dir_type in ['OncoH']:
            qc = get_stdout(f'cat {self.path}/report/qc/*.bam_qc.csv | cut -d , -f 2')
            if qc is not None:
                return (qc,)
            else:
                return None
        elif self.dir_type in ['OncoIR']:
            return None
        elif self.dir_type in ['DxIR']:
            return None
        elif self.dir_type in ['OncoAi']:
            return None


def save_sample(ifa_date):
    logger.info(f'{ifa_date} START')
    sp = SamplePath(ifa_date)
    for i in sp.sample_path:
        logger.debug(f'{i}')
        sbi = SampleBaseInfo(i)
        bi = BaseInfo(
            name=sbi.name,
            ifa_date=sbi.ifa_date,
            product_type=sbi.product_type,
            pipe_type=sbi.pipe_type,
            dir_type=sbi.dir_type,
            is_cnc=sbi.is_cnc,
            is_paired=sbi.is_paired,
            path=sbi.path
        )
        logger.debug(f'{bi.path}')
        if not BaseInfo.objects.filter(path=sbi.path).exists():
            bi.save()
            sqc = SampleQC(sbi.path, sbi.is_cnc, sbi.dir_type)
            logger.debug(f'{sqc.qc}')
            if sqc.qc is not None:
                if sbi.dir_type in ['OncoD', 'OncoS-BLung', 'OncoS-BColon', 'OncoS-BBreast', 'OncoS-B', 'OncoS-BLiver',
                                    'OncoS-T', 'MRD-T', 'OncoMD', 'DxPlasma', 'OncoET', 'OncoWES', 'OncoWES2',
                                    'OncoTop', 'OncoH']:
                    for j in sqc.qc:
                        qc = QC(sample_type1=j[0], lib=j[1], read_len=j[3], avg_seq_dep=j[5], sample=bi)
                        qc.save()
                elif sbi.dir_type in ['OncoFusion']:
                    for j in sqc.qc:
                        qc = QC(sample_type1=j[0], lib=j[-1], total_reads=j[2], total_bases=j[4], sample=bi)
                        qc.save()
    logger.info(f'{ifa_date} SAVE SUCCESS')


@shared_task
def save_sample_test(date_str):
    ifa_date = datetime.strptime(date_str, '%Y%m%d')
    save_sample(ifa_date)


@shared_task
def save_sample_to_now():
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_sample = {executor.submit(save_sample, d): d for d in
                            pd.date_range('20210101', datetime.now(), freq='D')}
        for future in as_completed(future_to_sample):
            sample = future_to_sample[future]
            try:
                data = future.result()
            except Exception as e:
                logger.info(f'{sample} {e}')
            else:
                return data
