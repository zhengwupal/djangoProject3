# -*- coding:utf-8 -*-
# @Time: 2022/5/28 14:20
# @Author: Zhengwu Cai
# @Email: zhengwupal@163.com
# from .models import Info, QC
import subprocess
from collections.abc import Iterable
from datetime import datetime
import os.path


def get_stdout(cmd):
    p = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if p.returncode == 0:
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
        return datetime.strptime(ifa_date, '%Y%m%d').date().strftime('%Y%m%d')

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
            return self.path.split('/')[-3][3:11]
        else:
            return self.path.split('/')[-4][3:11]

    def set_is_paired(self):
        if '_' in self.name:
            return True
        else:
            return False

    def set_pipe_type(self):
        if self.is_cnc:
            if any([(i in self.product_type) for i in ['OncoD', 'DxPlasma', 'OncoET', 'OncoS']]):
                return 'PanelPair'
            elif any([(i in self.product_type) for i in ['OncoH', 'OncoTop']]):
                return 'PanelSingle'
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
            return 'BNC'

    def set_dir_type(self):
        if self.is_cnc:
            if any([(i in self.product_type) for i in ['OncoD', 'DxPlasma', 'OncoET', 'OncoS']]):
                return 'PanelPair'
            elif any([(i in self.product_type) for i in ['OncoH']]):
                return 'OncoH'
            elif any([(i in self.product_type) for i in ['OncoTop']]):
                return 'OncoTop'
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
            elif any([(i in self.product_type) for i in ['OncoAi']]):
                return 'OncoAi'
            else:
                return 'Others'
        else:
            return 'BNC'


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
        self.c_lib = None
        self.c_read_len = None
        self.c_avg_seq_dep = None
        self.c_total_bases = None
        self.c_raw_reads = None
        self.n_lib = None
        self.n_read_len = None
        self.n_avg_seq_dep = None
        self.n_total_bases = None
        self.n_raw_reads = None
        self.cancer_qc, self.normal_qc = self.set_qc()

    def set_qc(self):
        if self.dir_type in ['PanelPair', 'WESPair']:
            qc = get_stdout(f'cat {self.path}/report/qc/*.bam_qc.csv | cut -d , -f 2,3')
            cancer_qc = [i.split(',')[0] for i in qc]
            normal_qc = [i.split(',')[1] for i in qc]
            self.c_lib = cancer_qc[1]
            self.c_read_len = cancer_qc[3]
            self.c_avg_seq_dep = cancer_qc[5]
            self.n_lib = normal_qc[1]
            self.n_read_len = normal_qc[3]
            self.n_avg_seq_dep = normal_qc[5]
            return cancer_qc, normal_qc
        elif self.dir_type in ['OncoTop']:
            qc = get_stdout(f'cat {self.path}/report/qc/*.bam_qc.csv | cut -d , -f 2')
            cancer_qc = [i.split(',')[0] for i in qc]
            self.c_lib = cancer_qc[1]
            self.c_read_len = cancer_qc[3]
            self.c_avg_seq_dep = cancer_qc[5]
            return cancer_qc, None
        elif self.dir_type in ['WTSSingle']:
            cancer_qc = get_stdout(f'cat {self.path}/report/reports/*_QC.tsv | cut -f 2')
            self.c_raw_reads = cancer_qc[3]
            self.c_total_bases = cancer_qc[4]
            return cancer_qc, None
        elif self.dir_type in ['OncoH']:
            normal_qc = get_stdout(f'cat {self.path}/report/qc/*.bam_qc.csv | cut -d , -f 2')
            self.n_lib = normal_qc[1]
            self.n_read_len = normal_qc[3]
            self.n_avg_seq_dep = normal_qc[5]
            return None, normal_qc
        elif self.dir_type in ['IRPair']:
            return None, None
        elif self.dir_type in ['IRSingle']:
            return None, None
        elif self.dir_type in ['OncoAi']:
            return None, None


def save_sample(ifa_date):
    sp = SamplePath(ifa_date)
    for i in sp.sample_path:
        sb = SampleBaseInfo(i)
        sqc = SampleQC(sb.path, sb.is_cnc, sb.dir_type)
        print(sqc.path)
        print(sqc.c_lib)
        print(sqc.n_lib)


save_sample('20220210')
