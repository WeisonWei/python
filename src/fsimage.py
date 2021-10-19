#!/usr/bin/env python3
import os, sys
import datetime, time
import configparser
import subprocess
from urllib import request
import json
import shutil

today = datetime.date.today().strftime("%Y%m%d")
yesterday = (datetime.date.today() - datetime.timedelta(1)).strftime("%Y%m%d")
last_3_day = (datetime.date.today() - datetime.timedelta(3)).strftime("%Y%m%d")
time_now = datetime.datetime.now().strftime("%H%M%S")


def run_shell(shell_content, wait=True, encoding='utf8'):
    res = subprocess.Popen(shell_content, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    results = []
    print("run_shell: \n%s" % shell_content)
    while True:
        line = res.stdout.readline().decode(encoding).strip()
        if line == '' and res.poll() is not None:
            break
        else:
            results.append(line)
    if wait:
        res.wait()
    return [res.returncode, '\n'.join(results)]


def translation_file(cluster, ns, command):
    # download_filse="/data1/fsimage_0000000003037961654"
    # download_filse=download_file_dir+"%s_%s"%(cluster,ns)
    submit_files = script_dir + "/submit"
    translate_shell = "%s oiv -i %s -o %s -p Delimited" % (submit_files, \
                                                           download_file, download_file + ".tsv")
    results = run_shell(translate_shell)
    if results[0] == 0:
        print("转译成功：%s" % translate_shell)
    else:
        print("转译失败：%s" % translate_shell)
        print("error: %s" % results[1])
        sys.exit(-1)


def check_nn_active():
    # ip_list = ["172.21.2.109","172.21.2.130"]
    ip_list = ["172.21.2.37", "172.21.2.38"]
    for ip in ip_list:
        url = "http://%s:50070/jmx?qry=Hadoop:service=NameNode,name=FSNamesystem" % ip
        response = request.urlopen("%s" % (url))
        result = json.loads(response.read().decode('GBK'))['beans'][0]['tag.HAState']
        if result == "active":
            return ip


if __name__ == "__main__":

    # global script_dir
    script_dir = os.path.split(os.path.realpath(__file__))[0]
    cluster = sys.argv[1]
    ns = sys.argv[2]

    # 从/data0/nn/current拷贝出来
    fsimage_dir = "/data0/nn/current/"
    # (startus,fsimage_file) = commands.getstatusoutput("ls -lt /data0/nn/current/|grep fsimage|grep -v md5|head -1|awk '{print $9}'")
    """
    flag = 0
    while True:
        num = len(os.listdir('/data0/nn/current/'))
        for filename in os.listdir('/data0/nn/current/'):
            if filename.startswith("fsimage.ckpt"):
                continue
            else:
                flag += 1
        print(num,flag)
        if num == flag:
            break
        else:
            time.sleep(5)
    """
    fsimage_file = \
    run_shell("ls -lt /data0/nn/current/|grep fsimage|grep -v md5|grep -v ckpt|head -1|awk '{print $9}'")[1]
    download_file = "/data1/%s_%s" % (cluster, ns)
    shutil.copy("%s/%s" % (fsimage_dir, fsimage_file), "%s" % download_file)

    # 开始转译
    download_file_dir = "/data1/"
    for i in range(3):
        translation_file(cluster, ns, "ovi")
        tsv_file = "/%s/%s_%s.tsv" % (download_file_dir, cluster, ns)
        if os.path.exists(tsv_file):
            if os.path.getsize(tsv_file) == 0:
                time.sleep(60)
                continue
            else:
                break
        else:
            shutil.copy("%s/%s" % (fsimage_dir, fsimage_file), "%s" % download_file)

    # 上传至hdfs
    ip = check_nn_active()
    todays = datetime.date.today().strftime("%Y-%m-%d")
    # cmd = "/software/servers/hadoop-2.7.1/bin/hdfs dfs -mkdir -p hdfs://%s:8020/user/hadp/bdm_fsimage_parse/dt=%s/cluster=%s_%s"%(ip,todays,cluster,ns)
    # run_shell(cmd)
    cmd = "/software/servers/hadoop-2.7.1/bin/hdfs dfs -mkdir -p hdfs://%s:8020/user/dd_edw/warehouse/bdm/bdm_fsimage_parse/dt=%s/cluster=%s_%s" % (
    ip, todays, cluster, ns)
    run_shell(cmd)
    cmd = "/software/servers/hadoop-2.7.1/bin/hdfs dfs -chmod -R 777 hdfs://%s:8020/user/dd_edw/warehouse/bdm/bdm_fsimage_parse/dt=%s" % (
    ip, todays)
    run_shell(cmd)
    # cmd = "/software/servers/hadoop-2.7.1/bin/hdfs dfs -put -f %s_%s.tsv hdfs://%s:8020/user/hadp/bdm_fsimage_parse/dt=%s/cluster=%s_%s/%s_%s.tsv"%(cluster,ns,ip,todays,cluster,ns,cluster,ns)
    # run_shell(cmd)
    cmd = "/software/servers/hadoop-2.7.1/bin/hdfs dfs -rm hdfs://%s:8020/user/dd_edw/warehouse/bdm/bdm_fsimage_parse/dt=%s/cluster=%s_%s/success" % (
    ip, todays, cluster, ns)
    run_shell(cmd)
    cmd = "/software/servers/hadoop-2.7.1/bin/hdfs dfs -put -f %s_%s.tsv hdfs://%s:8020/user/dd_edw/warehouse/bdm/bdm_fsimage_parse/dt=%s/cluster=%s_%s/%s_%s.tsv&&/software/servers/hadoop-2.7.1/bin/hdfs dfs -touchz hdfs://%s:8020/user/dd_edw/warehouse/bdm/bdm_fsimage_parse/dt=%s/cluster=%s_%s/_SUCCESS" % (
    cluster, ns, ip, todays, cluster, ns, cluster, ns, ip, todays, cluster, ns)

    run_shell(cmd)
    # cmd = "/software/servers/hadoop-2.7.1/bin/hdfs dfs -rm hdfs://%s:8020/user/dd_edw/warehouse/bdm/bdm_fsimage_parse/dt=%s/cluster=%s_%s/success"%(ip,todays,cluster,ns)
    # run_shell(cmd)

    # 清理文件
    os.remove(download_file)
    os.remove("%s/%s_%s.tsv" % (download_file_dir, cluster, ns))
