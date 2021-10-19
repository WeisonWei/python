'''
Usage:  X warn,error nn start:20210921 end:20210929
        
        # classify log
        python filter-log.py X warn,error nn start:20210921 end:20210929

        # only output classify result
        python filter-log.py Y warn,error nn start:20210921 end:20210929

        # exception
        python filter-log.py nn exception
'''

from __future__ import print_function

import datetime
import os
import shutil
import sys
import tempfile

import re


def append_to(filename, content):
    with open(filename, 'a') as f:
        f.write(content)


classify_regex = "^\[.*\[(DEBUG|INFO|WARN|ERROR|FATAL)\] .*?(?=\])\]{1,2} (.*)$"
nn_replace_regex = "(?<=data|bdir)\d{1,3}|\d{1,10}(?=ms|ns)" \
                   "|(?<=BP-)[0-9.-]*" \
                   "|(?<=DS-)[0-9a-z-]*" \
                   "|(?<=blk_)[0-9_-]*|(?<=blockId=)[0-9_-]*" \
                   "|\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:?\d{,5}" \
                   "|(?<=PacketResponder:).*$" \
                   "|(?<=lock identifier:).*$" \
                   "|(?<=@)[0-9a-z]*" \
                   "|(?<=data validation:).*(?=failed)" \
                   "|(?<=File ).*(?=does not)" \
                   "|(?<=set path ).*(?=is error)" \
                   "|(?<=offsetInBlock).*" \
                   "|(?<=Call#)\d*" \
                   "|(?<=Retry#)\d*" \
                   "|(?<=IPC Server handler )\d*" \
                   "|(?<=WARN : DN )[0-9a-z]{8}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{12}.* requested a lease even.*" \
                   "|(?<=WARN : Took).*to remote journal.*"

nn_remove_regex = "Failed to find datanode DatanodeRegistration" \
                  "|Failed to place enough replicas" \
                  "|Try to delete last internal block for dn" \
                  "|Lease of .* held by HDFS_NameNode.* has timeout" \
                  "|chooseTarget spend time than" \
                  "|internalReleaseLease: All existing blocks are COMPLETE, lease removed" \
                  "|has not been closed. Lease recovery is in progress" \
                  "|Got incremental block report from unregistered or dead node" \
                  "|processIncrementalBlockReport is received from dead or unregistered node DatanodeRegistration" \
                  "|because the DN is not in the pending set" \
                  "|Large response size" \
                  "|FSDirectory.unprotectedRenameTo: failed to rename" \
                  "|NameNode.blockReceivedAndDeleted: failed from DatanodeRegistration" \
                  "|RECEIVED SIGNAL 15: SIGTERM" \
                  "|mapping's userKey  = null" \
                  "|libisal.so.2" \
                  "|\[extends log\].*effectiveUser = blk_" \
                  "|Cluster IDs not matched: dn cid=" \
                  "|IPC Server handler .* output error" \
                  "|Cluster IDs not matched: dn cid=" \
                  "|DIR\* FSDirectory.unprotectedRenameTo" \
                  "|requested namenode lease even though it wasn't yet registered" \
                  "|bytes\) to remote journal" \
                  "|PendingReconstructionMonitor timed out blk_" \
                  "|Get namenode task ConvertTaskInfo" \
                  "|maybe redispatch cause of fail" \
                  "|system/Convert-History" \
                  "|system/Convertor-Backup" \
                  "|TTL worker thread can't list" \
                  "|BLOCK\* addStoredBlock: Redundant addStoredBlock" \
                  "|is expected to be removed from an unrecorded" \
                  "|Disk error on DatanodeRegistration" \
                  "|is not in whitelist" \
                  "|rename source.*is not found" \
                  "|ReplicationWork reconstruction chosen targets is empty" \
                  "|PendingReconstructionMonitor timed out" \
                  "|Failed to APPEND_FILE" \
                  "|Add block blk_.* to datanode*reconstruct queue" \
                  "|redispatch task" \
                  "|copy .* to .* failed:" \
                  "|ignore the task" \
                  "|relative job found" \
                  "|fail to add convert job.*" \
                  "|Error: can't add leaf node .* at depth .* to topology" \
                  "|Remote journal .* failed to write txns" \
                  "|Removed empty last block and closed file" \
                  "|HTTP POST Request Failure! Url: http://baizepg.*" \
                  "|Worker start" \
                  "|Monitor start" \
                  "|Only one image storage directory" \
                  "|BR lease .* is not valid .* because the lease has expired." \
                  "|[after exclude: ]weightSum -" \
                  "|rename destination .* already exist" \
                  "|Unresolved topology mapping" \
                  "|logAuditEvent JSONException client"

dn_replace_regex = "(?<=data|bdir)\d{1,3}|\d{1,10}(?=ms|ns)" \
                   "|(?<=BP-)[0-9.-]*" \
                   "|(?<=DS-)[0-9a-z-]*" \
                   "|(?<=blk_)[0-9_-]*|(?<=blockId=)[0-9_-]*" \
                   "|\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:?\d{,5}" \
                   "|(?<=PacketResponder:).*$" \
                   "|(?<=lock identifier:).*$" \
                   "|(?<=@)[0-9a-z]*" \
                   "|(?<=data validation:).*(?=failed)" \
                   "|(?<=File ).*(?=does not)" \
                   "|(?<=set path ).*(?=is error)" \
                   "|(?<=offsetInBlock).*" \
                   "|(?<=Call#)\d*" \
                   "|(?<=Retry#)\d*" \
                   "|(?<=IPC Server handler )\d*"

dn_remove_regex = "Failed to find datanode DatanodeRegistration" \
                  "|Failed to place enough replicas" \
                  "|Try to delete last internal block for dn" \
                  "|Lease of .* held by HDFS_NameNode.* has timeout" \
                  "|chooseTarget spend time than" \
                  "|internalReleaseLease: All existing blocks are COMPLETE, lease removed" \
                  "|has not been closed. Lease recovery is in progress" \
                  "|Got incremental block report from unregistered or dead node" \
                  "|processIncrementalBlockReport is received from dead or unregistered node DatanodeRegistration" \
                  "|because the DN is not in the pending set" \
                  "|Large response size" \
                  "|FSDirectory.unprotectedRenameTo: failed to rename" \
                  "|NameNode.blockReceivedAndDeleted: failed from DatanodeRegistration" \
                  "|RECEIVED SIGNAL 15: SIGTERM" \
                  "|mapping's userKey  = null" \
                  "|libisal.so.2" \
                  "|\[extends log\].*effectiveUser = blk_" \
                  "|Cluster IDs not matched: dn cid=" \
                  "|DIR\* FSDirectory.unprotectedRenameTo" \
                  "|requested namenode lease even though it wasn't yet registered" \
                  "|bytes\) to remote journal" \
                  "|PendingReconstructionMonitor timed out blk_" \
                  "|Get namenode task ConvertTaskInfo" \
                  "|maybe redispatch cause of fail" \
                  "|system/Convert-History" \
                  "|system/Convertor-Backup" \
                  "|TTL worker thread can't list" \
                  "|BLOCK\* addStoredBlock: Redundant addStoredBlock" \
                  "|is expected to be removed from an unrecorded" \
                  "|Operation category READ is not supported in state standby" \
                  "|ReplicationWork reconstruction chosen targets is empty" \
                  "|Failed to call blockReceivedAndDeleted" \
                  "|ERROR : Command:\[\[" \
                  "|ec_cmd\.sh" \
                  "|Connection refused" \
                  "|socket timeout exception" \
                  "|hadoop/ConnectionRefused" \
                  "|Failed to renew lease for" \
                  "|Call to .* error: NameNode still not started" \
                  "|Can't replicate block .* because on-disk length .* is shorter than NameNode recorded length .*" \
                  "|Call to .* error: DestHost:destPort .* , LocalHost:.*. Failed on local exception: java.io.IOException: Connection reset by peer" \
                  "|Start check disk error scan: java.io.EOFException" \
                  "|RemoteException in offerService" \
                  "|A packet was last sent .*ms ago. Maximum idle time: .*ms." \
                  "|The downstream error might be due to congestion in upstream including this node. Propagating the error" \
                  "|Slow BlockReceiver write packet to mirror took .*ms (threshold=.*ms), downstream DNs=.*" \
                  "|IOException in BlockReceiver.run().*" \
                  "|Lock held time above threshold: lock identifier:.*" \
                  "|Call to .* error: Call From .* to .* failed on connection exception: java.net.ConnectException: Connection refused" \
                  "|Scanning directory .* take .*ms over limit .*ms" \
                  "|NetIperfClientMonitor start" \
                  "|NetIperfServerMonitor start" \
                  "|IOException in offerService" \
                  "|Slow manageWriterOsCache took .*ms (threshold=.*ms), volume=file:/data.*/dfs/, blockId=" \
                  "|Slow PacketResponder send ack to upstream took .*ms (threshold=.*ms), PacketResponder:" \
                  "|Slow flushOrSync took .*ms (threshold=.*ms), isSync:false, flushTotalNanos=.*ns, volume=file:/data.*/dfs/, blockId=" \
                  "|Waited above threshold to acquire lock: lock identifier:" \
                  "|Slow BlockReceiver write data to disk cost:.*ms (threshold=.*ms), volume=file:/data.*/dfs/, blockId=" \
                  "|Could not get disk usage information" \
                  "|Block BP-.*:blk_.* unfinalized and removed" \
                  "|Detected pause in JVM or host machine (eg GC): pause of approximately .*ms" \
                  "|Call to .* error: ProcessReport from dead or unregistered node: DatanodeRegistration.*" \
                  "|Call to .* error: Call From .* to .* failed on socket timeout exception: java.net.SocketTimeoutException: .* millis timeout while waiting for channel to be ready for read. ch" \
                  "|Couldn't report bad block BP-.*:blk_.* to Block pool BP-.* (Datanode Uuid .*) service to" \
                  "|Maybe scanning directory .*/dfs/current/BP-.*/current/finalized take" \
                  "|Reporting bad BP-.*:blk_.* on /data.*" \
                  "|Failed to call blockReceivedAndDeleted: \[DatanodeStorage.*RECEIVING_BLOCK.*" \
                  "|DatanodeRegistration.*, datanodeUuid=.*" \
                  "|SocketTimeoutException: Call From.*50020"

jn_remove_regex = "Sync of transaction range .* took .*ms	" \
                  "|Client is requesting a new log segment" \
                  "|Caught exception after scanning through .* Position was " \
                  "|Latest log .* has no transactions. moving it aside	" \
                  "|Unable to delete no-longer-needed editlog" \
                  "|WARN : Committed before 500 getedit failed" \
                  "|After resync, position is .*" \
                  "|Received an invalid request file transfer request from"

# WARN nn X
levels = ['DEBUG', 'INFO', 'WARN', 'ERROR', 'FATAL']

argvs = sys.argv[1:]
# print(argvs, file=sys.stderr)
# sys.exit(0)
classify = True if 'X' in argvs else False
if classify:
    argvs.remove('X')
classify = True if 'Y' in argvs else False

only_classify_result = False
if 'Y' in argvs:
    only_classify_result = True
    classify = True
    argvs.remove('Y')

start_time = None
end_time = None

for arg in argvs:
    if arg.find("start") >= 0:
        start_time = arg[6:]
    if arg.find("end") >= 0:
        end_time = arg[4:]

select = argvs[0].upper().split(',')
print("filter in: " + str(select), file=sys.stderr)

is_exception = select[0].upper().find("EXCEPTION") >= 0
role = argvs[1:][0]
print("start role: " + role)

classify_pattern = re.compile(classify_regex)

role_list = ["nn", "dn", "jn"]
log_prefix_list = ["namenode", "datanode", "journalnode"]
excludes_list = ["datanode", "namenode", "namenode"]
replace_pattern_list = [nn_replace_regex, dn_replace_regex, dn_replace_regex]
remove_pattern_list = [nn_remove_regex, dn_remove_regex, jn_remove_regex]

for i in range(len(role_list)):
    if role == role_list[i]:
        log_prefix = log_prefix_list[i]
        exclude = excludes_list[i]
        replace_pattern = re.compile(replace_pattern_list[i])
        remove_pattern = re.compile(remove_pattern_list[i])

log_files = []
file_dir = "/data0/hadoop-logs"

walk = os.walk(file_dir)
for files in os.walk(file_dir):
    files_ = files[2]
    for file in files_:
        find = file.find(log_prefix)
        file_find = file.find("log")
        if (file.find("hadoop") >= 0 and file.find(log_prefix) >= 0 and file.find("log") >= 0 and
                file.find("zkfc") < 0 and file.find(exclude) < 0):
            log_files.append(file)

print("log_files>>>>>>>" + str(log_files))
if len(log_files) == 0:
    print("log_files is empty , will exist")
    sys.exit(0)

# log_files.sort()
log_files_read = []
log_files_read_num = len(log_files)

if len(log_files) > 3:
    index = 0
    while index < 3:
        suffix = "log." + str(index)
        if index == 0:
            suffix = "log"
        for the_file in log_files:
            if the_file.endswith(suffix):
                log_files_read.append(the_file)
                break
        index = index + 1
else:
    log_files_read = log_files

# print("log_files_read>>>>>>>" + str(log_files_read))
unselect = list(set(levels) - set(select))

select_regex = "(\[" + "\]|\[".join(select) + "\])"
unselect_regex = "(\[" + "\]|\[".join(unselect) + "\])"
# print(select_regex, file=sys.stderr)
# print(unselect_regex, file=sys.stderr)

select_pattern = re.compile(select_regex)
unselect_pattern = re.compile(unselect_regex)

# write tempfile
temp_file = os.path.join(tempfile.gettempdir(), "filter_log_temp.log")
with open(temp_file, "w") as tempf:
    output = False
    for log_file in log_files_read:
        log_file_path = file_dir + "/" + log_file
        if not os.path.exists(log_file_path):
            continue
        print("read file: {}".format(log_file_path), file=sys.stderr)
        with open(log_file_path) as logf:
            while True:
                line = logf.readline()
                if not line:
                    break

                if is_exception:
                    if line.find("Exception") < 0:
                        continue
                else:
                    # print("----write tempfile line---->" + line)
                    if not start_time is None:
                        if line.find("T") == 11:
                            line_time = line[1:20]
                            # print("----line_time---->" + line_time)
                            line_dt = datetime.datetime.strptime(line_time, "%Y-%m-%dT%H:%M:%S")
                            start_dt = datetime.datetime.strptime(start_time + " 00:00:00", '%Y%m%d %H:%M:%S')
                            if not end_time:
                                if line_dt < start_dt:
                                    continue
                            else:
                                end_dt = datetime.datetime.strptime(end_time + " 00:00:00", '%Y%m%d %H:%M:%S')
                                if line_dt < start_dt or line_dt > end_dt:
                                    continue

                    un_res = unselect_pattern.search(line)
                    if un_res:
                        output = False
                        continue

                if is_exception:
                    tempf.write(line)
                else:
                    res = select_pattern.search(line)
                    if res:
                        output = True
                    if output:
                        tempf.write(line)
# classify log

replaced_temp_file = os.path.join(tempfile.gettempdir(), "filter_log_replaced_temp.log")
print(replaced_temp_file, file=sys.stderr)

if not is_exception:
    with open(temp_file) as tempf, open(replaced_temp_file, "w") as replacedf:
        while True:
            line = tempf.readline()
            if not line:
                break

            res = classify_pattern.search(line)
            match_str = None
            if res:
                if remove_pattern.search(res.group(2)):
                    continue

                match_str = "{} {}".format(res.group(1), res.group(2))

                replaced_str = replace_pattern.sub('??', match_str)
                print(replaced_str, file=replacedf)

# append to filter log file
import socket
from datetime import datetime

filename = "filter_log_{}_{}_{}_{}.log".format(socket.gethostname(), datetime.now().strftime("%Y%m%d%H%M%S"),
                                               role.upper(), '-'.join(select))
result_file = os.path.join(tempfile.gettempdir(), filename)
print(result_file, file=sys.stderr)
if not only_classify_result:
    shutil.copy2(temp_file, result_file)
    if not is_exception:
        append_to(result_file, "\n\n===BEGIN\n\n")
else:
    # truncate result_file
    open(result_file, 'w').close()
    if not is_exception:
        append_to(result_file, "===BEGIN\n\n")

if not is_exception:
    os.system("sort {} | uniq -c | sort -n >> {}".format(replaced_temp_file, result_file))
    append_to(result_file, "\n===END")

os.remove(temp_file)
if os.path.exists(replaced_temp_file):
    os.remove(replaced_temp_file)

print("write to {}".format(result_file), file=sys.stderr)
