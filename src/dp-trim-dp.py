# coding:utf-8

readDir = "./10k_hope-nn"
# readDir = "./10k_hope-jn"
# readDir = "./log_dn"
writeDir = "./10_hope-nn_warn.log"
# writeDir = "./10_hope-jn_warn.log"
# writeDir = "./log_dn_error_dp.log"
outfile = open(writeDir, "w")
f = open(readDir, "r")

lines_seen = set()  # Build an unordered collection of unique elements.

for line in f:
    line = line.strip('\n')
    # if line not in lines_seen and line.find("ERROR") >= 0:
    if line[19:40] not in lines_seen and line.find("WARN") >= 0:
        line_ = line[12:]
        s = line[19:43]

        outfile.write(line_ + '\n')
        lines_seen.add(line[19:40])
