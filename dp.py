# coding:utf-8

# readDir = "./10k_hope-nn"
readDir = "./log_dn"
# readDir = "./10k_hope-jn"
# writeDir = "./10_hope-nn_warn.log"
writeDir = "./log_dn_warn_dp.log"
# writeDir = "./10_hope-jn_error.log"
outfile = open(writeDir, "w")
f = open(readDir, "r")

lines_seen = set()  # Build an unordered collection of unique elements.

for line in f:
    line = line.strip('\n')
    if line not in lines_seen and line.find("WARN") >= 0:
        # if line not in lines_seen and line.find("WARN") >= 0:
        outfile.write(line + '\n')
        lines_seen.add(line)
