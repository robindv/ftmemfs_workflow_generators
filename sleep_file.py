#!/usr/bin/python2

import sys
from DAX3 import *


if len(sys.argv) != 2:
    print "Give parameter: ./sleep_file.py <num_secs>"
    sys.exit(0)

num_secs = int(sys.argv[1])

print "Creating workflow of {} seconds".format(num_secs)

dag =  ADAG("sleep-{}".format(num_secs))

for i in range(1, num_secs + 1):
    j = Job(id="ID{:08d}".format(i), name="sleep")
    j.addArguments("100")



    out = File("sleep_out_{}".format(i))
    j.uses(out, link=Link.OUTPUT, transfer=True)
    j.setStdout(out)

    dag.addJob(j)

f = open("/home/rdevries/workflows/sleep_file_{}.dax".format(num_secs),"w")
dag.writeXML(f)
f.close()
