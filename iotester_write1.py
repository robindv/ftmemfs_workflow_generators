#!/usr/bin/python2

from DAX3 import *

t = "write1"

name1 = "iotester_{}_{}M".format(t, 1)
name10 = "iotester_{}_{}M".format(t, 10)
name100 = "iotester_{}_{}M".format(t, 100)

pname1 = "iotester_{}p_{}M".format(t, 1)
pname10 = "iotester_{}p_{}M".format(t, 10)
pname100 = "iotester_{}p_{}M".format(t, 100)

dag1 =  ADAG(name1)
dag10 =  ADAG(name10)
dag100 =  ADAG(name100)

pdag1 =  ADAG(pname1)
pdag10 =  ADAG(pname10)
pdag100 =  ADAG(pname100)

clear1 = ADAG("clear_write1_1M")
clear10 = ADAG("clear_write1_10M")
clear100 = ADAG("clear_write1_100M")

touch1 = ADAG("touch_write1_1M")
touch10 = ADAG("touch_write1_10M")
touch100 = ADAG("touch_write1_100M")


for i in range(1, 16384 + 1):

    if i <= 2048:
        j = Job(id="ID{:08d}".format(i), name="iotester")
        j.addArguments("append {}_{} {}".format(name100, i, 100))
        dag100.addJob(j)
        pdag100.addJob(j)

        j = Job(id="ID{:08d}".format(i), name="rm")
        j.addArguments("{}_{}".format(name100, i))
        clear100.addJob(j)

        j = Job(id="ID{:08d}".format(i), name="touch")
        j.addArguments("{}_{}".format(name100, i))
        touch100.addJob(j)

    if i <= 8192:
        j = Job(id="ID{:08d}".format(i), name="iotester")
        j.addArguments("append {}_{} {}".format(name10, i, 10))
        dag10.addJob(j)
        pdag10.addJob(j)

        j = Job(id="ID{:08d}".format(i), name="rm")
        j.addArguments("{}_{}".format(name10, i))
        clear10.addJob(j)

        j = Job(id="ID{:08d}".format(i), name="touch")
        j.addArguments("{}_{}".format(name10, i))
        touch10.addJob(j)

    j = Job(id="ID{:08d}".format(i), name="iotester")
    j.addArguments("append {}_{} {}".format(name1, i, 1))
    dag1.addJob(j)
    pdag1.addJob(j)

    j = Job(id="ID{:08d}".format(i), name="rm")
    j.addArguments("{}_{}".format(name1, i))
    clear1.addJob(j)

    j = Job(id="ID{:08d}".format(i), name="touch")
    j.addArguments("{}_{}".format(name1, i))
    touch1.addJob(j)


f = open("/home/rdevries/workflows/{}.dax".format(name1),"w")
dag1.writeXML(f)
f.close()
f = open("/home/rdevries/workflows/{}.dax".format(name10),"w")
dag10.writeXML(f)
f.close()
f = open("/home/rdevries/workflows/{}.dax".format(name100),"w")
dag100.writeXML(f)
f.close()

f = open("/home/rdevries/workflows/{}.dax".format(pname1),"w")
pdag1.writeXML(f)
f.close()
f = open("/home/rdevries/workflows/{}.dax".format(pname10),"w")
pdag10.writeXML(f)
f.close()
f = open("/home/rdevries/workflows/{}.dax".format(pname100),"w")
pdag100.writeXML(f)
f.close()

f = open("/home/rdevries/workflows/clear_write1_1M.dax","w")
clear1.writeXML(f)
f.close()
f = open("/home/rdevries/workflows/clear_write1_10M.dax","w")
clear10.writeXML(f)
f.close()
f = open("/home/rdevries/workflows/clear_write1_100M.dax","w")
clear100.writeXML(f)
f.close()

f = open("/home/rdevries/workflows/touch_write1_1M.dax","w")
touch1.writeXML(f)
f.close()
f = open("/home/rdevries/workflows/touch_write1_10M.dax","w")
touch10.writeXML(f)
f.close()
f = open("/home/rdevries/workflows/touch_write1_100M.dax","w")
touch100.writeXML(f)
f.close()
# f = open("/home/rdevries/workflows/prepare_{}.dax".format(t),"w")
# prepare_dag.writeXML(f)
# f.close()
