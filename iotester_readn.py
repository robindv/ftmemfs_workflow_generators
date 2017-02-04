#!/usr/bin/python2

from DAX3 import *

t = "readn"

name1 = "iotester_{}_{}M".format(t, 1)
name10 = "iotester_{}_{}M".format(t, 10)
name100 = "iotester_{}_{}M".format(t, 100)

dag1 =  ADAG(name1)
dag10 =  ADAG(name10)
dag100 =  ADAG(name100)

prepare_dag = ADAG("prepare_{}".format(t))

for i in range(1, 16384 + 1):

    if i <= 4096:
        j = Job(id="ID{:08d}".format(i), name="iotester")
        j.addArguments("read {} {}".format(name100, 100))
        dag100.addJob(j)

        j = Job(id="ID{:08d}".format(i), name="iotester")
        j.addArguments("read {} {}".format(name100, 10))
        dag10.addJob(j)

        j = Job(id="ID{:08d}".format(i), name="iotester")
        j.addArguments("read {} {}".format(name100, 1))
        dag1.addJob(j)


    elif i <= 8192:
        j = Job(id="ID{:08d}".format(i), name="iotester")
        j.addArguments("read {} {}".format(name10, 10))
        dag10.addJob(j)

        j = Job(id="ID{:08d}".format(i), name="iotester")
        j.addArguments("read {} {}".format(name10, 1))
        dag1.addJob(j)

    else:
        j = Job(id="ID{:08d}".format(i), name="iotester")
        j.addArguments("read {} {}".format(name1, 1))
        dag1.addJob(j)


p = Job(id="ID{:08d}".format(1), name="iotester")
p.addArguments("append {} {}".format(name1, 1))
prepare_dag.addJob(p)

p = Job(id="ID{:08d}".format(2), name="iotester")
p.addArguments("append {} {}".format(name10, 10))
prepare_dag.addJob(p)

p = Job(id="ID{:08d}".format(3), name="iotester")
p.addArguments("append {} {}".format(name100, 100))
prepare_dag.addJob(p)





f = open("/home/rdevries/workflows/{}.dax".format(name1),"w")
dag1.writeXML(f)
f.close()
f = open("/home/rdevries/workflows/{}.dax".format(name10),"w")
dag10.writeXML(f)
f.close()
f = open("/home/rdevries/workflows/{}.dax".format(name100),"w")
dag100.writeXML(f)
f.close()
f = open("/home/rdevries/workflows/prepare_{}.dax".format(t),"w")
prepare_dag.writeXML(f)
f.close()
