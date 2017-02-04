from DAX3 import *

dag =  ADAG("zeros-800k")
m = Job(id="ID{:04d}".format(1), name="mkdir")
m.addArguments("-p","zeros-800k")

dag.addJob(m)

for i in range(1, 10000 + 1):

    j = Job(id="ID{:04d}".format(i + 1), name="dd")

    f = File("zeros-{}".format(i+1))

    j.addArguments("if=/dev/zero","of=zeros-800k/zeros-{}".format(i+1),"bs=100k","count=8")
    j.uses(f, link=Link.OUTPUT)

    dag.addJob(j)
    dag.depends(parent=m, child=j)

f = open("/home/rdevries/workflows/zeros_800k.dax","w")
dag.writeXML(f)
f.close()
