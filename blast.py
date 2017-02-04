#!/usr/bin/python
import sys
from DAX3 import *

if len(sys.argv) != 2:
    print "Give parameter: ./blast.py <num_queries>"
    sys.exit(0)

num = int(sys.argv[1])

n = 1
dependencies = {}
query_files = {}
num_db = 512

dag = ADAG("blast-{}-{}".format(num_db,num))

# query file


for i in range(1, num + 1):

    filename = "query_big_{}.fastaa".format(i)
    queryfile = File(filename)
    queryjob = Job(id="ID{:08d}".format(n), name="cp")
    queryjob.addArguments("/var/scratch/aua400/blast_scripts/query_big.fastaa", queryfile)
    queryjob.uses(queryfile, link=Link.OUTPUT, transfer=True)
    n = n+1
    dag.addJob(queryjob)

    dependencies[filename] = queryjob
    query_files[i] = queryfile


# Copy all the files
for i in range(1, num_db + 1):

    filename = "frag{:03d}".format(i)

    target = File(filename)
    copyjob = Job(id="ID{:08d}".format(n), name="cp")
    n = n+1

    infile = "/var/scratch/aua400/blast_big/{}".format(filename)

    copyjob.addArguments(infile, target)
    copyjob.uses(target, link=Link.OUTPUT, transfer=True)
    dependencies[filename] = copyjob
    dag.addJob(copyjob)

for i in range(1, num_db + 1):

    filename = "frag{:03d}".format(i)

    infile = File(filename)
    formatdb = Job(id="ID{:08d}".format(n), name="formatdb")
    n = n+1

    formatdb.addArguments("-i", infile, "-p F -o T -l log_{}.log".format(filename))

    formatdb.uses(infile, link=Link.INPUT)


    for ext in ['.nhr','.nin','.nnd','.nni','.nsd','.nsq']:
        extfile = File(filename + ext)
        dependencies[filename + ext] = formatdb
        formatdb.uses(extfile, link=Link.OUTPUT)

    dag.addJob(formatdb)
    dag.depends(child=formatdb, parent=dependencies[filename])

for i in range(1, num_db + 1):

    filename = "frag{:03d}".format(i)

    for it in range(1, num + 1):
        # blastall -p blastn -i ${QUERY_FILE} -d $DIR/$file -o $DIR/${file}_${i}_output.txt

        blastall = Job(id="ID{:08d}".format(n), name="blastall")
        n = n+1

        dag.addJob(blastall)

        blastall.addArguments("-p blastn -i",query_files[it],"-d {} -o {}_{}_output.txt".format(filename,filename,it))
        dag.depends(child=blastall, parent= dependencies["query_big_{}.fastaa".format(it)] )

        blastall.uses(query_files[it], link=Link.INPUT)
        for ext in ['.nhr','.nin','.nnd','.nni','.nsd','.nsq']:
            extfile = File(filename + ext)
            blastall.uses(extfile, link=Link.INPUT)
        dag.depends(child=blastall, parent=dependencies[filename + ext])
        outfile = File("{}_{}_output.txt".format(filename,it))
        blastall.uses(outfile, link=Link.OUTPUT)


f = open("/home/rdevries/workflows/blast_{}_{}.dax".format(num_db,num),"w")
dag.writeXML(f)
f.close()
