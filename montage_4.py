#!/usr/bin/python2

from DAX3 import *
import xml.etree.ElementTree as ET
import os

STAGE1_INPUT_PATH =  "input/"
STAGE1_OUTPUT_PATH = ""
STAGE2_OUTPUT_PATH = "diffdir/"
#STAGE5_OUTPUT_PATH = "/"

class Montage(object):

    def __init__(self, filename):
        self.montage = ADAG("montage-4")
        self.n = 1
        self.input_workflow = ET.parse(filename).getroot()
        self.dependencies = {}

        self.mkdirs_job = Job(id="ID{:04d}".format(self.n), name="mkdir")
        self.mkdirs_job.addArguments("-p","input","projdir","diffdir","statdir")
        self.montage.addJob(self.mkdirs_job)

        self.copy()
        self.mProjectPP()
        self.mDiffFit()
        self.mConcatFit()
        self.mBgModel()
        self.mBackground()
        self.mFinish()

    def copy(self):
        '''Copy all the dependencies into the filesystem'''
        for filename in ["big_region_20140729_115511_5629.hdr",
                         "statfile_20140729_115511_5629.tbl",
                         "pimages_20140729_115511_5629.tbl",
                         "cimages_0_0_20140729_115511_5629.tbl",
                         "cimages_0_1_20140729_115511_5629.tbl",
                         "cimages_1_0_20140729_115511_5629.tbl",
                         "cimages_1_1_20140729_115511_5629.tbl",
                         "slist_20140729_115511_5629.tbl",
                         "region_0_0_20140729_115511_5629.hdr",
                         "region_1_0_20140729_115511_5629.hdr",
                         "region_0_1_20140729_115511_5629.hdr",
                         "region_1_1_20140729_115511_5629.hdr",
                         "shrunken_20140729_115511_5629.hdr"]:
            self.n += 1
            target = File(filename)
            copyjob = Job(id="ID{:04d}".format(self.n), name="cp")

            infile = "/var/scratch/aua400/montage_4/{}".format(filename)

            if not os.path.isfile(infile):
                infile = infile.replace("_20140729_115511_5629","")

            copyjob.addArguments(infile, target)
            copyjob.uses(target, link=Link.OUTPUT, transfer=True)
            self.dependencies[filename] = copyjob
            self.montage.addJob(copyjob)

        for job in self.input_workflow.getchildren():
            # Filter only jobs
            if "name" not in job.attrib:
                continue

            if job.attrib["name"] != "mProjectPP":
                continue

            filename = job.getchildren()[1].attrib["file"]

            self.n += 1
            target = File(STAGE1_INPUT_PATH + filename)
            copyjob = Job(id="ID{:04d}".format(self.n), name="cp")
            copyjob.addArguments("/var/scratch/aua400/montage_4/{}".format(filename), target)
            copyjob.uses(target, link=Link.OUTPUT, transfer=True)
            self.dependencies[target.name] = copyjob
            self.montage.addJob(copyjob)
            self.montage.depends(parent=self.mkdirs_job, child=copyjob)

    def mProjectPP(self):
        for job in self.input_workflow.getchildren():
            # Filter only jobs
            if "name" not in job.attrib:
                continue

            if job.attrib["name"] != "mProjectPP":
                continue

            self.n += 1
            mp = Job(id="ID{:04d}".format(self.n), name="mProjectPP")

            arg = [elem for elem in job.getchildren()[0].iter()][0].text.strip().split('\n')[1].strip()
            tmass = File(job.getchildren()[1].attrib["file"])
            p2mass = File(STAGE1_OUTPUT_PATH + job.getchildren()[2].attrib["file"])
            p2mass_area = File(STAGE1_OUTPUT_PATH + job.getchildren()[3].attrib["file"])
            bregion = File(job.getchildren()[4].attrib["file"])

            mp.addArguments("-X", arg, tmass, p2mass, bregion)

            self.montage.addJob(mp)

            self.add_input_file(mp, tmass)
            self.add_output_file(mp, p2mass)
            self.add_output_file(mp, p2mass_area)
            self.add_input_file(mp, bregion)

    def mDiffFit(self):
        for job in self.input_workflow.getchildren():
            # Filter only jobs
            if "name" not in job.attrib:
                continue

            if job.attrib["name"] != "mDiffFit":
                continue

            self.n += 1
            md = Job(id="ID{:04d}".format(self.n), name="mDiffFit")

            fit = File(STAGE2_OUTPUT_PATH + job.getchildren()[3].attrib["file"])
            p2mass1 = File(STAGE1_OUTPUT_PATH + job.getchildren()[4].attrib["file"])
            p2mass1a = File(STAGE1_OUTPUT_PATH + job.getchildren()[5].attrib["file"])
            p2mass2 = File(STAGE1_OUTPUT_PATH + job.getchildren()[6].attrib["file"])
            p2mass2a = File(STAGE1_OUTPUT_PATH + job.getchildren()[7].attrib["file"])
            diff = File(STAGE2_OUTPUT_PATH + job.getchildren()[8].attrib["file"])
            bregion = File(job.getchildren()[9].attrib["file"])

            self.montage.addJob(md)

            md.addArguments("-s",fit, p2mass1, p2mass2, diff, bregion)

            self.add_output_file(md, fit)
            self.add_input_file(md, p2mass1)
            self.add_input_file(md, p2mass1a)
            self.add_input_file(md, p2mass2)
            self.add_input_file(md, p2mass2a)
            self.add_output_file(md, diff)
            self.add_input_file(md, bregion)

    def mConcatFit(self):
        for job in self.input_workflow.getchildren():
            # Filter only jobs
            if "name" not in job.attrib:
                continue

            if job.attrib["name"] != "mConcatFit":
                continue

            self.n += 1
            mc = Job(id="ID{:04d}".format(self.n), name="mConcatFit")

            statfile = File(job.getchildren()[1].attrib["file"])
            fits_tbl = File(job.getchildren()[2].attrib["file"])

            self.montage.addJob(mc)

            mc.addArguments(statfile,fits_tbl,STAGE2_OUTPUT_PATH)

            self.add_input_file(mc, statfile)
            self.add_output_file(mc, fits_tbl)

            for i in range(3, len(job.getchildren())):
                fit_file = File(STAGE2_OUTPUT_PATH + job.getchildren()[i].attrib["file"])
                self.add_input_file(mc, fit_file)

    def mBgModel(self):
        for job in self.input_workflow.getchildren():
            # Filter only jobs
            if "name" not in job.attrib:
                continue

            if job.attrib["name"] != "mBgModel":
                continue

            self.n += 1
            mb = Job(id="ID{:04d}".format(self.n), name="mBgModel")


            arg = [elem for elem in job.getchildren()[0].iter()][0].text.strip()

            pimages = File(job.getchildren()[1].attrib["file"])
            fits = File(job.getchildren()[2].attrib["file"])
            corrections = File(job.getchildren()[3].attrib["file"])

            self.montage.addJob(mb)

            mb.addArguments(arg, pimages, fits, corrections)

            self.add_input_file(mb, pimages)
            self.add_input_file(mb, fits)
            self.add_output_file(mb, corrections)


    def mBackground(self):
        for job in self.input_workflow.getchildren():
            # Filter only jobs
            if "name" not in job.attrib:
                continue

            if job.attrib["name"] != "mBackground":
                continue

            self.n += 1
            mb = Job(id="ID{:04d}".format(self.n), name="mBackground")

            p2mass = File(STAGE1_OUTPUT_PATH + job.getchildren()[1].attrib["file"])
            p2mass_a = File(STAGE1_OUTPUT_PATH + job.getchildren()[2].attrib["file"])
            pimages = File(job.getchildren()[3].attrib["file"])
            corrections = File(job.getchildren()[4].attrib["file"])
            c2mass = File(job.getchildren()[5].attrib["file"])
            c2mass_a = File(job.getchildren()[6].attrib["file"])

            mb.addArguments("-t", p2mass, c2mass, pimages, corrections)

            self.montage.addJob(mb)
            self.add_input_file(mb, p2mass)
            self.add_input_file(mb, p2mass_a)
            self.add_input_file(mb, pimages)
            self.add_input_file(mb, corrections)
            self.add_output_file(mb, c2mass)
            self.add_output_file(mb, c2mass_a)

    def mFinish(self):

        shrunk = False

        for job in self.input_workflow.getchildren():
            # Filter only jobs
            if "name" not in job.attrib:
                continue

            if job.attrib["name"] not in ["mImgtbl","mAdd","mShrink","mJPEG"]:
                continue

            self.n += 1
            mf = Job(id="ID{:04d}".format(self.n), name=job.attrib["name"])
            self.montage.addJob(mf)

            if job.attrib["name"] == "mImgtbl":

                cimages = File(job.getchildren()[1].attrib["file"])
                newcimages = File(job.getchildren()[2].attrib["file"])

                mf.addArguments(".","-t", cimages, newcimages)

                self.add_input_file(mf, cimages)
                self.add_output_file(mf, newcimages)

                for i in range(3, len(job.getchildren())):
                    c2mass = File(job.getchildren()[i].attrib["file"])
                    self.add_input_file(mf, c2mass)

            elif job.attrib["name"] == "mAdd":

                if not shrunk:

                    tbl = File(job.getchildren()[1].attrib["file"])
                    hdr = File(job.getchildren()[2].attrib["file"])
                    fits = File(job.getchildren()[3].attrib["file"])
                    fits_a = File(job.getchildren()[4].attrib["file"])

                    mf.addArguments("-e", tbl, hdr, fits)

                    self.add_input_file(mf, tbl)
                    self.add_input_file(mf, hdr)
                    self.add_output_file(mf, fits)
                    self.add_output_file(mf, fits_a)

                    for i in range(5, len(job.getchildren())):
                        c2mass = File(job.getchildren()[i].attrib["file"])
                        self.add_input_file(mf, c2mass)

                else:

                    tbl = File(job.getchildren()[1].attrib["file"])
                    hdr = File(job.getchildren()[2].attrib["file"])
                    fits = File(job.getchildren()[3].attrib["file"])

                    mf.addArguments("-n -e", tbl, hdr, fits)

                    self.add_input_file(mf, tbl)
                    self.add_input_file(mf, hdr)
                    self.add_output_file(mf, fits)

                    for i in range(4, len(job.getchildren())):
                        sh = File(job.getchildren()[i].attrib["file"])
                        self.add_input_file(mf, sh)


            elif job.attrib["name"] == "mShrink":

                tile = File(job.getchildren()[1].attrib["file"])
                sh = File(job.getchildren()[2].attrib["file"])

                self.add_input_file(mf, tile)
                self.add_output_file(mf, sh)

                mf.addArguments(tile, sh, "11")

                shrunk = True

            else:

                fin = File(job.getchildren()[1].attrib["file"])
                fout = File(job.getchildren()[2].attrib["file"])

                self.add_input_file(mf, fin)
                self.add_output_file(mf, fout)

                mf.addArguments("-ct 1","-gray", fin, "min max gaussianlog", "-out",fout)

    def add_input_file(self, job, f):
        if f.name[:6] == "2mass-":
            f.name = STAGE1_INPUT_PATH + f.name

        job.uses(f, link=Link.INPUT)
        try:
            self.montage.depends(parent=self.dependencies[f.name], child=job)
        except DuplicateError:
            pass

    def add_output_file(self, job, f):

        job.uses(f, link=Link.OUTPUT)
        self.dependencies[f.name] = job

    def toXML(self):
        return self.montage.toXML()

    def toFile(self, filename):
        f = open(filename,"w")
        self.montage.writeXML(f)
        f.close()


montage = Montage("/var/scratch/aua400/montage_4/dag.xml")
montage.toFile("/home/rdevries/workflows/montage_4.dax")
