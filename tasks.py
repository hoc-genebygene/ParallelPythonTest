import subprocess
import os

from celery import Celery

app = Celery('tasks', backend='rpc://', broker='pyamqp://guest@Ubuntu-1.local//')

WORKER_REFERENCE_DIR="/tank/genome_data/references/"  # This should be a directory on the worker's local drive (to avoid network I/O) where we keep the references

TMP_DIR="/tank/test/tmp/"
BIN_DIR="/tank/test/bin/" # contains aengine executable

AENGINE_EXECUTABLE = "TMPDIR=" + TMP_DIR + " " + BIN_DIR + "aengine"

FASTQ_DIR = "/tank/test/fastq/"
ALIGNED_BAMS_DIR = "/tank/test/aligned_bams/"
SORTED_BAMS_DIR = "/tank/test/sorted_bams/"
PP_BAMS_DIR = "/tank/test/pp_bams/"
VCF_DIR = "/tank/test/vcf/"

WORKER_REFERENCE_PATH = WORKER_REFERENCE_DIR + "ucsc.hg19/ucsc.hg19.fasta"

def run_command_on_worker(command):
    subp = subprocess.Popen(command, stdout=subprocess.PIPE, stdin=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    subp.wait()

    return subp.returncode

@app.task
def align_paired_reads(fastq1, fastq2, out_aligned_bam):
    WORKER_fastq1_path = FASTQ_DIR + fastq1
    WORKER_fastq2_path = FASTQ_DIR + fastq2
    WORKER_output_path = ALIGNED_BAMS_DIR + out_aligned_bam

    # Run the command aengine align <reference> <fastq1> <fastq2> -f <output_bam>
    command = AENGINE_EXECUTABLE + " align " + WORKER_REFERENCE_PATH + " " + WORKER_fastq1_path + " " + WORKER_fastq2_path + " -f " + WORKER_output_path;

    return run_command_on_worker(command)

@app.task
def sort_bams(in_bam_files, out_merged_sorted_bam_file):
    for bam_file in in_bam_files:
        bam_files_path_string = bam_files_path_string + ALIGNED_BAMS_DIR + bam_file + " "

    WORKER_output_path = SORTED_BAMS_DIR + out_merged_sorted_bam_file

    command = AENGINE_EXECUTABLE + " sort " + bam_files_path_string + "-f " + WORKER_output_path

    return run_command_on_worker(command)

@app.task
def postprocess(in_bam_file, out_postprocessed_bam_file):
    WORKER_in_bam_file_path = SORTED_BAMS_DIR + in_bam_file
    WORKER_output_path = PP_BAMS_DIR + out_postprocessed_bam_file

    command = AENGINE_EXECUTABLE + " postprocess " + WORKER_REFERENCE_PATH + " " + WORKER_in_bam_file_path + " -f " + WORKER_output_path

    return run_command_on_worker(command)

@app.task
def variant_call(in_bam_file, out_vcf_file):
    WORKER_in_bam_file_path = PP_BAMS_DIR + in_bam_file
    WORKER_output_path = VCF_DIR + out_vcf_file

    command = AENGINE_EXECUTABLE + " variants " + WORKER_REFERENCE_PATH + " " + in_bam_file + " -f " + WORKER_output_path

    return run_command_on_worker(command)