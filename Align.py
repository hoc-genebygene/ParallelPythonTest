from celery import group
from tasks import align_paired_reads, sort_bams, postprocess, variant_call

input_fastqs = [("U0a_CGATGT_L001_R1_001.fastq", "U0a_CGATGT_L001_R2_001.fastq"), ("U0a_CGATGT_L001_R1_002.fastq", "U0a_CGATGT_L001_R2_002.fastq"), ("U0a_CGATGT_L001_R1_003.fastq", "U0a_CGATGT_L001_R2_003.fastq"),("U0a_CGATGT_L001_R1_004.fastq", "U0a_CGATGT_L001_R2_004.fastq")]

aligned_bam = "U0a_CGATGT.bam"
sorted_bam = "U0a_CGATGT.sorted.bam"
postprocessed_bam = "U0a_CGATGT.sorted.pp.bam"
vcf_file = "U0a_CGATGT.vcf"

# do alignment
aligned_bams = []
alignment_jobs = []
for input_fastq_1, input_fastq_2 in input_fastqs:
	out_bam = input_fastq_1 + input_fastq_2

	alignment_jobs.append(align_paired_reads.s(input_fastq_1, input_fastq_2, out_bam))
	aligned_bams.append(out_bam)

group(alignment_jobs)
result = alignment_jobs.apply_async()
result.join()

# do merge and sort
#sort_bams.delay(aligned_bams, sorted_bam)

#postprocess.delay(sorted_bam, postprocessed_bam)

#variant_call.delay(postprocessed_bam, vcf_file)
