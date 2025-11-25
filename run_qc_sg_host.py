#!/usr/bin/python

import argparse
import glob
import inflect
import os

from rich.console import Group
from rich.live import Live
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn
from rich.traceback import install

import common
from get_info import names_list, make_dict
# Using logger from common.py
logger = common.logger
p = inflect.engine()

# Rich traceback handler
install(show_locals=True)

# Using rich elements from common.py
console = common.console
panel = common.Panel

# Initiate Progress() object for n_studies
progress = Progress(
    SpinnerColumn(spinner_name='dots12', style='blue'),
    TextColumn("[progress.description]{task.description}"),
    BarColumn(),
    TextColumn("{task.completed}/{task.total} completed in"),
    TimeElapsedColumn(),
    transient=False,
)

# Run fastqc and multiqc
def generate_qc_reports(in_dir, qc_out, mqc_out, status_sub):
    status_sub.update(f"[i][dim]Generating QC reports for[/dim] [blue]{study}[/blue] {common.EMOJI_PLAY} [magenta]{os.path.basename(in_dir)}[/magenta] [dim]: ({studies.index(study)+1}/{len(studies)}) [/dim][/i] \n", spinner='point', spinner_style='magenta')

    logger.info(f"{common.EMOJI_SPARKLE} Running FastQC on [blue]{study}[/blue] {common.EMOJI_PLAY} [magenta]{in_dir}[/magenta]...")

    if len(os.listdir(qc_out)) == len(os.listdir(raw_reads))*2:
        logger.info(f"{common.EMOJI_CHECK} FastQC reports already generated for [green]{study}[/green]. Skipping...")
    else:
        logger.info(f"{common.EMOJI_PROCESS} Generating QC reports for [blue]{study}[/blue]...")
        # Doesn't work with subprocess because of wildcard
        common.run_command(
            f"mamba run -n {utility_paths['FastQC']} fastqc {in_dir}/*.gz -o {qc_out} -t {threads*split_size}",
            desc=f"Generating FastQC reports for {study}"
        )

    logger.info(f"{common.EMOJI_SPARKLE} Running MultiQC on [blue]{study}[/blue] {common.EMOJI_PLAY} [magenta]{qc_out}[/magenta] reports...")
    if len(os.listdir(mqc_out)) > 0:
        logger.info(f"{common.EMOJI_CHECK} MultiQC report already generated for [green]{study}[/green]. Skipping...")
    else:
        logger.info(f"{common.EMOJI_PROCESS} Generating cumulative report for [blue]{study}[/blue]...")
        common.run_command(
            f"mamba run -n {utility_paths['MultiQC']} multiqc {qc_out} -o {mqc_out} --interactive",
            desc=f"Generating cumulative reports for {study}"
        )

    console.rule(f"[dim i]{common.EMOJI_CHECK} Generated QC reports for [blue]{study}[/blue][/dim i]", characters="-", style='dim')


# Run BBDuk and fastp - This is only for Paired ends. Add logic for Single-ends
def run_qc(sub_list, status_sub):
    for sample in sub_list:
        status_sub.update(f"[i][dim]Filtering reads of [/dim] [blue]{study}[/blue] {common.EMOJI_PLAY} [magenta]{sample}[/magenta][dim]: ({sub_list.index(sample)+1}/{len(sub_list)}) | ({studies.index(study)+1}/{len(studies)}) [/dim][/i] \n", spinner='point', spinner_style='magenta')
        # BBDuk
        if any(file in os.listdir(bb_out) for file in [f"{sample}_R2.fq.gz", f"{sample}.fq.gz"]):
            logger.info(f"{common.EMOJI_CHECK} BBDuk already processed [blue]{study}[/blue] {common.EMOJI_PLAY} [magenta]{sample}[/magenta]. Skipping...")
        else:
            logger.info(f"{common.EMOJI_PROCESS} Processing [blue]{study}[/blue] {common.EMOJI_PLAY} [magenta]{sample}[/magenta] with BBDuk...")
            reads = glob.glob(f"{raw_reads}/{sample}*")
            for read in reads:
                if "_1." in read:
                    read1 = read
                    # Output: {ds}/raw_reads/{sample}*R1*
                else:
                    read2 = read
            common.run_command(
                f"mamba run -n {utility_paths['BBDuk']} bbduk.sh in1={read1} in2={read2} out1={bb_out}/{sample}_R1.fq.gz out2={bb_out}/{sample}_R2.fq.gz ref={utility_paths['bb_adapters']} k=19 mink=7 ktrim=r trimq=20 qtrim=r hdist=1 tpe tbo threads={threads} 2> {bb_out}/{sample}.log",
                desc=f"Trimming {sample} reads"
            )
        
        os.system(f"grep Result {bb_out}/{sample}.log >> {base}/{study}/bb_out_count.txt")
        
        # fastp
        status_sub.update(f"[i][dim]Deduplicating reads of [/dim] [blue]{study}[/blue] {common.EMOJI_PLAY} [magenta]{sample}[/magenta][dim]: ({sub_list.index(sample)+1}/{len(sub_list)}) | ({studies.index(study)+1}/{len(studies)}) [/dim][/i] \n", spinner='point', spinner_style='magenta')
        if f"{sample}_R2.fq.gz" in os.listdir(fp_out):
            logger.info(f"{common.EMOJI_CHECK} fastp already processed [blue]{study}[/blue] {common.EMOJI_PLAY} [green]{sample}[/green]. Skipping...")
        else:
            logger.info(f"{common.EMOJI_PROCESS} Processing [blue]{study}[/blue] {common.EMOJI_PLAY} [magenta]{sample}[/magenta] with fastp...")
            common.run_command(
                f"mamba run -n {utility_paths['fastp']} fastp -i {bb_out}/{sample}_R1.fq.gz -o {fp_out}/{sample}_R1.fq.gz -I {bb_out}/{sample}_R2.fq.gz -O {fp_out}/{sample}_R2.fq.gz -D -A -h {fp_out}/{sample}.html -j {fp_out}/{sample}.json -w {threads}",
                desc=f"Performing deduplication of {sample} reads"
            )

        console.rule(f"[dim i]{common.EMOJI_CHECK} Generated HQ reads for [blue]{study}[/blue] {common.EMOJI_PLAY} [magenta]{sample}[/magenta][/dim i]", characters="-", style='dim')
        status_sub.update(f"[i][dim]Filtering completed: ({sub_list.index(sample)+1}/{len(sub_list)}) | ({studies.index(study)+1}/{len(studies)}) [/dim][/i] \n", spinner='point', spinner_style='magenta')

# Run hostile
def remove_host(sub_list, status_sub):
    for sample in sub_list:
        status_sub.update(f"[i][dim]Removing host reads from [/dim] [blue]{study}[/blue] {common.EMOJI_PLAY} [magenta]{sample}[/magenta][dim]: ({sub_list.index(sample)+1}/{len(sub_list)}) | ({studies.index(study)+1}/{len(studies)}) [/dim][/i] \n", spinner='point', spinner_style='magenta')
        # hostile
        if f"{sample}_R2.clean_2.fastq.gz" in os.listdir(hostile_out):
            logger.info(f"{common.EMOJI_CHECK} Host reads already removed from [blue]{study}[/blue] {common.EMOJI_PLAY} [green]{sample}[/green]. Skipping...")
        else:
            logger.info(f"{common.EMOJI_PROCESS} Processing [blue]{study}[/blue] {common.EMOJI_PLAY} [magenta]{sample}[/magenta] with Hostile...")
            # Doesn't run with subprocess because of redirection
            common.run_command(
                f"mamba run -n {utility_paths['Hostile']} hostile clean --fastq1 {fp_out}/{sample}_R1.fq.gz --fastq2 {fp_out}/{sample}_R2.fq.gz --output {hostile_out} --index {utility_paths['Hostile_DB']} --threads {threads} > {hostile_out}/{sample}.log",
                desc=f"Removing host reads from {sample}"
            )
        
        console.rule(f"[dim i]{common.EMOJI_CHECK} Removed host reads from [blue]{study}[/blue] {common.EMOJI_PLAY} [magenta]{sample}[/magenta][/dim i]", characters="-", style='dim')
        status_sub.update(f"[i][dim]Removing host reads completed: ({sub_list.index(sample)+1}/{len(sub_list)}) | ({studies.index(study)+1}/{len(studies)}) [/dim][/i] \n", spinner='point', spinner_style='magenta')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Perform QC on all reads. Runs FastQC, MultiQC, BBDuk, fastp, and Hostile...")
    parser.add_argument("-b", "--base_dir", help="Base directory with all data. (Default: all_data)", default="all_data")
    parser.add_argument("-s", "--samples", help="List of sample IDs as text file. (Default: samples_list.txt)", default="samples_list.txt")
    parser.add_argument("-p", "--projects", help="List of project names as text file. (Default: studies_list.txt)", default="studies_list.txt")
    parser.add_argument("-u", "--utility_paths", help="Envs or Paths for tools and DBs as a CSV file. (Default: utility_paths.csv)", default="utility_paths.csv")
    parser.add_argument("-t", "--threads", type=int, default=1, help="Number of threads (Default: 1)")
    parser.add_argument("-l", "--split_size", type=int, choices=[1,2,3], nargs="?", default=1, help="Number of sub_lists to make, and run concurrently (Default: no splitting, everything as one list).")
    args=parser.parse_args()

    console.print(  
        panel(
            f"{common.EMOJI_SPARKLE} Performing QC...", 
            title=f"Study: {common.study_name.upper()}", 
            title_align="left", 
            border_style='dim bold yellow'
        ), 
        style='italic dim'
    )
    samples_in = args.samples
    utility_paths_in = args.utility_paths
    threads = args.threads
    split_size = args.split_size

    utility_paths = make_dict(utility_paths_in)
    console.print(
        panel.fit(
            f"[dim]{common.EMOJI_SPARKLE} Found[/] [bold]{len(utility_paths)}[/] [dim]utility {p.plural('path', len(utility_paths))} in[/] {utility_paths_in}", 
            title="Utility Paths", 
            border_style='dim cyan'
        ),
        style='italic'
    )

    if args.base_dir in [".", "./"]:
        base_dir = os.getcwd()
    else:
        base_dir = os.path.join(os.getcwd(), args.base_dir)

    if args.projects in os.listdir(base_dir):
        logger.info(f"{common.EMOJI_SPARKLE} Found project file [bold blue]{args.projects}[/] in [bold blue]{args.base_dir}[/]")
        base_dirs = [base_dir]
    else:
        logger.info(f"{common.EMOJI_PROCESS} [bold blue]{args.projects}[/] not in [bold blue]{args.base_dir}[/], searching subdirectories.")
        # It may work only for one sub-level
        base_dirs = [os.path.join(base_dir, d) for d in os.listdir(base_dir) if args.projects in os.listdir(os.path.join(base_dir, d))]

    # Main status
    status = console.status(f"[i][dim]Initiating QC on[/dim] {len(base_dirs)} [dim] study {p.plural('type', len(base_dirs))}[/dim][/i]")
    
    # Per-study statuses
    status_subs = []
    for i in range(split_size):
        status_subs.append(console.status(f"[i][dim]Running QC on thread {i+1}...[/dim][/i]"))

    with Live(Group(status, progress, *status_subs), console=console, transient=True):
        if base_dirs:
            for base in base_dirs:
                status.update(f"[i][dim]Performing QC on[/dim] [cyan]{os.path.basename(base)}[/cyan] [dim]datasets ({base_dirs.index(base)+1}/{len(base_dirs)})[/dim][/i]")

                console.rule(f"Performing QC on all studies in [b cyan]{os.path.basename(base)}[/]", characters="=", style='dim')

                studies_in = os.path.join(base, args.projects)
                studies = names_list(studies_in)
                console.print(
                    panel.fit(
                        f"[dim]{common.EMOJI_SPARKLE} Found[/] [bold]{len(studies)}[/] [dim]studies in[/] {os.path.basename(base)} [dim]dir[/]", 
                        title="# Studies",
                        border_style='dim cyan'
                    ),
                    style='italic'
                )
                
                task = progress.add_task(f"{common.EMOJI_PROCESS} [i][dim]Performing QC on[/dim] [cyan]{os.path.basename(base)}[/cyan] [dim]studies[/dim][/i]", total=len(studies))
                for study in studies:
                    
                    console.rule(f"[dim][i]Running QC on[/dim] {study}[/i]", characters="-", style='dim')
                    
                    raw_reads = f"{base}/{study}/raw_reads"
                    raw_qc = f"{base}/{study}/raw_qc"
                    raw_mqc = f"{base}/{study}/raw_mqc"
                    bb_out = f"{base}/{study}/bb_out"
                    bb_qc = f"{base}/{study}/bb_qc"
                    bb_mqc = f"{base}/{study}/bb_mqc"
                    fp_out = f"{base}/{study}/fp_out"
                    fp_qc = f"{base}/{study}/fp_qc"
                    fp_mqc = f"{base}/{study}/fp_mqc"
                    hostile_out = f"{base}/{study}/hostile_out"

                    os.makedirs(raw_qc, exist_ok=True)
                    os.makedirs(raw_mqc, exist_ok=True)
                    os.makedirs(bb_out, exist_ok=True)
                    os.makedirs(bb_qc, exist_ok=True)
                    os.makedirs(bb_mqc, exist_ok=True)
                    os.makedirs(fp_out, exist_ok=True)
                    os.makedirs(fp_qc, exist_ok=True)
                    os.makedirs(fp_mqc, exist_ok=True)
                    os.makedirs(hostile_out, exist_ok=True)

                    samples = names_list(f"{base}/{study}/{samples_in}")
                    samples_count = len(samples)
                    logger.info(f"{common.EMOJI_SPARKLE} Project [bold blue]{study}[/] has {samples_count} {p.plural('sample', samples_count)}...")

                    for status_sub in status_subs:
                        status_sub.update("[dim]Waiting for the single thread process to finish[/]")
                    # Generate QC reports for raw_reads
                    generate_qc_reports(raw_reads, raw_qc, raw_mqc, status_subs[0])

                    sample_lists = common.get_split_size(split_size, samples)

                    # Run BBDuk and fastp
                    common.run_concurrently(run_qc, split_size, sample_lists, status_subs=status_subs)

                    # Generate QC reports for filtered_reads
                    for status_sub in status_subs:
                        status_sub.update("[dim]Waiting for the single thread process to finish[/]")
                    generate_qc_reports(bb_out, bb_qc, bb_mqc, status_subs[0])
                    
                    for status_sub in status_subs:
                        status_sub.update("[dim]Waiting for the single thread process to finish[/]")
                    generate_qc_reports(fp_out, fp_qc, fp_mqc, status_subs[0])

                    # Run Hostile
                    common.run_concurrently(remove_host, split_size, sample_lists, status_subs=status_subs)

                    console.rule(f"[dim][i]Completed Quality Trimming for[/dim] {study}[/i]", characters="-", style='dim')
                    progress.update(task, advance=1)

                console.rule(f"Completed Quality Trimming for all studies in [b cyan]{os.path.basename(base)}[/]", characters="=", style='dim')
                console.print('\n')
                status.update(f"[i green dim]Processed [cyan b]{os.path.basename(base)}[/cyan b] studies[/i green dim]")
                
            
            console.print(
                panel.fit(
                    f"{common.EMOJI_CHECK} [bold green]Finished[/bold green] QC.", 
                    title="Done", 
                    border_style="green", title_align="right"
                ), 
                style='italic'
            )
        else:
            logger.error(f"{common.EMOJI_CROSS} Studies not found, is the provided {args.base_dir} directory correct?")