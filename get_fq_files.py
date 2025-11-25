#!/usr/bin/python

import argparse
import inflect
import os
import time
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
    transient=True,
    console=console,
)

def get_fq(sub_list, status_sub):
    env_name = utility_paths["sra-tools"]
    for sample in sub_list:
        status_sub.update(f"[i][dim]Retrieving data for[/dim] [blue]{study}[/blue] {common.EMOJI_PLAY} [magenta]{sample}[/magenta][dim]: ({sub_list.index(sample)+1}/{len(sub_list)}) | ({studies.index(study)+1}/{len(studies)}) [/dim][/i] \n", spinner='point', spinner_style='magenta')

        if any(file in os.listdir(raw_reads) for file in [f"{sample}_2.fastq", f"{sample}_2.fastq.gz", f"{sample}.fastq.gz"]):
            logger.info(f"{common.EMOJI_CHECK} [green]{sample}[/green] already downloaded. Skipping...")
        else:
            logger.info(f"{common.EMOJI_DOWNLOAD} Downloading [blue]{sample}.sra[/blue] from NCBI's SRA...")
            common.run_command(f"mamba run -n {env_name} prefetch {sample} -O {sra_files} -X 150G", desc=f"Fetching {sample}")
            # logger.debug(f"{common.EMOJI_DOWNLOAD} mamba run -n {env_name} prefetch {sample} -p -O {sra_files}")
            # os.system(f"touch {sra_files}/{sample}.sra")  # Simulate download
            # time.sleep(0.2)  # Simulate download time
            logger.info(f"{common.EMOJI_PROCESS} Extracting FASTQ files from [blue]{sample}.sra[/blue]...")
            common.run_command(f"mamba run -n {env_name} fasterq-dump {sra_files}/{sample} -3 -O {raw_reads} -e {threads}", desc=f"Extracting {sample}.fastq files")
            # logger.debug(f"{common.EMOJI_PROCESS} mamba run -n {env_name} fasterq-dump {sra_files}/{sample} -3 -O {raw_reads} -e {threads} -p")
            # os.system(f"echo 'R1' > {raw_reads}/{sample}_1.fastq")  # Simulate extraction
            # os.system(f"echo 'R2' > {raw_reads}/{sample}_2.fastq")  # Simulate extraction
            # os.system(f"echo 'R1' > {raw_reads}/{sample}.fastq")  # Simulate extraction
            # time.sleep(0.2)  # Simulate extraction time
        console.rule(f"[dim i]Obtained FASTQ files for [magenta]{sample}[/magenta][/dim i]", characters="-", style='dim')
        status_sub.update(f"[i][dim]Fastq files downloaded: ({sub_list.index(sample)+1}/{len(sub_list)}) | ({studies.index(study)+1}/{len(studies)}) [/dim][/i] \n", spinner='point', spinner_style='magenta')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download FASTQ files from SRA and compress them...")
    parser.add_argument("-b", "--base_dir", help="Base directory with all data. (Default: all_data)", default="all_data")
    parser.add_argument("-s", "--samples", help="List of sample IDs as text file. (Default: samples_list.txt)", default="samples_list.txt")
    parser.add_argument("-p", "--projects", help="List of project names as text file. (Default: studies_list.txt)", default="studies_list.txt")
    parser.add_argument("-u", "--utility_paths", help="Envs or Paths for tools and DBs as a CSV file. (Default: utility_paths.csv)", default="utility_paths.csv")
    parser.add_argument("-t", "--threads", type=int, default=1, help="Number of threads (Default: 1)")
    parser.add_argument("-l", "--split_size", type=int, choices=[1,2,3,4,5], nargs="?", default=1, help="Number of sub_lists to make, and run concurrently (Default: no splitting, everything as one list).")
    args=parser.parse_args()

    console.print(  
        panel(
            f"{common.EMOJI_SPARKLE} Downloading FASTQ files...", 
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
        logger.info(f"{common.EMOJI_SPARKLE} Found project file [bold blue]{args.projects}[/] in [bold blue]{args.base_dir}[/].")
        base_dirs = [base_dir]
    else:
        logger.info(f"{common.EMOJI_PROCESS} [bold blue]{args.projects}[/] not in [bold blue]{args.base_dir}[/], searching subdirectories.")
        # It may work only for one sub-level
        base_dirs = [os.path.join(base_dir, d) for d in os.listdir(base_dir) if args.projects in os.listdir(os.path.join(base_dir, d))]

    # Main status
    status = console.status(f"[i][dim]Initiating on[/dim] {len(base_dirs)} [dim]data types[/dim][/i]")
    # Per-type status (replaced with progress bar)
    # status_st = console.status(f"[i][dim]Initiating per-type retrievals[/dim][/i]")
    
    # Per-study statuses
    status_subs = []
    for i in range(split_size):
        status_subs.append(console.status(f"[i][dim]Running on[/dim] {i+1} [dim] thread...[/dim][/i]"))

    with Live(Group(status, progress, *status_subs), console=console, transient=True):
        if base_dirs:
            for base in base_dirs:
                status.update(f"[i][dim]Retrieving data for[/dim] [cyan]{os.path.basename(base)}[/cyan] [dim]datasets ({base_dirs.index(base)+1}/{len(base_dirs)})[/dim][/i]")

                console.rule(f"Obtaining FASTQ files for all studies in [b cyan]{os.path.basename(base)}[/]", characters="=", style='dim')

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
                
                task = progress.add_task(f"{common.EMOJI_PROCESS} [i][dim]Fetching Fastq files of[/dim] [cyan]{os.path.basename(base)}[/cyan] [dim]studies[/dim][/i]", total=len(studies))
                for study in studies:
                    # status_st.update(f"[i][dim]Retrieving data for[/dim] [cyan]{os.path.basename(base)}[/cyan] {common.EMOJI_PLAY} [blue]{study}[/blue][dim]:({studies.index(study)+1}/{len(studies)}) | ({base_dirs.index(base)+1}/{len(base_dirs)}) [/dim][/i] \n", spinner='simpleDots', spinner_style='blue')

                    console.rule(f"[dim][i]Obtaining Fastq files for[/dim] {study}[/i]", characters="-", style='dim')
                    
                    sra_files = f"{base}/{study}/sra_files"
                    raw_reads = f"{base}/{study}/raw_reads"

                    os.makedirs(raw_reads, exist_ok=True)
                    os.makedirs(sra_files, exist_ok=True)

                    samples = names_list(f"{base}/{study}/{samples_in}")
                    samples_count = len(samples)
                    logger.info(f"{common.EMOJI_SPARKLE} Project [bold blue]{study}[/] has {samples_count} {p.plural('sample', samples_count)}...")

                    sample_lists = common.get_split_size(split_size, samples)

                    common.run_concurrently(get_fq, split_size, sample_lists, status_subs=status_subs)

                    for status_sub in status_subs:
                        status_sub.update(f"[dim]Compressing FASTQ files of [blue]{study}[/blue][/dim]")
                    logger.info(f"{common.EMOJI_ZIP} Compressing FASTQ files of [bold blue]{study}[/]...")
                    common.run_command(f"pigz -v -p {threads} {raw_reads}/*.fastq", desc=f"Compressing fastq files of {study}")
                    # time.sleep(0.5)  # Simulate compression time
                    
                    for status_sub in status_subs:
                        status_sub.update(f"[dim]Removing SRA files of [blue]{study}[/blue][/dim]")
                    logger.info(f"{common.EMOJI_TRASH} Removing SRA files of [bold blue]{study}[/]...\n")
                    common.run_command(f"rm -rv {sra_files}", desc=f'Removing sra files from {study}')
                    # time.sleep(0.2)  # Simulate compression time
                    for status_sub in status_subs:
                        status_sub.update(f"[dim]Removed SRA files from [blue]{study}[/blue]. Waiting for other processes to finish.[/dim]")

                    console.rule(f"[dim][i]Completed fetching Fastq files for[/dim] {study}[/i]", characters="-", style='dim')
                    progress.update(task, advance=1)

                console.rule(f"Completed downloading FASTQ files for all studies in [b cyan]{os.path.basename(base)}[/]", characters="=", style='dim')
                console.print('\n')
                status.update(f"[i green dim]Retrieved [cyan b]{os.path.basename(base)}[/cyan b] studies[/i green dim]")
                
            
            console.print(
                panel.fit(
                    f"{common.EMOJI_CHECK} [bold green]Finished[/bold green] downloading FASTQ files from SRA.", 
                    title="Done", 
                    border_style="green", title_align="right"
                ), 
                style='italic'
            )
        else:
            logger.error(f"{common.EMOJI_CROSS} Studies not found, is the provided {args.base_dir} directory correct?")