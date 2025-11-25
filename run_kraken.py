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
    console=console,
    transient=True,
)

# Kraken2 function
def run_kraken(sub_list, status_sub):
    for sample in sub_list:
        status_sub.update(f"[i][dim]Classifying reads of[/dim] [blue]{study}[/blue] {common.EMOJI_PLAY} [magenta]{sample}[/magenta][dim]: ({sub_list.index(sample)+1}/{len(sub_list)}) [/dim][/i] \n", spinner='point', spinner_style='magenta')

        os.makedirs(f"{kraken_out}/{sample}", exist_ok=True)
        if f"{sample}.report" in os.listdir(f"{kraken_out}/{sample}"):
            logger.info(f"{common.EMOJI_CHECK} Already classified the reads of [green]{sample}[/green]. Skipping...")
        else:
            logger.info(f"{common.EMOJI_PROCESS} Running Kraken2 on [blue]{sample}[/blue]...")
            # common.run_command(
            #     f"mamba run -n {utility_paths['Kraken2']} k2 classify --db {utility_paths['kraken_DB']} --threads {threads} --paired --output {kraken_out}/{sample}/{sample}.out --report {kraken_out}/{sample}/{sample}.report --use-names {hostile_out}/{sample}_R1.clean_1.fastq.gz {hostile_out}/{sample}_R2.clean_2.fastq.gz",
            #     desc=f"Running Kraken2 on {sample}"
            # )

            # With memory mapping
            common.run_command(
                f"mamba run -n {utility_paths['Kraken2']} k2 classify --db {utility_paths['kraken_DB']} --memory-mapping --threads {threads} --paired --output {kraken_out}/{sample}/{sample}.out --report {kraken_out}/{sample}/{sample}.report --use-names {hostile_out}/{sample}_R1.clean_1.fastq.gz {hostile_out}/{sample}_R2.clean_2.fastq.gz",
                desc=f"Running Kraken2 on {sample}"
            )
        console.rule(f"[dim i]{common.EMOJI_CHECK} Classified the reads of [blue]{study}[/blue] {common.EMOJI_PLAY} [magenta]{sample}[/magenta][/dim i]", characters="-", style='dim')
        status_sub.update(f"[i][dim]Classified reads of [/dim] [blue]{study}[/blue] {common.EMOJI_PLAY} [magenta]{sample}[/magenta][dim] with Kraken2, Waiting for other processes [/dim][/i] \n")

# Bracken function
def run_bracken(sub_list, status_sub):
    for sample in sub_list:
        status_sub.update(f"[i][dim]Running Bracken on[/dim] [blue]{study}[/blue] {common.EMOJI_PLAY} [magenta]{sample}[/magenta] [dim]: ({sub_list.index(sample)+1}/{len(sub_list)}) [/dim][/i] \n", spinner='point', spinner_style='magenta')
        if f"{sample}.bracken" in os.listdir(f"{kraken_out}/{sample}"):
            logger.info(f"{common.EMOJI_CHECK} Already estimated the abundance of [green]{sample}[/green]. Skipping...")
        else:
            logger.info(f"{common.EMOJI_PROCESS} Running Bracken on [blue]{sample}[/blue]...")
            common.run_command(
                f"mamba run -n {utility_paths['Bracken']} bracken -d {utility_paths['kraken_DB']} -i {kraken_out}/{sample}/{sample}.report -o {kraken_out}/{sample}/{sample}.bracken",
                desc=f"Calculating abundances with Bracken"
            )
        
        console.rule(f"[dim i]{common.EMOJI_CHECK} Generated abundance table for [blue]{study}[/blue] {common.EMOJI_PLAY} [magenta]{sample}[/magenta][/dim i]", characters="-", style='dim')
        status_sub.update(f"[i][dim]Generated abundance table for [/dim] [blue]{study}[/blue] {common.EMOJI_PLAY} [magenta]{sample}[/magenta][dim] with Bracken, Waiting for other processes [/dim][/i] \n")





if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Classify reads using kraken2 and bracken, followed by conversion to MPA format...")
    parser.add_argument("-b", "--base_dir", help="Base directory with all data. (Default: all_data)", default="all_data")
    parser.add_argument("-s", "--samples", help="List of sample IDs as text file. (Default: samples_list.txt)", default="samples_list.txt")
    parser.add_argument("-p", "--projects", help="List of project names as text file. (Default: studies_list.txt)", default="studies_list.txt")
    parser.add_argument("-u", "--utility_paths", help="Envs or Paths for tools and DBs as a CSV file. (Default: utility_paths.csv)", default="utility_paths.csv")
    parser.add_argument("-t", "--threads", type=int, default=1, help="Number of threads (Default: 1)")
    parser.add_argument("-l", "--split_size", type=int, choices=[1,2,3], nargs="?", default=1, help="Number of sub_lists to make, and run concurrently (Default: no splitting, everything as one list).")
    args=parser.parse_args()

    console.print(  
        panel(
            f"{common.EMOJI_SPARKLE} Classifying reads with Kraken2 suite...", 
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
    status = console.status(f"[i][dim]Initiating read classification on[/dim] {len(base_dirs)} [dim] study {p.plural('type', len(base_dirs))}[/dim][/i]")
    
    # Per-study statuses
    status_subs = []
    for i in range(split_size):
        status_subs.append(console.status(f"[i][dim]Running Kraken2 on thread {i+1}...[/dim][/i]"))

    with Live(Group(status, progress, *status_subs), console=console, transient=True):
        if base_dirs:
            for base in base_dirs:
                status.update(f"[i][dim]Running Kraken2 on[/dim] [cyan]{os.path.basename(base)}[/cyan] [dim]datasets ({base_dirs.index(base)+1}/{len(base_dirs)})[/dim][/i]")

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
                
                task = progress.add_task(f"{common.EMOJI_PROCESS} [i][dim]Classifying reads from[/dim] [cyan]{os.path.basename(base)}[/cyan] [dim]studies[/dim][/i]", total=len(studies))
                for study in studies:
                    
                    console.rule(f"[dim][i]Running Kraken2 on[/dim] {study}[/i] \n", characters="-", style='dim')
                    
                    hostile_out = f"{base}/{study}/hostile_out"
                    kraken_out = f"{base}/{study}/kraken_out"

                    os.makedirs(kraken_out, exist_ok=True)

                    samples = names_list(f"{base}/{study}/{samples_in}")
                    samples_count = len(samples)
                    logger.info(f"{common.EMOJI_SPARKLE} Project [bold blue]{study}[/] has {samples_count} {p.plural('sample', samples_count)}...")

                    sample_lists = common.get_split_size(split_size, samples)

                    # Run Kraken2
                    common.run_concurrently(run_kraken, split_size, sample_lists, status_subs=status_subs)

                    # Run Bracken
                    for status_sub in status_subs:
                        status_sub.update("[dim]Running Bracken...[/dim]", spinner='toggle9', spinner_style='magenta')
                    
                    run_bracken(samples, status_sub=status_subs[0])

                    # Convert Bracken reports to MPA format
                    for status_sub in status_subs:
                        status_sub.update("[dim]Converting bracken reports to mpa format...[/dim] \n", spinner='toggle9', spinner_style='magenta')
                    
                    for sample in samples:
                        if f"{sample}_mpa.txt" in os.listdir(f"{kraken_out}/{sample}"):
                            logger.info(f"{common.EMOJI_CHECK} MPA file already exists for [green]{sample}[/green]. Skipping...")
                        else:
                            logger.info(f"{common.EMOJI_PROCESS} Converting [blue]{sample}[/blue]'s Bracken report to MPA format...")
                            common.run_command(
                                f"mamba run -n {utility_paths['krakentools']} kreport2mpa.py -r {kraken_out}/{sample}/{sample}_bracken_species.report -o  {kraken_out}/{sample}/{sample}_mpa.txt"
                            )
                        
                    console.rule(f"[dim i]{common.EMOJI_CHECK} Converted [blue]{study}[/blue]'s Bracken reports to MPA format[/dim i]", characters="-", style='dim')

                    console.rule(f"[dim][i]Completed read classification for[/dim] {study}[/i]", characters="-", style='dim')
                    progress.update(task, advance=1)

                console.rule(f"Completed read classification for all studies in [b cyan]{os.path.basename(base)}[/]", characters="=", style='dim')
                console.print('\n')
                status.update(f"[i green dim]Processed [cyan b]{os.path.basename(base)}[/cyan b] studies[/i green dim]")
                
            
            console.print(
                panel.fit(
                    f"{common.EMOJI_CHECK} [bold green]Finished[/bold green] Read Classification.", 
                    title="Done", 
                    border_style="green", title_align="right"
                ), 
                style='italic'
            )
        else:
            logger.error(f"{common.EMOJI_CROSS} Studies not found, is the provided {args.base_dir} directory correct?")