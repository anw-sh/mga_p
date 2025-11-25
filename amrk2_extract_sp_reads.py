#!/usr/bin/python

import argparse
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
    transient=False,
)

# extract_kraken_reads.py -k ansi_11_irl/kraken_out/IRL_R1/IRL_R1.out -r ansi_11_irl/kraken_out/IRL_R1/IRL_R1.report -1 ansi_11_irl/hostile_out/IRL_R1_R1.clean_1.fastq.gz -2 ansi_11_irl/hostile_out/IRL_R1_R1.clean_1.fastq.gz -o test_IRL_R1.fq -o2 test_IRL_R1_2.fq --fastq-output -t 239935 --include-children

# Extract reads function
def extract_sp_reads(sub_list, status_sub):
    for species in sub_list:
        sp_dir = f"{amrk2_sp_reads}/{species}"
        os.makedirs(sp_dir, exist_ok=True)
        tax_id = sp_IDs_dict[species]
        for sample in samples:
            status_sub.update(f"[i][dim]Extracting reads of[/dim] [blue]{study}[/blue] {common.EMOJI_PLAY} [magenta]{sample}[/magenta] ({samples.index(sample)+1}/{len(samples)})[dim] for species[/dim] [green]{species}[/green][dim]: ({sub_list.index(species)+1}/{len(sub_list)}) [/dim][/i]", spinner='point', spinner_style='magenta')

            if any(file in os.listdir(sp_dir) for file in [f"{sample}_2.fq.gz", f"{sample}.fq.gz"]):
                # logger.info(f"{common.EMOJI_CHECK} {species} reads already extracted for [green]{sample}[/green]. Skipping...")
                continue
            elif any(file in os.listdir(sp_dir) for file in [f"{sample}_2.fq", f"{sample}.fq"]):
                # logger.info(f"{common.EMOJI_CHECK} {species} reads already extracted for [green]{sample}[/green] but uncompressed...")
                
                status_sub.update(f"[i][dim]Compressing extracted reads of[/dim] [blue]{study}[/blue] {common.EMOJI_PLAY} [magenta]{sample}[/magenta] ({samples.index(sample)+1}/{len(samples)})[dim] for species[/dim] [green]{species}[/green][dim]: ({sub_list.index(species)+1}/{len(sub_list)}) [/dim][/i]", spinner='toggle10', spinner_style='sky_blue2')
                common.run_command(
                    f"pigz {sp_dir}/{sample}_*fq",
                    desc=f"Compressing {species} - {sample} reads"
                )
                status_sub.update(f"[i][dim]Compressed the extracted reads. Waiting for other processes to finish [/dim][/i]", spinner='toggle9', spinner_style='blue')
            else:
                logger.info(f"{common.EMOJI_PROCESS} Extracting reads of [green]{species}[/green] from [blue]{sample}[/blue]...")
                common.run_command(
                    f"mamba run -n {utility_paths['krakentools']} extract_kraken_reads_mod.py -k {kraken_out}/{sample}/{sample}.out -r {kraken_out}/{sample}/{sample}.report -1 {hostile_out}/{sample}_R1.clean_1.fastq.gz -2 {hostile_out}/{sample}_R2.clean_2.fastq.gz -o {sp_dir}/{sample}_1.fq -o2 {sp_dir}/{sample}_2.fq --fastq-output -t {tax_id} --include-children",
                    desc=f"Extracting {species} reads from {sample}"
                )
                
                console.rule(f"[dim i]{common.EMOJI_CHECK} Extracted reads of [green]{species}[/green] from [blue]{study}[/blue] {common.EMOJI_PLAY} [magenta]{sample}[/magenta][/dim i]", characters="-", style='dim')
                    
                status_sub.update(f"[i][dim]Compressing extracted reads of[/dim] [blue]{study}[/blue] {common.EMOJI_PLAY} [magenta]{sample}[/magenta] ({samples.index(sample)+1}/{len(samples)})[dim] for species[/dim] [green]{species}[/green][dim]: ({sub_list.index(species)+1}/{len(sub_list)}) [/dim][/i]", spinner='toggle10', spinner_style='sky_blue2')
                common.run_command(
                    f"pigz {sp_dir}/{sample}_*fq",
                    desc=f"Compressing {species} - {sample} reads"
                )
            status_sub.update(f"[i][dim]Compressed the extracted reads. Waiting for other processes to finish [/dim][/i]", spinner='toggle9', spinner_style='blue')

        
        console.rule(f"[dim i]{common.EMOJI_CHECK} Completed extracting reads for species [green]{species}[/green] from all samples in study [blue]{study}[/blue][/dim i]", characters="=", style='dim')



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Classify reads using kraken2 and bracken, followed by conversion to MPA format...")
    parser.add_argument("-b", "--base_dir", help="Base directory with all data. (Default: all_data)", default="all_data")
    parser.add_argument("-s", "--samples", help="List of sample IDs as text file. (Default: samples_list.txt)", default="samples_list.txt")
    parser.add_argument("-p", "--projects", help="List of project names as text file. (Default: studies_list.txt)", default="studies_list.txt")
    parser.add_argument("-u", "--utility_paths", help="Envs or Paths for tools and DBs as a CSV file. (Default: utility_paths.csv)", default="utility_paths.csv")
    parser.add_argument("-c", "--species", help="Species list file (Default: species_list.csv)", default="species_list.csv")
    parser.add_argument("-t", "--threads", type=int, default=1, help="Number of threads (Default: 1)")
    parser.add_argument("-l", "--split_size", type=int, choices=[1,2,3,5,10,20,30,40,50,60,70,80,90,100], nargs="?", default=1, help="Number of sub_lists to make, and run concurrently (Default: no splitting, everything as one list).")
    args=parser.parse_args()

    console.print(  
        panel(
            f"{common.EMOJI_SPARKLE} Extracting reads with KrakenTools...", 
            title=f"Study: {common.study_name.upper()}", 
            title_align="left", 
            border_style='dim bold yellow'
        ), 
        style='italic dim'
    )
    samples_in = args.samples
    species_in = args.species
    utility_paths_in = args.utility_paths
    threads = args.threads
    split_size = args.split_size

    # Get the env names
    utility_paths = make_dict(utility_paths_in)
    console.print(
        panel.fit(
            f"[dim]{common.EMOJI_SPARKLE} Found[/] [bold]{len(utility_paths)}[/] [dim]utility {p.plural('path', len(utility_paths))} in[/] {utility_paths_in}", 
            title="Utility Paths", 
            border_style='dim cyan'
        ),
        style='italic'
    )

    sp_IDs_dict = make_dict(species_in)
    console.print(
        panel.fit(
            f"[dim]{common.EMOJI_SPARKLE} Found[/dim] [bold]{len(sp_IDs_dict)}[/bold] [dim]species in[/dim] {species_in}", 
            title="Species to extract", 
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
    status = console.status(f"[i][dim]Initiating read extraction on[/dim] {len(base_dirs)} [dim] study {p.plural('type', len(base_dirs))}[/dim][/i]")
    
    # Per-study statuses
    status_subs = []
    for i in range(split_size):
        status_subs.append(console.status(f"[i][dim]Starting KrakenTools on thread {i+1}...[/dim][/i]"))

    with Live(panel(Group(status, progress, *status_subs)), console=console, transient=True):
        if base_dirs:
            for base in base_dirs:
                status.update(f"[i][dim]Running KrakenTools on[/dim] [cyan]{os.path.basename(base)}[/cyan] [dim]datasets ({base_dirs.index(base)+1}/{len(base_dirs)})[/dim][/i]")

                console.rule(f"Extracting reads from all studies in [b cyan]{os.path.basename(base)}[/]", characters="=", style='dim')

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
                
                task = progress.add_task(f"{common.EMOJI_PROCESS} [i][dim]Extracting reads from[/dim] [cyan]{os.path.basename(base)}[/cyan] [dim]studies[/dim][/i]", total=len(studies))
                
                for study in studies:
                    
                    console.rule(f"[dim][i]Running KrakenTools on[/dim] {study}[/i] \n", characters="-", style='dim')
                    
                    hostile_out = f"{base}/{study}/hostile_out"
                    kraken_out = f"{base}/{study}/kraken_out"
                    amrk2_sp_reads = f"{base}/amrk2_species_reads"

                    os.makedirs(kraken_out, exist_ok=True)
                    os.makedirs(amrk2_sp_reads, exist_ok=True)

                    samples = names_list(f"{base}/{study}/{samples_in}")
                    samples_count = len(samples)
                    logger.info(f"{common.EMOJI_SPARKLE} Project [bold blue]{study}[/] has {samples_count} {p.plural('sample', samples_count)}...")

                    species_all = list(sp_IDs_dict.keys())
                    console.print(f"Total species: {len(species_all)}", style='italic dim')

                    # sample_lists = common.get_split_size(split_size, samples)
                    species_lists = common.get_split_size(split_size, species_all)

                    # Extract species reads
                    common.run_concurrently(extract_sp_reads, split_size, species_lists, status_subs=status_subs)

                    # Status to deal with empty processes
                    for status_sub in status_subs:
                        status_sub.update("[dim]Waiting for the other processes to finish...[/dim] \n", spinner='toggle9', spinner_style='magenta')

                    console.rule(f"[dim][i]Extracted reads per species for[/dim] {study}[/i]", characters="-", style='dim')
                    progress.update(task, advance=1)

                console.rule(f"Completed read extraction for all studies in [b cyan]{os.path.basename(base)}[/]", characters="=", style='dim')
                status.update(f"[i green dim]Processed [cyan b]{os.path.basename(base)}[/cyan b] studies[/i green dim]")
                
            
            console.print(
                panel.fit(
                    f"{common.EMOJI_CHECK} [bold green]Finished[/bold green] Read Extraction.", 
                    title="Done", 
                    border_style="green", title_align="right"
                ), 
                style='italic'
            )
        else:
            logger.error(f"{common.EMOJI_CROSS} Studies not found, is the provided {args.base_dir} directory correct?")