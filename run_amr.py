#!/usr/bin/python

import argparse
import inflect
import os
import subprocess

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

# run_command

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Classify reads using kraken2 and bracken, followed by conversion to MPA format...")
    parser.add_argument("-b", "--base_dir", help="Base directory with all data. (Default: all_data)", default="all_data")
    parser.add_argument("-s", "--samples", help="List of sample IDs as text file. (Default: samples_list.txt)", default="samples_list.txt")
    parser.add_argument("-p", "--projects", help="List of project names as text file. (Default: studies_list.txt)", default="studies_list.txt")
    parser.add_argument("-u", "--utility_paths", help="Envs or Paths for tools and DBs as a CSV file. (Default: utility_paths.csv)", default="utility_paths.csv")
    parser.add_argument("-t", "--threads", type=int, default=1, help="Number of threads (Default: 1)")
    parser.add_argument("-l", "--split_size", type=int, choices=[1], nargs="?", default=1, help="Number of sub_lists to make, and run concurrently (Default: no splitting, everything as one list).")
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
    status = console.status(f"[i][dim]Initiating AMR identification on[/dim] {len(base_dirs)} [dim] study {p.plural('type', len(base_dirs))}[/dim][/i]")

    with Live(panel(Group(status, progress)), console=console, transient=True):
        if base_dirs:
            # Change to AMRplusplus directory
            os.chdir(utility_paths['AMR++_path'])
            cwd = os.getcwd()
            print("Current working directory:", cwd)
            
            for base in base_dirs:
                status.update(f"[i][dim]Running AMR++ on[/dim] [cyan]{os.path.basename(base)}[/cyan] [dim]datasets ({base_dirs.index(base)+1}/{len(base_dirs)})[/dim][/i]")

                console.rule(f"Running AMR++ on all studies in [b cyan]{os.path.basename(base)}[/]", characters="=", style='dim')

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

                task = progress.add_task(f"[blue]:gear:[/blue] [i][dim]Running AMR++ on[/dim] [cyan]{len(studies)}[/cyan] [dim]studies[/dim][/i]", total=len(studies))

                for study in studies:
                    status.update(f"[i][dim]Running AMR++ on [/dim] [blue]{study}[/blue][dim]: ({studies.index(study)+1}/{len(studies)}) [/dim][/i]", spinner='point', spinner_style='magenta')

                    hq_reads = f"{base_dir}/{study}/hostile_out"
                    amr_out = f"{base_dir}/{study}/amr_out"

                    os.makedirs(amr_out, exist_ok=True)

                    if os.path.exists(f'{amr_out}/Results') and len(os.listdir(f"{amr_out}/Results")) == 4:
                        logger.info(f"{common.EMOJI_CHECK} AMR++ already run for [blue]{study}[/blue]. Skipping...")
                    else:
                        # common.run_command(
                        #     f'conda run -n {utility_paths['AMR++']} nextflow run main_AMR++.nf --pipeline resistome --reads "{hq_reads}/*_R{{1,2}}.clean_{{1,2}}.fastq.gz" --output "{amr_out}" --snp Y --deduped Y --threads {threads} -resume',
                        #     desc=f"Running AMR++ on {study}"
                        # )

                        # With manual activation of conda env
                        common.run_command(
                            f'nextflow run main_AMR++.nf --pipeline resistome --reads "{hq_reads}/*_R{{1,2}}.clean_{{1,2}}.fastq.gz" --output "{amr_out}" --snp Y --deduped Y --threads {threads} -resume',
                            desc=f"Running AMR++ on {study}"
                        )
                        
                        status.update(f"[i][dim]Clearing the [b]work[/b] directory[/dim][/i]", spinner='toggle9', spinner_style='gold1')
                        logger.info(f"{common.EMOJI_TRASH} Clearing the work directory...")
                        common.run_command("rm -rv work/*", desc="Removing temporary files from work dir")

                    progress.update(task, advance=1)
                    console.rule(f"[dim][i]Generated AMR gene abundance tables for[/dim] [blue]{study}[/blue][/i]", characters="-", style='dim')
            
            # Change to amr_hg directory
            # os.system(f'cd {base_dir}')
            console.print(
                panel.fit(
                    f"{common.EMOJI_CHECK} [bold green]Finished[/bold green] AMR++.", 
                    title="Done", 
                    border_style="green", title_align="right"
                ), 
                style='italic'
            )
        else:
            logger.error(f"{common.EMOJI_CROSS} Studies not found, is the provided {args.base_dir} directory correct?")

