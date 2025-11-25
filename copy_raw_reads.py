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
    transient=False,
)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Classify reads using kraken2 and bracken, followed by conversion to MPA format...")
    parser.add_argument("-b", "--base_dir", help="Base directory with all data. (Default: all_data)", default="all_data")
    parser.add_argument("-s", "--samples", help="List of sample IDs as text file. (Default: samples_list.txt)", default="samples_list.txt")
    parser.add_argument("-p", "--projects", help="List of project names as text file. (Default: studies_list.txt)", default="studies_list.txt")
    parser.add_argument("-a", "--aliases", help="Study aliases. (Default: st_aliases.csv)", default="st_aliases.csv")
    parser.add_argument("-t", "--threads", type=int, default=1, help="Number of threads (Default: 1)")
    args=parser.parse_args()

    console.print(  
        panel(
            f"{common.EMOJI_SPARKLE} Copying raw reads...", 
            title=f"Study: {common.study_name.upper()}", 
            title_align="left", 
            border_style='dim bold yellow'
        ), 
        style='italic dim'
    )
    samples_in = args.samples
    threads = args.threads

    st_aliases = make_dict(args.aliases)
    console.print(
        panel.fit(
            f"[dim]{common.EMOJI_SPARKLE} Found[/] [bold]{len(st_aliases)}[/] [dim]study {p.plural('aliases', len(st_aliases))} in[/] {args.aliases}", 
            title="Study aliases", 
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

    with Live(panel(progress), console=console, transient=True):
        if base_dirs:
            for base in base_dirs:
                console.rule(f"Copying files of [b cyan]{os.path.basename(base)}[/]", characters="=", style='dim')

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

                task = progress.add_task(f"{common.EMOJI_PROCESS} [i][dim]Syncing reads to[/dim] [cyan]{os.path.basename(base)}[/cyan] [dim]studies[/dim][/i]", total=len(studies))
                for study in studies:
                    
                    console.rule(f"[dim][i]Running rsync on[/dim] {study}[/i] \n", characters="-", style='dim')
                    
                    raw_reads = f"{base}/{study}/raw_reads"
                    source_dir = f"/data/cmi_admin/anwesh/hgm/all_data/sg/{st_aliases[study]}/raw_reads"

                    os.makedirs(raw_reads, exist_ok=True)

                    samples = names_list(f"{base}/{study}/{samples_in}")
                    samples_count = len(samples)
                    logger.info(f"{common.EMOJI_SPARKLE} Project [bold blue]{study}[/] has {samples_count} {p.plural('sample', samples_count)}...")

                    task_samples = progress.add_task(f"{common.EMOJI_PROCESS} [i][dim]Syncing[/dim] [cyan]{samples_count}[/cyan] [dim]samples of[/dim] [cyan]{study}[/cyan][/i]", total=samples_count)
                    for sample in samples:
                        common.run_command(
                            f"rsync -avrP {source_dir}/{sample}* {raw_reads}/",
                            desc=f"Syncing {sample} reads"
                        )
                        
                        progress.update(task_samples, advance=1)
                        console.rule(f"[dim i]{common.EMOJI_CHECK} Copied [blue]{sample}[/blue]'s raw reads[/dim i]", characters="-", style='dim')

                    console.rule(f"[dim][i]Completed syncing raw reads of[/dim] {study}[/i]", characters="-", style='dim')
                    progress.update(task, advance=1)                
            
            console.print(
                panel.fit(
                    f"{common.EMOJI_CHECK} [bold green]Finished[/bold green] Read Sync.", 
                    title="Done", 
                    border_style="green", title_align="right"
                ), 
                style='italic'
            )
        else:
            logger.error(f"{common.EMOJI_CROSS} Studies not found, is the provided {args.base_dir} directory correct?")