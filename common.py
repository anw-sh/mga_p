#!/usr/bin/python

import concurrent.futures
import logging
import os
import re
import subprocess
import time

from datetime import datetime
from rich.console import Console
from rich.panel import Panel
from rich.pretty import Pretty
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn, TimeRemainingColumn
from rich.logging import RichHandler

# Initiate Console() object
console = Console()

# LOG format
logging.basicConfig(level="NOTSET", format="[%(asctime)s]: %(message)s", datefmt="%Y-%m-%d %H:%M:%S", handlers=[RichHandler(show_time=False, rich_tracebacks=True, markup=True)])
logger = logging.getLogger('rich')

# Initiate Progress() object
progress = Progress(
    SpinnerColumn(),
    TextColumn("[progress.description]{task.description}"),
    BarColumn(),
    TextColumn("{task.completed}/{task.total}"),
    TimeElapsedColumn(),
    TimeRemainingColumn(),
    transient=False,
)

# Study name
study_name = os.path.basename(os.getcwd())

# Emoji variables
EMOJI_CHECK = "[green]:heavy_check_mark:[/green]"
EMOJI_CROSS = "[red]:heavy_multiplication_x:[/red]"
EMOJI_DOWNLOAD = "[steel_blue]:arrow_down:[/steel_blue]"
EMOJI_LIST = "[magenta]:right_arrow:[/magenta]"
EMOJI_PLAY = "[white]:play_button:[/white]"
EMOJI_PROCESS = "[blue]:gear:[/blue]"
EMOJI_SPARKLE = "[cyan]:sparkle:[/cyan]"
EMOJI_TRASH = "[orange1]:wastebasket:[/orange1]"
EMOJI_WARNING = "[yellow]:warning:[/yellow]"
EMOJI_ZIP = "[purple]:compression:[/purple]"

# Run a command without Live console update
def run_command_simple(command, desc=None, style="italic"):
    try:
        cmd = f"[yellow dim]$ {command}[/yellow dim]"
        start_time = datetime.now()
        console.print(Panel(cmd, border_style="dim", title=desc, expand=False))
        subprocess.run(command, check=True, shell=True)
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        console.print(f"{EMOJI_CHECK} Command finished in {(duration/60):.2f} minutes", style='italic')
    except subprocess.CalledProcessError as e:
        console.print(f"{EMOJI_CROSS} Error running {command}: {e}", style='italic red')
        console.print_exception(show_locals=True)

# Modified run_command - subprocess with rich (AI suggestion)
def run_command(command, desc=None, style="italic"):
    """
    Run a shell command with live Rich output
    """

    cmd = f"[yellow dim]$ {command}[/yellow dim]"
    # if desc:
    #     console.rule(f"[dim]Staring: {desc}[/dim]", characters="-", style="dim")
    
    start_time = datetime.now()
    console.print(Panel(cmd, border_style="dim", title=desc, expand=False))

    process = subprocess.Popen(
        command,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        universal_newlines=True
    )

    # Stream live output (line by line)
    # with Live(console=console, refresh_per_second=8, transient=True):
    for line in iter(process.stdout.readline, ""):
        line = line.strip()
        if line:
            if "error" in line.lower():
                console.print(line, style="bold red")
            elif "warning" in line.lower():
                console.print(line, style="yellow")
            else:
                console.print(line, style="green dim")
    
    process.wait()
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    if process.returncode == 0:
        console.print(f"{EMOJI_CHECK} Command finished in {(duration/60):.2f} minutes", style='italic')
    else:
        console.print(f"{EMOJI_CROSS} Command stopped after {(duration/60):2f} minutes (exit code [red]{process.returncode}[/red])", style='italic')

    # if desc:
    #     console.rule(f"[dim]{desc} - Done[/dim]", characters='-', style='dim')
    return process.returncode

# Get split size based lists
def get_split_size(split_size, samples):
    if split_size == 1:
        return samples
    else:
        split_list = [0, len(samples)]
        for i in range(1, split_size):
            split_list.insert(-1, int(len(samples)*(i/split_size)))
        # pprint(split_list, expand_all=True)

        sample_lists = []
        for i in range(split_size):
            sample_lists.append(samples[split_list[i]:split_list[i+1]])
        # pprint(sample_lists, expand_all=True)
        # print(sample_lists)
        return sample_lists

# Run concurrent processes on sample_lists with a pre-defined function, if split_size is greater than 1
def run_concurrently(run_func, split_size, sample_lists, status_subs):
    if split_size == 1:
        logger.info(f"{EMOJI_PROCESS} Executing as single process...")
        run_func(sample_lists, status_subs[0])
    else:
        logger.info(f"{EMOJI_PROCESS} Initiating {split_size} processes...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=split_size) as executor:
            # executor.map(run_func, sample_lists)
            # futures = [executor.submit(run_func, sub_list, status_subs[i]) for i, sub_list in enumerate(sample_lists)]
            # Adding delay between submissions
            futures = []
            for i, sub_list in enumerate(sample_lists):
                futures.append(executor.submit(run_func, sub_list, status_subs[i])) 
                time.sleep(1)  # Slight delay to stagger starts
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"{EMOJI_CROSS} Error in concurrent execution: {e}")
                    console.print_exception(show_locals=True)

# Get unique items from a list
def get_unique_items(in_list, pattern=None):
    if pattern:
        unique_items = {re.sub(pattern, '', item) for item in in_list}
    else:
        unique_items = list(set(in_list))
    return unique_items

# Get the names of files and dirs
def get_f_d_names(query_in):
    query_path = os.path.abspath(query_in)
    if os.path.isfile(query_path):
        logger.info(f"{EMOJI_SPARKLE} Retrieved the names of input [u]file[/u], and its parent directory")
        return query_path.split('/')[-2:]
    if os.path.isdir(query_path):
        logger.info(f"{EMOJI_SPARKLE} Retrieved the name of input [u]directory[/u]")
        # Alternatively use basename method
        return query_path.strip('/').split('/')[-1]


if __name__ == "__main__":
    console.print(
        Panel.fit(
            f"{EMOJI_SPARKLE} Common module for {study_name.upper()} scripts", 
            title=f"Study: {study_name.upper()}", 
            border_style="yellow dim", 
            title_align="left"
        ),
        style='italic dim'
    )

    logger.warning(f"{EMOJI_WARNING} This is a common module and is not intended to be run directly.")
    logger.info(f"{EMOJI_SPARKLE} However, here are a few [bold cyan]example usages[/] of the functions defined herein.")

    # Example usage of run_command
    logger.info(f'{EMOJI_PROCESS} Using run_command(cmd) to execute: echo "Hello, $USER!"')
    run_command('echo "Hello, $USER!"')
    
    # Example usage of get_split_size
    samples = ['sample1', 'sample2', 'sample3', 'sample4', 'sample5']
    split_size = 2
    logger.info(f'{EMOJI_PROCESS} Using get_split_size(split_size, samples) to split samples: {samples} into {split_size} parts')
    split_samples = get_split_size(split_size, samples)
    logger.info(f'{EMOJI_SPARKLE} Split lists of samples: ')
    console.print(Panel.fit(Pretty(split_samples, expand_all=True), title='Split lists', border_style='yellow dim'))
    
    # Example usage of run_concurrently
    status_subs = []
    for i in range(split_size):
        status_subs.append(console.status(f"[i][dim]Running on[/dim] {i+1} [dim] thread...[/dim][/i]"))

    def example_run_func(sub_list, status_sub):
        for sample in sub_list:
            status_sub.update(f"[i][dim]Example status[/dim] [blue]{sample}[/blue] [dim]: ({sub_list.index(sample)+1}/{len(sub_list)}) [/dim][/i] \n", spinner='point', spinner_style='magenta')
            logger.info(f"{EMOJI_LIST} Processing sample: {sample}")
            run_command('echo "Hello, $USER!"', desc="Test")
            time.sleep(1)  # Simulate processing time

    logger.info(f'{EMOJI_SPARKLE} Using run_concurrently(run_func, split_size, sample_lists) to process samples...')
    run_concurrently(example_run_func, split_size, split_samples, status_subs)
    
    console.print(
        Panel.fit(f"{EMOJI_CHECK} [bold green]Finished[/bold green] example usages of common module functions.", title="Done", border_style="green", title_align="right"),
        style='italic'
    )
    
    # Test print all EMOJI variables
    console.print(Panel.fit(f"{EMOJI_SPARKLE} {EMOJI_CHECK} {EMOJI_CROSS} {EMOJI_PLAY} {EMOJI_PROCESS} {EMOJI_WARNING} {EMOJI_LIST} {EMOJI_DOWNLOAD} {EMOJI_TRASH} {EMOJI_ZIP}", title='EMOJI Variables', border_style='cyan dim'))