#!/usr/bin/python

import argparse
import csv
import inflect

import common

# Using logger from common.py
logger = common.logger
p = inflect.engine()

# Using rich elements from common.py
console = common.console
panel = common.Panel

def names_list(in_file):
    try:
        with open(in_file) as f:
            dir_name, file_name = common.get_f_d_names(in_file)
            logger.info(f"{common.EMOJI_SPARKLE} Reading IDs from [i blue]{dir_name} {common.EMOJI_PLAY} {file_name}[/]")
            return [line.strip() for line in f]
    except IOError as e:
        logger.error(f"{common.EMOJI_CROSS} Error reading file: {e}")
        console.print_exception(show_locals=True)
        return []

def make_dict(in_file):
    try:
        with open(in_file) as f:
            reader = csv.reader(f)
            header = next(reader)  # Read and skip the header row
            logger.info(f"{common.EMOJI_SPARKLE} Generating [i][dim]{header[0]}:[/dim] [blue]{header[1]}[/blue][/i] pairs from [i dim]{in_file}[/]")
            return {row[0]: row[1] for row in reader}
    except IOError as e:
        logger.error(f"{common.EMOJI_CROSS} Error reading file: {e}")
        console.print_exception(show_locals=True)
        return {}



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Read Ids or names from a text file and parse a CSV file to generate Key-Value pairs...")
    parser.add_argument("-i", "--input", help="Text file containing the Ids or names.", default='studies_list.txt')
    parser.add_argument("-c", "--csv", help="List of key-value pairs as a CSV file.", default='utility_paths.csv')
    args = parser.parse_args()

    console.print(
        panel.fit(f"{common.EMOJI_SPARKLE} Obtaining study details...", title=f"Study: {common.study_name.upper()}", title_align="left", border_style='dim bold yellow'), 
        style='italic dim'
    )

    names = names_list(args.input)
    env_paths = make_dict(args.csv)
    if names and env_paths:
        names_count = len(names)
        dir_name, file_name = common.get_f_d_names(args.input)
        logger.info(f"{common.EMOJI_SPARKLE} [blue bold italic]{dir_name} {common.EMOJI_PLAY} {file_name}[/] has {names_count} {p.plural('ID', names_count)}...\n")
    
        console.print(
            panel.fit(
                f"[dim]{common.EMOJI_SPARKLE}[/] [bold]{len(env_paths)}[/] [dim]{p.plural('pair', len(env_paths))} found in {args.csv}[/]", 
                title="Info", 
                title_align="left", 
                border_style='dim yellow'
            ), 
            style='italic'
        )
        for key, value in env_paths.items():
            console.print(f"{common.EMOJI_LIST} [dim]{key}:[/]\t[blue]{value}[/]", style='bold')
        
        console.print(
            panel.fit(f"{common.EMOJI_CHECK} [bold green]Finished[/bold green] extracting information from input files.", title="Done", border_style="green", title_align="right"), 
            style='italic'
        )
    else:
        console.print(
            panel.fit(f"{common.EMOJI_CROSS} Errors found, see above.", title="Failed", border_style="red", title_align="right"), 
            style='italic red'
        )