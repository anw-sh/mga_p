#!/usr/bin/python

# Paths are relative in this file

import argparse
import inflect
import os

import common, get_info

# Using logger from common.py
logger = common.logger
p = inflect.engine()

# Using rich elements from common.py
console = common.console
panel = common.Panel


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate a text file with list of samples in each project directory")
    parser.add_argument("-i", "--input", help="Path to the studies list as a text file.")
    args = parser.parse_args()

    if args.input:
        console.print(
            panel.fit(f"{common.EMOJI_SPARKLE} Generating list of samples for each project...", title=f"Study: {common.study_name.upper()}", title_align="left", border_style='dim bold yellow'),
            style='italic dim'
        )

        studies = get_info.names_list(args.input)

        for study in studies:
            os.system(f"ls {study}/raw_reads/ | sed 's/_.*//g' | sort | uniq > {study}/samples_list.txt")

            logger.warning(f"{common.EMOJI_LIST} List of samples generated for [bold]{study}[/].")
        
        console.print(
            panel.fit(f"{common.EMOJI_CHECK} Sample lists created successfully.", title="Success", border_style="green", title_align="right"),
            style="italic"
        )
    
    else:
        logger.error(f"{common.EMOJI_CROSS} Error: No such file. Provide a list of study names")