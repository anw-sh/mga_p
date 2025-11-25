#!/usr/bin/python

import argparse
import inflect  
import os
import pandas as pd

import common

# Using logger from common.py
logger = common.logger
p = inflect.engine()

# Using rich elements from common.py
console = common.console
panel = common.Panel
progress = common.progress

def list_to_text(file_path, items):
    try:
        with open(file_path, 'w') as f:
            for item in items:
                f.write(f"{item}\n")
        logger.info(f"{common.EMOJI_LIST} Created text file at: [italic dim]{file_path}[/] with {len(items)} entries.")
    except IOError as e:
        logger.error(f"{common.EMOJI_CROSS} Error writing to file: {e}")
        console.print_exception(show_locals=True)

def make_dir_file(study, path_prefix):
    study_dir = os.path.join(os.getcwd(), path_prefix, study)
    os.makedirs(study_dir, exist_ok=True)
    logger.info(f":file_cabinet: Created directory for study: [bold blue]{study}[/] at [italic dim]{study_dir}[/]")
    
    study_df = df[df['Study_Alias'] == study]
    run_ids = study_df['Run'].tolist()
    run_file_path = os.path.join(study_dir, "samples_list.txt")
    list_to_text(run_file_path, run_ids)



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate a dataframe from CSV file and create required files and directories...")
    parser.add_argument("-i", "--input", help="Path to the input CSV file.")
    args = parser.parse_args()

    if args.input:
        console.print(
            panel.fit(f"{common.EMOJI_SPARKLE} Generating files and directories...", title=f"Study: {common.study_name.upper()}", title_align="left", border_style='dim bold yellow'),
            style='italic dim'
        )

        df = pd.read_csv(args.input)
        # console.print(df.head(), style="dim")

        # Create a list of unique Study_Aliases from the dataframe
        studies = df['Study_Alias'].unique().tolist()
        logger.info(f"{common.EMOJI_SPARKLE} Found {len(studies)} unique studies in the CSV file.")

        with progress:
            task = progress.add_task(f"{common.EMOJI_PROCESS} Generating files and directories...", total=len(studies))
                
            for study in studies:
                if study.startswith('a_'):
                    path_prefix = 'all_data/amp'
                    make_dir_file(study, path_prefix)
                    # time.sleep(0.05) # For testing progress bar
                elif study.startswith('w_'):
                    path_prefix = 'all_data/sg'
                    make_dir_file(study, path_prefix)
                    # time.sleep(0.05) # For testing progress bar
                elif study.startswith('m_'):
                    path_prefix = 'all_data/mix'
                    make_dir_file(study, path_prefix)
                    # time.sleep(0.05) # For testing progress bar
                else:
                    logger.warning(f"{common.EMOJI_WARNING} Study alias [bold]{study}[/] does not match expected prefixes. Skipping...")
                
                progress.update(task, advance=1)

        for type_dir in os.listdir('all_data'):
            type_list = os.listdir(os.path.join('all_data', type_dir))
            type_path = os.path.join('all_data', type_dir, 'studies_list.txt')
            list_to_text(type_path, type_list)

        console.print(
            panel.fit(f"{common.EMOJI_CHECK} Files and directories created successfully.", title="Success", border_style="green", title_align="right"),
            style="italic"
        )
    else:
        logger.error(f"{common.EMOJI_CROSS} Error: No input CSV file provided. Use -i or --input to specify the file path.")
