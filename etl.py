from queries_sql import *
import os
import glob
import psycopg2
import pandas as pd

def process_data(cur, conn, filepath, func):
    """
    Process data from a given filepath and execute a given function.
    """
    
    # Get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))
    
    # Get total number of files found        
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))
    
    # Iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed'.format(i, num_files))
    