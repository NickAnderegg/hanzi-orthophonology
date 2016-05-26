import sqlite3
import numpy as np
import pandas as pd
import csv
import time
import pathlib

monobase = time.time() - time.monotonic()
def getMonotonic():
    return int((monobase + time.monotonic()) * 1000)

dbpath = '/Volumes/Portable Expansion/nick/github/orthophonology/word_orthography.db'
print('Opening database...')
conn = sqlite3.connect(dbpath)
c = conn.cursor()

c.execute('''PRAGMA temp_store = 1''')
c.execute('''PRAGMA temp_store_directory = "/Volumes/Portable Expansion/nick/github/orthophonology/tmp/"''')
conn.commit()

print('Reading table...')
c.execute('''SELECT word1, word2, sim FROM word_ortho_comps_temp ORDER BY sim, word1, word2''')

count = 0
running_count = 0
runtime = 0
timestamp = time.perf_counter()

print('Opening CSV file...')
p = pathlib.Path('/Volumes/Portable Expansion/nick/github/orthophonology/word_orthography.csv')
with p.open('w', encoding='utf-8', newline='') as csvfile:
    csvwriter = csv.writer(csvfile, lineterminator='\n', delimiter=',')

    print('Writing output to file...')

    for row in c:
        csvwriter.writerow(row)
        count += 1
        running_count += 1

        if count % 1000000 == 0:
            process_time = time.perf_counter() - timestamp
            runtime += process_time
            avg_time = runtime/(count/1000000)

            print('{} rows processed.\tDuration: {}'.format(running_count, process_time))
            print('Total processed: {} in {:0>2}:{:0>2}:{:0>2} (Avg: {:.1f}s)'.format(count, int(runtime/3600), int((runtime%3600)/60), int(runtime%60), avg_time))
            timestamp = time.perf_counter()
            running_count = 0
