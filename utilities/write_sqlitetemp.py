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

dbase = pathlib.Path(dbpath)
if not dbase.exists():
    print('Creating new database...')
    conn = sqlite3.connect(dbpath)
    c = conn.cursor()
    c.executescript('''
        PRAGMA page_size=4096;
        PRAGMA cache_size=5000;
        PRAGMA locking_mode=EXCLUSIVE;
        PRAGMA synchronous=OFF;
        PRAGMA temp_store=MEMORY;
    ''')
    conn.commit()
    conn.close()

timestamp = time.perf_counter()

print('Opening database...')
conn = sqlite3.connect(dbpath)
c = conn.cursor()

print('Creating table...')
c.execute('''DROP TABLE IF EXISTS word_ortho_comps_temp''')
c.execute(
    '''CREATE TABLE IF NOT EXISTS word_ortho_comps_temp
    (
        word1 INT NOT NULL,
        word2 INT NOT NULL,
        sim REAL NOT NULL
    )'''
)
# c.execute('''CREATE INDEX ix_sim ON word_ortho_comps_temp (sim)''')
conn.commit()

print('Acquiring file names...')
p = pathlib.Path('/Volumes/Portable Expansion/nick/github/orthophonology/word_orthography/session5/')

paths = []
for x in p.iterdir():
    if x.is_file() and '.csv' in x.suffixes:
        paths.append(x.name)
total = len(paths)

count = 0
runtime = 0

print('Starting data insertion...')
for path in paths:
    timestamp = time.perf_counter()
    p = pathlib.Path('/Volumes/Portable Expansion/nick/github/orthophonology/word_orthography/session5/{}'.format(path))
    with p.open('r', encoding='utf-8', newline='') as csvfile:
        csvreader = csv.reader(csvfile, lineterminator='\n', delimiter=',')

        for row in csvreader:
            c.execute('''INSERT INTO word_ortho_comps_temp VALUES (?, ?, ?)''', row)

        conn.commit()

    count += 1
    process_time = time.perf_counter() - timestamp
    runtime += process_time
    avg_time = runtime/count
    est_remaining = (total - count) * avg_time
    print('File "{}" processed.\tDuration: {}'.format(path, process_time))
    print('Total processed: {} ({:.2%}) in {:0>2}:{:0>2}:{:0>2} (Avg: {:.1f}s / Est. remaining: {:0>2}:{:0>2}:{:0>2})'.format(count, (count/total), int(runtime/3600), int((runtime%3600)/60), int(runtime%60), avg_time, int(est_remaining/3600), int((est_remaining%3600)/60), int(est_remaining%60)))

conn.commit()
conn.close()
