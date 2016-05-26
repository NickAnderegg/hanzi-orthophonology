import sqlite3
import csv
import pathlib
import json
# import mysql.connector as mariadb
import time
import random
import operator
import numpy as np
import pandas as pd
random.seed()

dbpath = '/Volumes/research/orthophonology/db/orthophon.db'

monobase = time.time() - time.monotonic()
def getMonotonic():
    return int((monobase + time.monotonic()) * 1000)

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
# mariadb_connection = mariadb.connect(user='orthophon', passwd='78!^Y#%dn9xk72RU1X2SJM$O6', host='192.168.1.150', port='3306', database='orthophonology')
# cursor = mariadb_connection.cursor()

def syllable_table():
    cursor.execute('''DROP TABLE IF EXISTS syllables''')
    cursor.execute(
        '''CREATE TABLE IF NOT EXISTS syllables
        (
            syl1 VARCHAR(15) CHARACTER SET utf8 NOT NULL,
            syl2 VARCHAR(15) CHARACTER SET utf8 NOT NULL,
            diff DOUBLE UNSIGNED NOT NULL,
            CONSTRAINT pk_syllables PRIMARY KEY (syl1, syl2),
            CONSTRAINT uq_reverse UNIQUE (syl2, syl1)
        ) CHARACTER SET = utf8'''
    )

    mariadb_connection.commit()

    syl_distances = pathlib.Path('data/syllable_similarity.csv')
    with syl_distances.open('r', encoding='utf-8', newline='') as csvfile:
        csvreader = csv.reader(csvfile, lineterminator='\n', delimiter=',')
        print('Opened syllable comparions file')

        csvreader = list(csvreader)
        count = 0
        numrows = len(csvreader)
        chunks = int(numrows / 100)
        print('Submitting syllables to database')
        for idx in range(100):
            start = idx * chunks
            if idx == 99:
                stop = numrows
            else:
                stop = (idx * chunks) + chunks

            # count += 1
            # if row[0] > row[1]:
            #     cursor.execute('''INSERT IGNORE INTO syllables (syl1, syl2, diff) VALUES (%s, %s, %s)''', (row[1], row[0], row[2]))
            # else:
            #     cursor.execute('''INSERT IGNORE INTO syllables (syl1, syl2, diff) VALUES (%s, %s, %s)''', (row[0], row[1], row[2]))

            cursor.executemany('''INSERT IGNORE INTO syllables (syl1, syl2, diff) VALUES (%s, %s, %s)''', csvreader[start:stop])

            # cursor.execute('''INSERT IGNORE INTO syllables (syl1, syl2, diff) VALUES (%s, %s, %s)''', (row[0], row[1], row[2]))
            # print('Inserted: {}, {}, {}'.format(row[0], row[1], row[2]))
            # if row[0] != row[1]:
            #     cursor.execute('''INSERT IGNORE INTO syllables (syl1, syl2, diff) VALUES (%s, %s, %s)''', (row[1], row[0], row[2]))
                # print('Inserted: {}, {}, {}'.format(row[1], row[0], row[2]))
            # conn.commit()

            # if stop > 100000 == 0:
            print('Inserted syllable rows up to: {}'.format(stop))

            mariadb_connection.commit()

            # if count > 500:
            #     break

    mariadb_connection.commit()

def character_comps_table():
    cursor.execute('''DROP TABLE IF EXISTS character_comps''')
    cursor.execute(
        '''CREATE TABLE IF NOT EXISTS character_comps
        (
            id INT UNSIGNED AUTO_INCREMENT NOT NULL PRIMARY KEY,
            char1 INT UNSIGNED NOT NULL,
            char2 INT UNSIGNED NOT NULL,
            sim DOUBLE UNSIGNED NOT NULL,
            FOREIGN KEY (char1) REFERENCES characters (id) ON UPDATE CASCADE,
            FOREIGN KEY (char2) REFERENCES characters (id) ON UPDATE CASCADE
        ) CHARACTER SET = utf8'''
    )

    mariadb_connection.commit()

    timestamp = time.perf_counter()
    print('Opening character comparisons file...')
    char_distances = pathlib.Path('data/character_comparisons.csv')
    with char_distances.open('r', encoding='utf-8', newline='') as csvfile:
        reader = csv.reader(csvfile, lineterminator='\n', delimiter=',')
        print('Opened character comparions file. Duration: {}'.format(time.perf_counter() - timestamp))

        csvreader = [[x[0], x[1], float(x[2])] for x in reader]
        count = 0
        numrows = len(csvreader)
        chunks = int(numrows / 5000)
        print('Submitting characters to database')
        for idx in range(5000):
            start = idx * chunks
            if idx == 4999:
                stop = numrows
            else:
                stop = (idx * chunks) + chunks

            # count += 1
            print(csvreader[start])
            timestamp = time.perf_counter()
            cursor.executemany('''INSERT IGNORE INTO character_comps (char1, char2, sim) VALUES ((SELECT id FROM characters WHERE glyph=%s LIMIT 1), (SELECT id FROM characters WHERE glyph=%s LIMIT 1), %s)''', csvreader[start:stop])
            print('Executed insert of character rows up to: {}. Duration: {}'.format(stop, (time.perf_counter() - timestamp)))

            timestamp = time.perf_counter()
            mariadb_connection.commit()
            print('Executed commit of character rows up to: {}. Duration: {}'.format(stop, (time.perf_counter() - timestamp)))

            # c.execute('''INSERT OR IGNORE INTO characters VALUES (?, ?, ?)''', (row[0], row[1], row[2]))
            # # print('Inserted: {}, {}, {}'.format(row[0], row[1], row[2]))
            # if row[0] != row[1]:
            #     c.execute('''INSERT OR IGNORE INTO characters VALUES (?, ?, ?)''', (row[1], row[0], row[2]))
            #     # print('Inserted: {}, {}, {}'.format(row[1], row[0], row[2]))
            #
            # if count % 250000 == 0:
            #     print('Inserted character rows up to: {}'.format(count))
            #     conn.commit()

    mariadb_connection.commit()
    print('Character comparison table creation finished')

def subtlex_char_table():
    c.execute(
        '''CREATE TABLE IF NOT EXISTS subtlex_char
        (
            token text PRIMARY KEY NOT NULL,
            token_count int NOT NULL,
            tpm real NOT NULL,
            log_count real NOT NULL,
            contexts int NOT NULL,
            contexts_percent real NOT NULL,
            log_contexts real NOT NULL
        )'''
    )
    conn.commit()

    # cursor.execute('''DROP TABLE IF EXISTS subtlex_char''')
    # cursor.execute(
    #     '''CREATE TABLE IF NOT EXISTS subtlex_char
    #     (
    #         token INT UNSIGNED UNIQUE NOT NULL,
    #         token_count INT UNSIGNED NOT NULL,
    #         tpm DOUBLE UNSIGNED NOT NULL,
    #         log_count DOUBLE UNSIGNED NOT NULL,
    #         contexts INT UNSIGNED NOT NULL,
    #         contexts_percent DOUBLE UNSIGNED NOT NULL,
    #         log_contexts DOUBLE UNSIGNED NOT NULL
    #     ) CHARACTER SET = utf8'''
    # )
    # mariadb_connection.commit()

    timestamp = time.perf_counter()
    subtlex_char = pathlib.Path('data/subtlex-ch/SUBTLEX-CH-CHR.csv')
    with subtlex_char.open('r', encoding='utf-8', newline='') as csvfile:
        csvreader = csv.reader(csvfile, lineterminator='\n', delimiter='\t')
        print('Opened SUBTLEX-CH character file. Duration: {}'.format(time.perf_counter() - timestamp))

        csvreader = list(csvreader)
        count = 0
        numrows = len(csvreader)
        chunks = int(numrows / 10)
        print('Submitting characters to database')
        for idx in range(10):
            start = (idx * chunks) + 3
            if idx == 9:
                stop = numrows
            else:
                stop = (idx * chunks) + chunks + 3

            timestamp = time.perf_counter()
            cursor.executemany('''INSERT IGNORE INTO subtlex_char (token, token_count, tpm, log_count, contexts, contexts_percent, log_contexts) VALUES ((SELECT id FROM characters WHERE glyph=%s LIMIT 1), %s, %s, %s, %s, %s, %s)''', csvreader[start:stop])
            print('Inserted SUBTLEX-CH character rows up to: {}. Duration: {}'.format(stop, (time.perf_counter() - timestamp)))

            mariadb_connection.commit()

        # count = 0
        # for row in csvreader:
            # if count < 3:
            #     count += 1
            #     continue

    #         c.execute('''INSERT OR IGNORE INTO subtlex_char VALUES (?, ?, ?, ?, ?, ?, ?)''', tuple(row))
    #         # print('Inserted: {}, {}, {}'.format(row[0], row[1], row[2]))
    #
    #         count += 1
    #         if count % 250000 == 0:
    #             print('Inserted character corpus rows up to: {}'.format(count))
    #             conn.commit()
    #
    # conn.commit()
    print('SUBTLEX-CH character corpus table creation finished')

def subtlex_word_table():
    # c.execute('''DROP TABLE IF EXISTS subtlex_word''')
    # c.execute(
    #     '''CREATE TABLE IF NOT EXISTS subtlex_word
    #     (
    #         word text NOT NULL,
    #         word_count int NOT NULL,
    #         wpm real NOT NULL,
    #         log_count real NOT NULL,
    #         contexts int NOT NULL,
    #         contexts_percent real NOT NULL,
    #         log_contexts real NOT NULL,
    #         PRIMARY KEY(word)
    #     )'''
    # )
    #
    # conn.commit()

    cursor.execute('''DROP TABLE IF EXISTS subtlex_word''')
    cursor.execute(
        '''CREATE TABLE IF NOT EXISTS subtlex_word
        (
            word INT UNSIGNED UNIQUE NOT NULL,
            word_count INT UNSIGNED NOT NULL,
            wpm DOUBLE UNSIGNED NOT NULL,
            log_count DOUBLE UNSIGNED NOT NULL,
            contexts INT UNSIGNED NOT NULL,
            contexts_percent DOUBLE UNSIGNED NOT NULL,
            log_contexts DOUBLE UNSIGNED NOT NULL
        ) CHARACTER SET = utf8'''
    )
    mariadb_connection.commit()

    timestamp = time.perf_counter()
    subtlex_char = pathlib.Path('data/subtlex-ch/SUBTLEX-CH-WF.csv')
    with subtlex_char.open('r', encoding='utf-8', newline='') as csvfile:
        csvreader = csv.reader(csvfile, lineterminator='\n', delimiter='\t')
        print('Opened SUBTLEX-CH word file. Duration: {}'.format(time.perf_counter() - timestamp))

        csvreader = list(csvreader)

        count = 0
        skipped = 0
        words = []
        for row in csvreader:
            if len(row[0]) > 2:
                # if skipped < 100:
                #     print('Skipping: {}'.format(row[0]))
                #     skipped += 1
                continue
            elif len(row[0]) == 2:
                words.append(row)
                words[-1].insert(1, list(row[0])[1])
                words[-1][0] = list(row[0])[0]
                # if count < 100:
                #     print('Appended: {}'.format(words[-1]))
                #     count += 1
            else:
                words.append(row)
                words[-1].insert(1, None)
                # if count < 100:
                #     print('Appended: {}'.format(words[-1]))
                #     count += 1

        count = 0
        numrows = len(words)
        print('Total words to insert: {}'.format(numrows))

        chunks = int(numrows / 100)
        print('Submitting SUBTLEX words to database')
        timestamp = time.perf_counter()
        for row in words:
            # start = (idx * chunks) + 3
            # if idx == 99:
            #     stop = numrows
            # else:
            #     stop = (idx * chunks) + chunks + 3

            if row[1] is None:
                row[1:2] = []
                cursor.execute(
                    '''
                        INSERT IGNORE INTO subtlex_word (
                                word, word_count, wpm,
                                log_count, contexts, contexts_percent,
                                log_contexts
                            )
                        VALUES (
                                (SELECT id FROM words WHERE
                                    char1=(SELECT id FROM characters WHERE glyph=%s LIMIT 1)
                                    AND char2 IS NULL LIMIT 1),
                                %s, %s, %s, %s, %s, %s
                            )
                    ''', row)
            else:
                cursor.execute(
                    '''
                        INSERT IGNORE INTO subtlex_word (
                                word, word_count, wpm,
                                log_count, contexts, contexts_percent,
                                log_contexts
                            )
                        VALUES (
                                (SELECT id FROM words WHERE
                                    char1=(SELECT id FROM characters WHERE glyph=%s LIMIT 1)
                                    AND char2=(SELECT id FROM characters WHERE glyph=%s LIMIT 1)
                                    LIMIT 1),
                                %s, %s, %s, %s, %s, %s
                            )
                    ''', row)
            # print(row)

            count += 1
            if count % 5000 == 0:
                mariadb_connection.commit()
                print('Inserted SUBTLEX-CH word rows up to: {}. Duration: {}'.format(count, (time.perf_counter() - timestamp)))
                timestamp = time.perf_counter()

        mariadb_connection.commit()
        print('Inserted SUBTLEX-CH word rows up to: {}. Duration: {}'.format(count, (time.perf_counter() - timestamp)))
    #     count = 0
    #     for row in csvreader:
    #         if count < 3:
    #             count += 1
    #             continue
    #
    #         c.execute('''INSERT OR IGNORE INTO subtlex_word VALUES (?, ?, ?, ?, ?, ?, ?)''', tuple(row))
    #         # print('Inserted: {}, {}, {}'.format(row[0], row[1], row[2]))
    #
    #         count += 1
    #         if count % 250000 == 0:
    #             print('Inserted word corpus rows up to: {}'.format(count))
    #             conn.commit()
    #
    # conn.commit()
    print('SUBTLEX-CH wprd corpus table creation finished')

def create_word_orthography():
    cursor.execute('''DROP TABLE IF EXISTS word_ortho_comps_twochar_freq50''')
    cursor.execute(
        '''CREATE TABLE IF NOT EXISTS word_ortho_comps_twochar_freq50
        (
            word1 INT UNSIGNED NOT NULL,
            word2 INT UNSIGNED NOT NULL,
            sim DOUBLE UNSIGNED NOT NULL,
            PRIMARY KEY(word1, word2),
            KEY(sim)
        )'''
    )


    # FOREIGN KEY (word1) REFERENCES words (id) ON UPDATE CASCADE,
    # FOREIGN KEY (word2) REFERENCES words (id) ON UPDATE CASCADE,
    # simx DOUBLE UNSIGNED NOT NULL DEFAULT 0
    # cursor.execute('''ALTER TABLE word_ortho_comps_twochar DISABLE KEYS''')
    # cursor.execute('''SET @@session.unique_checks = 0; SET @@session.foreign_key_checks = 0;''', multi=True)

    mariadb_connection.commit()

def create_word_orthography_sqlite():
    c.execute('''DROP TABLE IF EXISTS word_ortho_comps''')
    c.execute(
        '''CREATE TABLE IF NOT EXISTS word_ortho_comps
        (
            word1 INT NOT NULL,
            word2 INT NOT NULL,
            sim REAL NOT NULL,
            PRIMARY KEY(word1, word2),
            FOREIGN KEY (word1) REFERENCES
        )'''
    )

    conn.commit()

def word_orthography():
    # cursor.execute('''DROP TABLE IF EXISTS word_ortho_comps_twochar''')
    # cursor.execute(
    #     '''CREATE TABLE IF NOT EXISTS word_ortho_comps_twochar
    #     (
    #         word1 INT UNSIGNED NOT NULL,
    #         word2 INT UNSIGNED NOT NULL,
    #         sim DOUBLE UNSIGNED NOT NULL,
    #         FOREIGN KEY (word1) REFERENCES words (id) ON UPDATE CASCADE,
    #         FOREIGN KEY (word2) REFERENCES words (id) ON UPDATE CASCADE
    #     )'''
    # )
    # cursor.execute('''ALTER TABLE word_ortho_comps_twochar DISABLE KEYS''')
    # # cursor.execute('''SET @@session.unique_checks = 0; SET @@session.foreign_key_checks = 0;''', multi=True)
    #
    # mariadb_connection.commit()


    print('Creating comparisons dictionary...')
    comparisons = dict()
    for i in range(6000):
        comparisons[i] = dict()

    timestamp = time.perf_counter()
    # comparisons = []
    cursor.execute('''SELECT char1, char2, sim FROM character_comps''')
    print('Character comps table downloaded Time: {}'.format(time.perf_counter() - timestamp))

    # idx = len(comparisons)
    for row in cursor.fetchall():
        char1, char2, sim = row
        # comparisons.append(row) #(str(char1), str(char2), sim)
        comparisons[char1][char2] = sim
        # if random.randint(1,50) == 5:
            # char1, char2, sim = row
            # comparisons['char1'] = comparisons['char1'].astype(int)
            # comparisons['char2'] = comparisons['char2'].astype(int)
            # print('Char1: {}, Char2: {}, Sim: {}'.format(char1, char2, sim))
            # print(comparisons.loc[idx])
        # idx += 1

    # print('Converting to DataFrame... Time: {}'.format(time.perf_counter() - timestamp))
    # comparisons = pd.DataFrame(comparisons, columns=['char1', 'char2', 'sim'])
    # print('Adjusting column types... Time: {}'.format(time.perf_counter() - timestamp))
    # comparisons['char1'] = comparisons['char1'].astype(int)
    # comparisons['char2'] = comparisons['char2'].astype(int)
    # print('Setting indices... Time: {}'.format(time.perf_counter() - timestamp))
    # comparisons = comparisons.set_index(['char1', 'char2'])
    # print('Time: {}'.format(time.perf_counter() - timestamp))

    print('Sorting comparison keys...')
    comparisonskeys = sorted(list(comparisons.keys()))
    # comparisonskeys = list(comparisons.index.levels[1])
    print('Comparisons dictionary created!')

    print('Creating words dictionary...')
    freqs = dict()
    # freqs = []
    cursor.execute('''SELECT word, wpm, log_count FROM subtlex_word ORDER BY word''')

    for row in cursor.fetchall():
        # freqs.append(row)
        word, wpm, log_count = row
        freqs[word] = {'wpm': wpm, 'log_count': log_count}
    freqskeys = sorted(list(freqs.keys()))
    # freqs = pd.DataFrame(freqs, columns=['word', 'wpm', 'log_count'])
    # freqs = freqs.set_index(['word'])
    print('Frequency dictionary created!')

    print('Creating frequency dictionary...')
    words = dict()
    # words = []
    cursor.execute('''SELECT id, char1, char2 FROM words WHERE char2 IS NOT NULL ORDER BY id''')

    for row in cursor.fetchall():
        wordid, char1, char2 = row
        words[wordid] = [char1, char2]
        # words.append(row)
    wordskeys = sorted(list(words.keys()))
    # words = pd.DataFrame(words, columns=['id', 'char1', 'char2'])
    # words = words.set_index(['id'])
    # wordskeys = list(words.index)
    print('Words dictionary created!')

    count = 0
    skipped = 0
    running_count = 0
    words_len = max(wordskeys)
    print(words_len)
    print('Number of words to process: {}'.format(len(wordskeys)))

    # for i in range(5937, words_len):
    #     if i not in wordskeys or i not in comparisonskeys:
    #         continue
    # similarities = pd.DataFrame(columns=['char1', 'char2', 'sim'])
    similarities = []
    sims = pd.DataFrame(columns=['char1', 'char2', 'sim'])
    # sim_chunk_sm = []
    # sim_chunk_big = []
    timestamp = time.perf_counter()
    for ix, i in enumerate(wordskeys):
        # for j in range(i+1, words_len):
        for j in wordskeys[(ix+1):]:
            # if j not in wordskeys or j not in comparisonskeys:
            #     continue

            word1 = words[i]
            word2 = words[j]

            word1freq = freqs[i]['log_count']
            word2freq = freqs[j]['log_count']

            if max(word1freq, word2freq) - min(word1freq, word2freq) > .75:
                skipped += 1
                continue

            sim_sum = 0

            # if (word1['char1'], word2['char1']) in comparisons.index:
            #     sim_sum += comparisons.loc[word1['char1'], word2['char1']]['sim']
            # elif (word2['char1'], word1['char1']) in comparisons.index:
            #     sim_sum += comparisons.loc[word2['char1'], word1['char1']]['sim']
            # else:
            #     skipped += 1
            #     continue
            #
            # if (word1['char2'], word2['char2']) in comparisons.index:
            #     sim_sum += comparisons.loc[word1['char2'], word2['char2']]['sim']
            # elif (word2['char2'], word1['char2']) in comparisons.index:
            #     sim_sum += comparisons.loc[word2['char2'], word1['char2']]['sim']
            # else:
            #     skipped += 1
            #     continue

            if word2[0] in comparisons[word1[0]].keys():
                sim_sum += comparisons[word1[0]][word2[0]]
            elif word1[0] in comparisons[word2[0]].keys():
                sim_sum += comparisons[word2[0]][word1[0]]
            else:
                continue

            if word2[1] in comparisons[word1[1]].keys():
                sim_sum += comparisons[word1[1]][word2[1]]
            elif word1[1] in comparisons[word2[1]].keys():
                sim_sum += comparisons[word2[1]][word1[1]]
            else:
                continue

            similarity = sim_sum / 2
            # similarities.loc[count] = [i, j, similarity]
            similarities.append([i, j, similarity])
            count += 1
            running_count += 1

        # sim_chunk_big += sorted(sim_chunk_sm, key=operator.itemgetter(2))
        # sim_chunk_sm = []
        # if count > 10000000:
        #     break
        if running_count > 5000000:
            similarities = pd.DataFrame(similarities, columns=['char1', 'char2', 'sim'])
            similarities = similarities.sort_values(by=['sim', 'char1', 'char2'])

            filestamp = str(int(monobase)) + '-' + str(getMonotonic())
            similarities.to_csv('/Volumes/Portable Expansion/nick/github/orthophonology/word_orthography/segment' + filestamp + '.csv', header=False, index=False)

            sims = pd.concat([sims, similarities], ignore_index=True)
            similarities = []
            print('Computed {} word pair similarities ({} total, {} skipped). Duration: {}'.format(running_count, count, skipped, (time.perf_counter() - timestamp)))
            timestamp = time.perf_counter()

            running_count = 0
            # similarities = sorted(sim_chunk_big, key=operator.itemgetter(2))
            # sim_chunk_big = []
            # sims = pd.DataFrame(sim_chunk_big, columns=['char1', 'char2', 'sim'])
            # sim_chunk_big = []
            # sims['char1'] = sims['char1'].astype(int)
            # sims['char2'] = sims['char2'].astype(int)

            # print('Write time: {}'.format(time.perf_counter() - timestamp))
            # timestamp = time.perf_counter()
            # cursor.executemany('''INSERT IGNORE INTO word_ortho_comps_twochar_freq50 VALUES (%s,%s,%s)''', similarities)
            # mariadb_connection.commit()
            # print('Submitted rows to database. Duration: {}'.format((time.perf_counter() - timestamp)))
            # timestamp = time.perf_counter()

    similarities = pd.DataFrame(similarities, columns=['char1', 'char2', 'sim'])
    similarities = similarities.sort_values(by=['sim', 'char1', 'char2'])

    filestamp = str(int(monobase)) + '-' + str(getMonotonic())
    similarities.to_csv('/Volumes/Portable Expansion/nick/github/orthophonology/word_orthography/segment' + filestamp + '.csv', header=False, index=False)

    sims = pd.concat([sims, similarities], ignore_index=True)
    similarities = []
    print('Computed {} word pair similarities ({} total, {} skipped). Duration: {}'.format(running_count, count, skipped, (time.perf_counter() - timestamp)))
    timestamp = time.perf_counter()

    # sim_chunk_big = []
    # print('Converting to DataFrame')
    # sims = pd.DataFrame(similarities, columns=['char1', 'char2', 'sim'])
    # sims['char1'] = sims['char1'].astype(int)
    # sims['char2'] = sims['char2'].astype(int)
    print('Sorting similarity values. Time: {}'.format(time.perf_counter() - timestamp))
    similarities = None
    sims = sims.sort_values(by=['sim', 'char1', 'char2'])
    print(sims)
    print('Sorted DataFrame. Time: {}'.format(time.perf_counter() - timestamp))
    # sims = sorted(similarities, key=operator.itemgetter(2))
    # similarities = []
    print('Outputting values to file...')
    timestamp = time.perf_counter()
    filestamp = str(int(monobase)) + '-' + str(getMonotonic())
    sims.to_csv('/Volumes/Portable Expansion/nick/github/orthophonology/word_orthography/full' + filestamp + '.csv', header=False, index=False)
    print('Write time: {}'.format(time.perf_counter() - timestamp))
    # with output_file.open('w', encoding='utf-8', newline='') as csvfile:
    #     csvwriter = csv.writer(csvfile, lineterminator='\n')
    #     csvwriter.writerows(sims)

    # print('Computed {} word pair similarities ({} total). Duration: {}'.format(running_count, count, (time.perf_counter() - timestamp)))

def characters_table():
    c.execute('''DROP TABLE IF EXISTS characters''')
    c.execute(
        '''CREATE TABLE IF NOT EXISTS characters
        (
            id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
            glyph TEXT NOT NULL UNIQUE
        )'''
    )
    conn.commit()

    # cursor.execute('''DROP TABLE IF EXISTS characters''')
    # cursor.execute(
    #     '''CREATE TABLE IF NOT EXISTS characters
    #     (
    #         id INT UNSIGNED AUTO_INCREMENT NOT NULL PRIMARY KEY,
    #         glyph VARCHAR(1) CHARACTER SET utf8 NOT NULL
    #     ) CHARACTER SET = utf8'''
    # )
    # mariadb_connection.commit()

    timestamp = time.perf_counter()
    subtlex_char = pathlib.Path('data/subtlex-ch/SUBTLEX-CH-CHR.csv')
    with subtlex_char.open('r', encoding='utf-8', newline='') as csvfile:
        csvreader = csv.reader(csvfile, lineterminator='\n', delimiter='\t')
        print('Opened SUBTLEX-CH character file. Duration: {}'.format(time.perf_counter() - timestamp))

        print('Submitting characters to database')
        count = 0
        for row in csvreader:
            if count < 3:
                count += 1
                continue

            # timestamp = time.perf_counter()
            # cursor.execute('''INSERT IGNORE INTO characters SET glyph=%(glyph)s''', {'glyph': row[0].strip()})
            c.execute('''INSERT OR IGNORE INTO characters (glyph) VALUES %(glyph)s''', {'glyph': row[0].strip()})
            # print('Inserted SUBTLEX-CH character rows up to: {}. Duration: {}'.format(stop, (time.perf_counter() - timestamp)))

        conn.commit()
            # mariadb_connection.commit()

def exploded_words():
    # cursor.execute('''DROP TABLE IF EXISTS words''')
    # cursor.execute(
    #     '''CREATE TABLE IF NOT EXISTS words
    #     (
    #         id INT UNSIGNED AUTO_INCREMENT NOT NULL PRIMARY KEY,
    #         char1 INT UNSIGNED NOT NULL,
    #         char2 INT UNSIGNED NULL,
    #         FOREIGN KEY (char1) REFERENCES characters (id) ON UPDATE CASCADE,
    #         FOREIGN KEY (char2) REFERENCES characters (id) ON UPDATE CASCADE
    #     ) CHARACTER SET = utf8'''
    # )
    # mariadb_connection.commit()
    # return

    words = []
    subtlex_words = pathlib.Path('data/subtlex-ch/SUBTLEX-CH-WF.csv')
    with subtlex_words.open('r', encoding='utf-8', newline='') as csvfile:
        csvreader = csv.reader(csvfile, lineterminator='\n', delimiter='\t')
        print('Opened SUBTLEX-CH word file.')

        count = 0
        skipped = 0
        for row in csvreader:
            word = list(row[0])
            if len(word) != 2:
                skipped += 1
                continue
            else:
                words.append(word)
                count += 1

    count = 0
    numwords = len(words)
    chunks = int(numwords / 50)
    print('Submitting {} characters to database'.format(numwords))
    for idx in range(50):
        start = (idx * chunks)
        if idx == 49:
            stop = numwords
        else:
            stop = (idx * chunks) + chunks

        timestamp = time.perf_counter()
        cursor.executemany('''INSERT IGNORE INTO words (char1, char2) VALUES ((SELECT id FROM characters WHERE glyph=%s LIMIT 1), (SELECT id FROM characters WHERE glyph=%s LIMIT 1))''', words[start:stop])
        print('Inserted SUBTLEX-CH character rows up to: {}. Duration: {}'.format(stop, (time.perf_counter() - timestamp)))

        mariadb_connection.commit()


# syllable_table()
# character_comps_table()
# #
# # c.execute('''SELECT char1, char2, diff FROM characters WHERE diff > .95''')
# # print(json.dumps(c.fetchall(), indent=4))
#
# subtlex_char_table()
# subtlex_word_table()

characters_table()
# exploded_words()

# create_word_orthography()
# word_orthography()
#
# mariadb_connection.commit()
# mariadb_connection.close()

conn.commit()
conn.close()
