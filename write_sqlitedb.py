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

dbpath = '/Volumes/Portable Expansion/nick/github/orthophonology/db/toy-orthophon.db'

monobase = time.time() - time.monotonic()
def getMonotonic():
    return int((monobase + time.monotonic()) * 1000)
timestamp = time.perf_counter()

def delete_old_db():
    dbase = pathlib.Path(dbpath)
    if dbase.exists():
        print('Deleting old database...')
        dbase.unlink()

def open_db():
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

    print('Opening database...')
    conn = sqlite3.connect(dbpath)
    c = conn.cursor()

    return conn, c
    # mariadb_connection = mariadb.connect(user='orthophon', passwd='78!^Y#%dn9xk72RU1X2SJM$O6', host='192.168.1.150', port='3306', database='orthophonology')
    # cursor = mariadb_connection.cursor()

def syllable_table():
    import handata

    c.execute('''DROP TABLE IF EXISTS syllables''')
    c.execute(
        '''CREATE TABLE IF NOT EXISTS syllables
        (
            id INTEGER PRIMARY KEY NOT NULL,
            pinyin_tone TEXT UNIQUE NOT NULL,
            pinyin TEXT NOT NULL,
            tone INTEGER NOT NULL
        )
        '''
    )
    conn.commit()

    print('Loading syllable data...')
    syllables = []
    for syllable in handata.syllables:
        pinyin_tone = syllable
        pinyin = pinyin_tone[0:-1]
        tone = int(pinyin_tone[-1])
        syllables.append([pinyin_tone, pinyin, tone])

    print(str(len(syllables)))

    print('Inserting syllables into database...')
    c.executemany('''INSERT OR IGNORE INTO syllables (pinyin_tone, pinyin, tone) VALUES (?, ?, ?)''', syllables)
    conn.commit()

    c.execute('''CREATE INDEX syllables_pinyin_ix ON syllables (pinyin)''')
    c.execute('''CREATE INDEX syllables_tone_ix ON syllables (tone)''')
    conn.commit()

def syllable_comps_table():
    c.execute('''DROP TABLE IF EXISTS syllable_comps''')
    c.execute(
        '''CREATE TABLE IF NOT EXISTS syllable_comps
        (
            id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
            syl1 INTEGER NOT NULL,
            syl2 INTEGER NOT NULL,
            difference REAL NOT NULL,
            FOREIGN KEY (syl1) REFERENCES syllables (id),
            FOREIGN KEY (syl2) REFERENCES syllables (id)
        )'''
    )
    conn.commit()

    print('Getting characters...')
    c.execute('''SELECT id, pinyin_tone FROM syllables''')

    syllables = dict()
    for row in c:
        syllables[row[1]] = row[0]

    syl_distances = pathlib.Path('data/syllable_similarity.csv')
    with syl_distances.open('r', encoding='utf-8', newline='') as csvfile:
        reader = csv.reader(csvfile, lineterminator='\n', delimiter=',')
        print('Opened syllable comparions file')

        csvreader = []
        for row in reader:
            syl1 = syllables[row[0]]
            syl2 = syllables[row[1]]
            difference = row[2]

            if syl2 > syl1:
                csvreader.append([syl1, syl2, difference])
            else:
                csvreader.append([syl2, syl1, difference])

        count = 0
        numrows = len(csvreader)
        print(numrows)
        chunks = int(numrows / 100)
        print('Submitting syllables to database')
        for idx in range(100):
            start = idx * chunks
            if idx == 99:
                stop = numrows
            else:
                stop = (idx * chunks) + chunks

            print(csvreader[start])

            # count += 1
            # if row[0] > row[1]:
            #     cursor.execute('''INSERT IGNORE INTO syllables (syl1, syl2, diff) VALUES (%s, %s, %s)''', (row[1], row[0], row[2]))
            # else:
            #     cursor.execute('''INSERT IGNORE INTO syllables (syl1, syl2, diff) VALUES (%s, %s, %s)''', (row[0], row[1], row[2]))

            c.executemany('''INSERT INTO syllable_comps (syl1, syl2, difference) VALUES (?, ?, ?)''', csvreader[start:stop])

            # cursor.execute('''INSERT IGNORE INTO syllables (syl1, syl2, diff) VALUES (%s, %s, %s)''', (row[0], row[1], row[2]))
            # print('Inserted: {}, {}, {}'.format(row[0], row[1], row[2]))
            # if row[0] != row[1]:
            #     cursor.execute('''INSERT IGNORE INTO syllables (syl1, syl2, diff) VALUES (%s, %s, %s)''', (row[1], row[0], row[2]))
                # print('Inserted: {}, {}, {}'.format(row[1], row[0], row[2]))
            # conn.commit()

            # if stop > 100000 == 0:
            print('Inserted syllable rows up to: {}'.format(stop))

            conn.commit()

            # if count > 500:
            #     break

    c.execute('''CREATE INDEX syllable_comps_difference_ix ON syllable_comps (difference)''')
    conn.commit()

def character_comps_table():
    print('Dropping table...')
    c.execute('''DROP TABLE IF EXISTS character_comps''')
    print('Creating new table...')
    c.execute(
        '''CREATE TABLE IF NOT EXISTS character_comps
        (
            id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
            char1 INTEGER NOT NULL,
            char2 INTEGER NOT NULL,
            similarity REAL NOT NULL,
            FOREIGN KEY (char1) REFERENCES characters (id),
            FOREIGN KEY (char2) REFERENCES characters (id)
        )'''
    )
    conn.commit()

    print('Getting characters...')
    c.execute('''SELECT id, glyph FROM characters''')

    comps = []
    characters = dict()
    for row in c:
        characters[row[1]] = row[0]
        comps.append([row[0], row[0], 1])


    timestamp = time.perf_counter()
    print('Opening character comparisons file...')
    char_distances = pathlib.Path('data/character_comparisons.csv')
    with char_distances.open('r', encoding='utf-8', newline='') as csvfile:
        reader = csv.reader(csvfile, lineterminator='\n', delimiter=',')
        print('Opened character comparions file. Duration: {}'.format(time.perf_counter() - timestamp))

        # csvreader = [[x[0], x[1], float(x[2])] for x in reader]

        for row in reader:
            char1 = characters[row[0]]
            char2 = characters[row[1]]
            sim = float(row[2])

            if char2 > char1:
                comps.append([char1, char2, sim])
            else:
                comps.append([char2, char1, sim])

        count = 0
        numrows = len(comps)
        chunks = int(numrows / 50)
        print('Submitting characters to database')
        for idx in range(50):
            start = idx * chunks
            if idx == 49:
                stop = numrows
            else:
                stop = (idx * chunks) + chunks

            # count += 1
            # print(csvreader[start])
            timestamp = time.perf_counter()
            c.executemany('''INSERT OR IGNORE INTO character_comps (char1, char2, similarity) VALUES (?, ?, ?)''', comps[start:stop])
            print('Executed insert of character rows up to: {}. Duration: {}'.format(stop, (time.perf_counter() - timestamp)))
            conn.commit()

    c.execute('''CREATE INDEX character_comps_similarity_ix ON character_comps (similarity)''')
    conn.commit()
    print('Character comparison table creation finished')

def subtlex_char_table():
    c.execute(
        '''CREATE TABLE IF NOT EXISTS subtlex_characters
        (
            id INTEGER PRIMARY KEY NOT NULL,
            token_count INTEGER NOT NULL,
            tpm REAL NOT NULL,
            log_count REAL NOT NULL,
            contexts INTEGER NOT NULL,
            contexts_percent REAL NOT NULL,
            log_contexts REAL NOT NULL,
            FOREIGN KEY (id) REFERENCES characters (id) ON UPDATE CASCADE
        )'''
    )
    conn.commit()

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
            c.executemany('''INSERT OR IGNORE INTO subtlex_characters (id, token_count, tpm, log_count, contexts, contexts_percent, log_contexts) VALUES ((SELECT id FROM characters WHERE glyph=? LIMIT 1), ?, ?, ?, ?, ?, ?)''', csvreader[start:stop])
            print('Inserted SUBTLEX-CH character rows up to: {}. Duration: {}'.format(stop, (time.perf_counter() - timestamp)))

            conn.commit()

    c.execute('''CREATE INDEX subtlex_characters_log_count_ix ON subtlex_characters (log_count)''')
    conn.commit()
    print('SUBTLEX-CH character corpus table creation finished')

def subtlex_word_table():
    c.execute('''DROP TABLE IF EXISTS subtlex_words''')
    c.execute(
        '''CREATE TABLE IF NOT EXISTS subtlex_words
        (
            id INTEGER PRIMARY KEY NOT NULL,
            word_count INTEGER NOT NULL,
            wpm REAL NOT NULL,
            log_count REAL NOT NULL,
            contexts INTEGER NOT NULL,
            contexts_percent REAL NOT NULL,
            log_contexts REAL NOT NULL,
            FOREIGN KEY (id) REFERENCES words_ortho (id)
        )'''
    )
    conn.commit()

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
                c.execute(
                    '''
                        INSERT OR IGNORE INTO subtlex_words (
                                id, word_count, wpm,
                                log_count, contexts, contexts_percent,
                                log_contexts
                            )
                        VALUES (
                                (SELECT id FROM words_ortho WHERE
                                    char1=(SELECT id FROM characters WHERE glyph=? LIMIT 1)
                                    AND char2 IS NULL LIMIT 1),
                                ?, ?, ?, ?, ?, ?
                            )
                    ''', row)
            else:
                c.execute(
                    '''
                        INSERT OR IGNORE INTO subtlex_words (
                                id, word_count, wpm,
                                log_count, contexts, contexts_percent,
                                log_contexts
                            )
                        VALUES (
                                (SELECT id FROM words_ortho WHERE
                                    char1=(SELECT id FROM characters WHERE glyph=? LIMIT 1)
                                    AND char2=(SELECT id FROM characters WHERE glyph=? LIMIT 1)
                                    LIMIT 1),
                                ?, ?, ?, ?, ?, ?
                            )
                    ''', row)
            # print(row)

            count += 1
            if count % 5000 == 0:
                conn.commit()
                print('Inserted SUBTLEX-CH word rows up to: {}. Duration: {}'.format(count, (time.perf_counter() - timestamp)))
                timestamp = time.perf_counter()

        conn.commit()
        print('Inserted SUBTLEX-CH word rows up to: {}. Duration: {}'.format(count, (time.perf_counter() - timestamp)))

    c.execute('''CREATE INDEX subtlex_words_log_count_ix ON subtlex_words (log_count)''')
    conn.commit()
    print('SUBTLEX-CH word corpus table creation finished')

def create_word_ortho_comps():
    print('Dropping word_ortho_comps...')
    c.execute('''DROP TABLE IF EXISTS word_ortho_comps''')
    print('Creating word_ortho_comps...')
    c.execute(
        '''CREATE TABLE IF NOT EXISTS word_ortho_comps
        (
            id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
            word1 INTEGER NOT NULL,
            word2 INTEGER NOT NULL,
            similarity REAL NOT NULL,
            wpm_diff REAL,
            log_count_diff REAL,
            FOREIGN KEY (word1) REFERENCES words_ortho (id),
            FOREIGN KEY (word2) REFERENCES words_ortho (id)
        )'''
    )
    conn.commit()

    c.execute('''CREATE INDEX word_ortho_comps_similarity_ix ON word_ortho_comps (similarity)''')
    conn.commit()

    print('Import character comparisons to table...')
    c.execute('''
        INSERT INTO word_ortho_comps (word1, word2, similarity)
        SELECT char1, char2, similarity
        FROM character_comps
        LIMIT 1000000
    ''')
    conn.commit()

    print('Creating indexes...')
    c.execute('''CREATE INDEX word_ortho_comps_word1_ix ON word_ortho_comps (word1)''')
    c.execute('''CREATE INDEX word_ortho_comps_word2_ix ON word_ortho_comps (word2)''')
    conn.commit()

def word_ortho_comps():
    print('Creating comparisons dictionary...')
    comparisons = dict()
    for i in range(6000):
        comparisons[i] = dict()
        comparisons[i][i] = 1

    timestamp = time.perf_counter()
    # comparisons = []
    c.execute('''SELECT char1, char2, similarity FROM character_comps''')
    print('Character comps table downloaded Time: {}'.format(time.perf_counter() - timestamp))

    for row in c:
        char1, char2, sim = row
        comparisons[char1][char2] = sim

    print('Sorting comparison keys...')
    comparisonskeys = sorted(list(comparisons.keys()))
    print('Comparisons dictionary created!')

    print('Creating frequency dictionary...')
    freqs = dict()
    # freqs = []
    c.execute('''SELECT id, word_count, wpm, log_count FROM subtlex_words ORDER BY id''')

    for row in c:
        # freqs.append(row)
        word, word_count, wpm, log_count = row
        freqs[word] = {'word_count': word_count, 'wpm': wpm, 'log_count': log_count}
    freqskeys = sorted(list(freqs.keys()))
    # freqs = pd.DataFrame(freqs, columns=['word', 'wpm', 'log_count'])
    # freqs = freqs.set_index(['word'])
    print('Frequency dictionary created!')

    print('Creating words dictionary...')
    words = dict()
    # words = []
    c.execute('''SELECT id, char1, char2 FROM words_ortho WHERE char2 IS NOT NULL ORDER BY id''')

    for row in c:
        wordid, char1, char2 = row

        if char2 is None:
            words[wordid] = [char1]
        else:
            words[wordid] = [char1, char2]

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
    sim_chunk = []
    # sim_chunk_big = []
    timestamp = time.perf_counter()
    for ix, i in enumerate(wordskeys):
        # for j in range(i+1, words_len):
        for j in wordskeys[(ix+1):]:
            # if j not in wordskeys or j not in comparisonskeys:
            #     continue

            # if i <= 5936 and j > 5936:
            #     skipped += words_len - j
            #     break

            if i % 2 == 0 or j % 2 == 0:
                continue

            word1 = words[i]
            word2 = words[j]

            # if len(word1) != len(word2):
            #     skipped += 1
            #     continue

            freq1 = freqs[i]
            freq2 = freqs[j]

            freq_diff = dict()
            freq_diff['word_count'] =   abs(freq1['word_count'] - freq2['word_count'])
            freq_diff['wpm'] =          abs(freq1['wpm'] - freq2['wpm'])
            freq_diff['log_count'] =    abs(freq1['log_count'] - freq2['log_count'])

            if freq_diff['log_count'] > .5:
                skipped += 1
                continue

            sim_sum = 0


            if word2[0] in comparisons[word1[0]].keys():
                sim_sum += comparisons[word1[0]][word2[0]]
            elif word1[0] in comparisons[word2[0]].keys():
                sim_sum += comparisons[word2[0]][word1[0]]
            else:
                continue

            # if len(word1) == 1:
            #     similarity = sim_sum
            # else:
            if word2[1] in comparisons[word1[1]].keys():
                sim_sum += comparisons[word1[1]][word2[1]]
            elif word1[1] in comparisons[word2[1]].keys():
                sim_sum += comparisons[word2[1]][word1[1]]
            else:
                continue

            similarity = sim_sum / 2

            sim_chunk.append([i, j, similarity, freq_diff['wpm'], freq_diff['log_count']])

            count += 1
            running_count += 1

        similarities += sorted(sim_chunk, key=operator.itemgetter(2))
        sim_chunk =[]

        if count > 20000000:
            break

        if running_count > 5000000:
            # similarities = pd.DataFrame(similarities, columns=['word1', 'word2', 'sim'])
            # similarities = similarities.sort_values(by=['sim', 'word1', 'word2'])
            #
            # filestamp = str(int(monobase)) + '-' + str(getMonotonic())
            # similarities.to_csv('/Volumes/Portable Expansion/nick/github/orthophonology/word_orthography/segment' + filestamp + '.csv', header=False, index=False)
            #
            # sims = pd.concat([sims, similarities], ignore_index=True)
            # similarities = []
            similarities = sorted(similarities, key=operator.itemgetter(2))
            print('Computed {} word pair similarities ({} total, {} skipped). Duration: {}'.format(running_count, count, skipped, (time.perf_counter() - timestamp)))
            timestamp = time.perf_counter()

            c.executemany('''INSERT OR IGNORE INTO word_ortho_comps (word1, word2, similarity, wpm_diff, log_count_diff) VALUES (?,?,?,?,?)''', similarities)
            conn.commit()
            print('Submitted rows to database. Duration: {}'.format((time.perf_counter() - timestamp)))
            timestamp = time.perf_counter()
            similarities = []
            running_count = 0

    # similarities = sorted(similarities, key=operator.itemgetter(2))
    print('Computed {} word pair similarities ({} total, {} skipped). Duration: {}'.format(running_count, count, skipped, (time.perf_counter() - timestamp)))
    timestamp = time.perf_counter()

    c.executemany('''INSERT OR IGNORE INTO word_ortho_comps (word1, word2, similarity, wpm_diff, log_count_diff) VALUES (?,?,?,?,?)''', similarities)
    conn.commit()
    print('Submitted rows to database. Duration: {}'.format((time.perf_counter() - timestamp)))
    timestamp = time.perf_counter()
    similarities = []
    running_count = 0

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
            c.execute('''INSERT OR IGNORE INTO characters (glyph) VALUES (?)''', row[0].strip())
            # print('Inserted SUBTLEX-CH character rows up to: {}. Duration: {}'.format(stop, (time.perf_counter() - timestamp)))

        conn.commit()
            # mariadb_connection.commit()

def words_ortho():
    c.execute('''DROP TABLE IF EXISTS words_ortho''')
    c.execute(
        '''CREATE TABLE IF NOT EXISTS words_ortho
        (
            id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
            char1 INTEGER NOT NULL,
            char2 INTEGER NULL,
            FOREIGN KEY (char1) REFERENCES characters (id),
            FOREIGN KEY (char2) REFERENCES characters (id),
            UNIQUE(char1,char2)
        )'''
    )
    conn.commit()

    c.execute('''INSERT INTO words_ortho (id, char1) SELECT ALL id, id FROM characters''')
    conn.commit()

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
        c.executemany('''INSERT INTO words_ortho (char1, char2) VALUES ((SELECT id FROM characters WHERE glyph=? LIMIT 1), (SELECT id FROM characters WHERE glyph=? LIMIT 1))''', words[start:stop])
        print('Inserted SUBTLEX-CH character rows up to: {}. Duration: {}'.format(stop, (time.perf_counter() - timestamp)))

        conn.commit()
    c.execute('''CREATE INDEX words_char1_ix ON words_ortho (char1)''')
    c.execute('''CREATE INDEX words_char2_ix ON words_ortho (char2)''')
    conn.commit()

def remove_nonstandalone():
    c.execute('''DELETE FROM words_ortho WHERE id NOT IN (SELECT id FROM subtlex_words)''')
    conn.commit()

def load_dictionary():
    import handata
    print('Creating dictionary table...')
    c.execute('''DROP TABLE IF EXISTS cedict''')
    c.execute(
        '''CREATE TABLE IF NOT EXISTS cedict
        (
            id INTEGER PRIMARY KEY NOT NULL,
            definitions TEXT NOT NULL,
            FOREIGN KEY (id) REFERENCES subtlex_words (id)
        )'''
    )
    conn.commit()

    print('Creating words_phono table...')
    c.execute('''DROP TABLE IF EXISTS words_phono''')
    c.execute(
        '''CREATE TABLE IF NOT EXISTS words_phono
        (
            id INTEGER PRIMARY KEY NOT NULL,
            syl1 INTEGER NOT NULL,
            syl2 INTEGER NULL,
            FOREIGN KEY (id) REFERENCES subtlex_words (id),
            FOREIGN KEY (syl1) REFERENCES syllables (id),
            FOREIGN KEY (syl2) REFERENCES syllables (id)
        )'''
    )
    conn.commit()

    print('Getting glyphs of subtlex words...')
    c.execute('''
        SELECT subtlex.id, c1.glyph
        FROM subtlex_words AS subtlex
            JOIN words_ortho ON subtlex.id = words_ortho.id
            JOIN characters AS c1 ON words_ortho.char1 = c1.id
            WHERE words_ortho.char2 IS NULL
        ORDER BY subtlex.id ASC
    ''')

    print('Generating dictionary...')
    dictionary = []
    for row in c:
        wordid, ortho = row

        ortho = ortho.strip()

        if ortho not in handata.cedict:
            continue

        syl1 = handata.cedict[ortho]['pinyin'].replace('5', '0').replace('u:', 'v')
        syl2 = None

        definitions = ' | '.join(handata.cedict[ortho]['definitions'])

        dictionary.append([wordid, syl1, syl2, definitions])

    print('Inserting single character words to CEDICT table...')

    for row in dictionary:
        try:
            c.execute('''INSERT INTO words_phono (id, syl1, syl2) VALUES (?, (SELECT id FROM syllables WHERE pinyin_tone=? LIMIT 1), ?)''', row[0:3])
            c.execute('''INSERT INTO cedict (id, definitions) VALUES (?, ?)''', (row[0], row[3]))
        except sqlite3.IntegrityError:
            print(row)
    print('Inserted {} words to dictionary table.'.format(len(dictionary)))

    c.execute('''
        SELECT subtlex.id, c1.glyph, c2.glyph
        FROM subtlex_words AS subtlex
            JOIN words_ortho ON subtlex.id = words_ortho.id
            JOIN characters AS c1 ON words_ortho.char1 = c1.id
            JOIN characters AS c2 ON words_ortho.char2 = c2.id
        ORDER BY subtlex.id ASC
    ''')

    dictionary = []
    for row in c:
        wordid, char1, char2 = row

        ortho = ''.join([char1.strip(), char2.strip()])
        if ortho not in handata.cedict:
            continue

        pinyin = handata.cedict[ortho]['pinyin'].replace('5', '0').replace('u:', 'v').split(' ')
        syl1 = pinyin[0]
        syl2 = pinyin[1]

        definitions = ' | '.join(handata.cedict[ortho]['definitions'])

        dictionary.append([wordid, syl1, syl2, definitions])


    print('Inserting data to CEDICT table...')

    for row in dictionary:
        try:
            c.execute('''INSERT INTO words_phono (id, syl1, syl2) VALUES (?, (SELECT id FROM syllables WHERE pinyin_tone=? LIMIT 1), (SELECT id FROM syllables WHERE pinyin_tone=? LIMIT 1))''', row[0:3])
            c.execute('''INSERT INTO cedict (id, definitions) VALUES (?, ?)''', (row[0], row[3]))
        except sqlite3.IntegrityError:
            print(row)
    print('Inserted {} words to dictionary table.'.format(len(dictionary)))

def create_word_phono_comps():
    print('Dropping word_phono_comps...')
    c.execute('''DROP TABLE IF EXISTS word_phono_comps''')
    print('Creating word_phono_comps...')
    c.execute(
        '''CREATE TABLE IF NOT EXISTS word_phono_comps
        (
            id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
            word1 INTEGER NOT NULL,
            word2 INTEGER NOT NULL,
            difference REAL NOT NULL,
            FOREIGN KEY (word1) REFERENCES words_phono (id),
            FOREIGN KEY (word2) REFERENCES words_phono (id)
        )'''
    )
    conn.commit()

    c.execute('''CREATE INDEX word_phono_comps_difference_ix ON word_phono_comps (difference)''')
    c.execute('''CREATE INDEX word_phono_comps_word1_ix ON word_phono_comps (word1)''')
    c.execute('''CREATE INDEX word_phono_comps_word2_ix ON word_phono_comps (word2)''')
    conn.commit()

def word_phono_comps():
    print('Creating phonological comparison dictionary...')
    comparisons = dict()
    for i in range(2005):
        comparisons[i+1] = dict()
        comparisons[i+1][i+1] = 0

    timestamp = time.perf_counter()
    # comparisons = []
    c.execute('''SELECT syl1, syl2, difference FROM syllable_comps''')
    print('Syllable comps table downloaded Time: {}'.format(time.perf_counter() - timestamp))

    for row in c:
        syl1, syl2, diff = row
        comparisons[syl1][syl2] = diff

    print('Sorting comparison keys...')
    comparisonskeys = sorted(list(comparisons.keys()))
    print('Comparisons dictionary created!')

    print('Creating words dictionary...')
    words = dict()
    # words = []
    c.execute('''SELECT id, syl1, syl2 FROM words_phono ORDER BY id''')

    for row in c:
        wordid, syl1, syl2 = row

        if syl2 is not None:
            words[wordid] = [syl1, syl2]
        else:
            words[wordid] = [syl1]

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

    differences = []
    diff_chunk = []
    timestamp = time.perf_counter()
    for ix, i in enumerate(wordskeys):
        for j in wordskeys[(ix+1):]:

            if i % 2 == 0 or j % 2 == 0:
                continue

            if i <= 5936 and j > 5936:
                skipped += words_len - j
                break

            word1 = words[i]
            word2 = words[j]

            if len(word1) != len(word2):
                continue

            diff_sum = 0

            if word2[0] in comparisons[word1[0]].keys():
                diff_sum += comparisons[word1[0]][word2[0]]
            elif word1[0] in comparisons[word2[0]].keys():
                diff_sum += comparisons[word2[0]][word1[0]]
            else:
                continue

            if len(word1) == 1:
                difference = diff_sum
            else:
                if word2[1] in comparisons[word1[1]].keys():
                    diff_sum += comparisons[word1[1]][word2[1]]
                elif word1[1] in comparisons[word2[1]].keys():
                    diff_sum += comparisons[word2[1]][word1[1]]
                else:
                    continue

                difference = diff_sum / 2

            diff_chunk.append([i, j, difference])
            print(diff_chunk[-1])
            count += 1
            running_count += 1

        differences += sorted(diff_chunk, key=operator.itemgetter(2))
        diff_chunk = []

        if count > 25000000:
            break

        if running_count > 5000000:
            differences = sorted(differences, key=operator.itemgetter(2))
            print('Computed {} word pair phonological differences ({} total, {} skipped). Duration: {}'.format(running_count, count, skipped, (time.perf_counter() - timestamp)))
            timestamp = time.perf_counter()

            c.executemany('''INSERT INTO word_phono_comps (word1, word2, difference) VALUES (?,?,?)''', differences)
            conn.commit()
            print('Submitted rows to database. Duration: {}'.format((time.perf_counter() - timestamp)))
            timestamp = time.perf_counter()
            differences = []
            running_count = 0

    differences = sorted(differences, key=operator.itemgetter(2))
    print('Computed {} word pair phonological differences ({} total, {} skipped). Duration: {}'.format(running_count, count, skipped, (time.perf_counter() - timestamp)))
    timestamp = time.perf_counter()

    c.executemany('''INSERT INTO word_phono_comps (word1, word2, difference) VALUES (?,?,?)''', differences)
    conn.commit()
    print('Submitted rows to database. Duration: {}'.format((time.perf_counter() - timestamp)))
    timestamp = time.perf_counter()
    differences = []
    running_count = 0

delete_old_db()
conn, c = open_db()
characters_table()
conn.commit()

words_ortho()
conn.commit()

subtlex_char_table()
subtlex_word_table()
remove_nonstandalone()
conn.commit()

character_comps_table()
conn.commit()

syllable_table()
syllable_comps_table()
conn.commit()

create_word_ortho_comps()
# create_word_ortho_comps_temp()
word_ortho_comps()
conn.commit()

load_dictionary()
create_word_phono_comps()
word_phono_comps()

conn.commit()
conn.close()
