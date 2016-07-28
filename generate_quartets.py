import sqlite3
import pathlib
import numpy as np
import pandas as pd
import box_muller
# import handata
from collections import deque
import time
import random
from sklearn import preprocessing
import csv
import warnings
import decimal
from decimal import Decimal

decimal.getcontext().prec = 6
random.seed()

# rand_dist = []
# def next_rand(bound=51191):
#     return random.randint(0, bound)
#
#     global rand_dist
#     if len(rand_dist) == 0:
#         rand_dist = box_muller.box_muller(mean=2000, std=750, lower_bound=0, upper_bound=51191)
#
#     return rand_dist.pop()

# dbpath = '/Volumes/Portable Expansion/nick/github/orthophonology/db/toy-orthophon.db'
quit()
dbpath = 'data/db/toy-orthophon.db'

print('Opening database...')
conn = sqlite3.connect(dbpath)
c = conn.cursor()
print('Database opened.')

c.execute('''
    SELECT words.id, words.word_count, words.wpm, words.log_count
    FROM subtlex_words AS words
    WHERE EXISTS (SELECT 1 FROM subtlex_characters AS chars WHERE chars.id = words.id)
    ORDER BY words.log_count DESC
''')

subtlex_words = pd.DataFrame(c.fetchall(), columns=['id', 'word_count', 'wpm', 'log_count'])
tot_words = len(subtlex_words)
print(tot_words)

c.execute('''DROP TABLE IF EXISTS set_pool''')
c.execute('''
    CREATE TABLE IF NOT EXISTS set_pool
    (
        critical INTEGER NOT NULL,
        distractor INTEGER NOT NULL,
        distract_type INTEGER NOT NULL,
        distract_score REAL NOT NULL,
        ortho_sim REAL NOT NULL,
        phono_diff REAL NOT NULL,
        ortho_sim_z REAL NOT NULL,
        phono_diff_z REAL NOT NULL,
        log_diff REAL NOT NULL,
        FOREIGN KEY (critical) REFERENCES subtlex_words (id),
        FOREIGN KEY (distractor) REFERENCES subtlex_words (id),
        PRIMARY KEY (critical, distractor, distract_type)
    )
''')
c.execute('''CREATE INDEX set_pool_score_ix ON set_pool (distract_score)''')
# c.execute('''CREATE INDEX set_pool_context_ix ON set_pool (context_corr)''')
c.execute('''CREATE INDEX set_pool_log_diff_ix ON set_pool (log_diff)''')
conn.commit()

# def generate_zscores():
#     print('Generating z-score constants...')
#     c.execute('''
#         SELECT ALL similarity
#         FROM word_ortho_comps
#     ''')
#
#     sim_scores = pd.DataFrame(c.fetchall(), columns=['similarity'])
#     sim_mean = sim_scores['similarity'].mean()
#     sim_std = sim_scores['similarity'].std()
#
#     c.execute('''
#         SELECT ALL difference
#         FROM word_phono_comps
#     ''')
#
#     diff_scores = pd.DataFrame(c.fetchall(), columns=['difference'])
#     diff_mean = diff_scores['difference'].mean()
#     diff_std = diff_scores['difference'].std()
#
#     print('z-score constants generated')
#
#     return {
#         'ortho_mean': sim_mean,
#         'ortho_std': sim_std,
#         'phono_mean': diff_mean,
#         'phono_std': diff_std
#     }
#
#     # return {
#     #     'ortho_mean': .5,
#     #     'ortho_std': .25,
#     #     'phono_mean': .5,
#     #     'phono_std': .25
#     # }
#
# zscore_vars = generate_zscores()
#
# def zscore(value, which):
#     if which == 'p':
#         return (value - zscore_vars['phono_mean']) / zscore_vars['phono_std']
#     elif which == 'o':
#         return (value - zscore_vars['ortho_mean']) / zscore_vars['ortho_std']

with warnings.catch_warnings():
    warnings.simplefilter('ignore')

    c.execute('''
        SELECT ALL similarity
        FROM word_ortho_comps
    ''')

    sim_scores = pd.DataFrame(c.fetchall(), columns=['similarity'])

    sim_scores_scaler = preprocessing.MinMaxScaler(feature_range=(-50, 50))
    sim_scores_scaler = sim_scores_scaler.fit(sim_scores['similarity'])

    c.execute('''
        SELECT ALL difference
        FROM word_phono_comps
    ''')

    diff_scores = pd.DataFrame(c.fetchall(), columns=['difference'])

    diff_scores_scaler = preprocessing.MinMaxScaler(feature_range=(-50, 50))
    diff_scores_scaler = diff_scores_scaler.fit(diff_scores['difference'])

def generate_set(i):
    target_word = subtlex_words.iloc[i]

    # print('Target word: {}'.format(target_word['id']))
    timestamp = time.perf_counter()
    comps = pd.DataFrame(columns=['word', 'ortho_sim', 'phono_diff'])
    comps.set_index(['word'], inplace=True)

    def process_comps(results, target):
        comparisons = []
        for row in results:
            word1, word2, comparison = row

            if word2 == target:
                word = word1
            else:
                word = word2

            comparisons.append([word, comparison])

        return comparisons

    c.execute('''
        SELECT word1, word2, similarity
        FROM word_ortho_comps
        WHERE (word2 = :target OR word1 = :target) AND word2 < :max AND word1 < :max
        ORDER BY word1 ASC, word2 ASC
    ''', {'target': target_word['id'], 'max': tot_words})

    ortho_sims = process_comps(c.fetchall(), target_word['id'])
    for row in ortho_sims:
        comps.set_value(row[0], 'ortho_sim', row[1])

    c.execute('''
        SELECT word1, word2, difference
        FROM word_phono_comps
        WHERE (word2 = :target OR word1 = :target) AND word2 < :max AND word1 < :max
        ORDER BY word1 ASC, word2 ASC
    ''', {'target': target_word['id'], 'max': tot_words})

    phono_diffs = process_comps(c.fetchall(), target_word['id'])
    for row in phono_diffs:
        comps.set_value(row[0], 'phono_diff', row[1])

    comps = comps[comps.ortho_sim.notnull()]
    comps = comps[comps.phono_diff.notnull()]

    if len(comps) == 0:
        return []

    with warnings.catch_warnings():
        warnings.simplefilter('ignore')

        comps['ortho_sim_norm'] = sim_scores_scaler.transform(comps['ortho_sim'])
        comps['phono_diff_norm'] = diff_scores_scaler.transform(comps['phono_diff'])

        # min_max_scaler = preprocessing.MinMaxScaler(feature_range=(-1, 1))

        # comps['ortho_sim_norm'] = min_max_scaler.fit_transform(comps['ortho_sim'])
        # comps['phono_diff_norm'] = min_max_scaler.fit_transform(comps['phono_diff'])

    # comps['phono_diff_norm'] = (comps['phono_diff'] - zscore_vars['phono_mean']) / zscore_vars['phono_std']
    # comps['ortho_sim_norm'] = (comps['ortho_sim'] - zscore_vars['ortho_mean']) / zscore_vars['ortho_std']

    comps['both_sim'] = (comps['ortho_sim_norm'] - comps['phono_diff_norm']) * comps['ortho_sim_norm'].abs() * comps['phono_diff_norm'].abs()
    comps['one_diff'] = (comps['ortho_sim_norm'] + comps['phono_diff_norm']) * comps['ortho_sim_norm'].abs() * comps['phono_diff_norm'].abs()

    conditions = {
        'ortho_sim': {
            'phono_sim': None,
            'phono_diff': None
        },
        'ortho_diff': {
            'phono_sim': None,
            'phono_diff': None
        }
    }

    stmt = '''
        SELECT
            subtlex_words.id, characters.glyph,
            cedict.definitions, syllables.pinyin_tone,
            subtlex_words.word_count, subtlex_words.wpm,
            subtlex_words.log_count
        FROM words_ortho
            JOIN characters ON words_ortho.char1 = characters.id
            JOIN cedict ON characters.id = cedict.id
            JOIN words_phono ON characters.id = words_phono.id
            JOIN syllables ON words_phono.syl1 = syllables.id
            JOIN subtlex_words ON characters.id = subtlex_words.id
        WHERE words_ortho.id = ?
    '''
    c.execute(stmt, (int(target_word['id']), ))
    # origin = list(c.fetchone()[1:4])
    origin = list(c.fetchone())
    origin[2] = ' | '.join(origin[2].split(' | ')[:3])
    # print(origin)

    conditions['ortho_sim']['phono_sim'] = comps.sort_values(by=['both_sim'], ascending=[False]).iloc[:25]
    conditions['ortho_sim']['phono_diff'] = comps.sort_values(by=['one_diff'], ascending=[False]).iloc[:25]
    conditions['ortho_diff']['phono_sim'] = comps.sort_values(by=['one_diff'], ascending=[True]).iloc[:25]
    conditions['ortho_diff']['phono_diff'] = comps.sort_values(by=['both_sim'], ascending=[True]).iloc[:25]

    dict_info = dict()
    for ortho_key, phono in conditions.items():
        for phono_key, values in phono.items():

            # print(ortho_key, phono_key)
            # print(values)
            matched_words = [int(x) for x in values.index]
            # matched_word = int(values.name)
            stmt = '''
                SELECT
                    subtlex_words.id, characters.glyph,
                    cedict.definitions, syllables.pinyin_tone,
                    subtlex_words.word_count, subtlex_words.wpm,
                    subtlex_words.log_count
                FROM words_ortho
                    JOIN characters ON words_ortho.char1 = characters.id
                    JOIN cedict ON characters.id = cedict.id
                    JOIN words_phono ON characters.id = words_phono.id
                    JOIN syllables ON words_phono.syl1 = syllables.id
                    JOIN subtlex_words ON characters.id = subtlex_words.id
                WHERE words_ortho.id IN ({})
            '''.format(', '.join('?' for w in matched_words))
            c.execute(stmt, matched_words)

            for row in c:
                for ix, col in enumerate(c.description):
                    # if ix == 0:
                    #     continue

                    conditions[ortho_key][phono_key].set_value(row[0], col[0], row[ix])
                    # conditions[ortho_key][phono_key].set_value(col[0], row[ix])

            # print(conditions[ortho_key][phono_key])
            conditions[ortho_key][phono_key] = conditions[ortho_key][phono_key].to_dict('records')
            # print(conditions[ortho_key][phono_key])

    # mixed_set = []
    # mixed_set.append(conditions['ortho_sim']['phono_sim'])
    # mixed_set.append(conditions['ortho_sim']['phono_diff'])
    # mixed_set.append(conditions['ortho_diff']['phono_sim'])
    # mixed_set.append(conditions['ortho_diff']['phono_diff'])

    full_set = []
    def process_set(candidates, distract_type):
        processed = []
        # first = True
        # print(origin[1])
        for candidate in candidates:
            if distract_type == 'both_sim':
                distract_score = Decimal(candidate['ortho_sim_norm']) + (Decimal(candidate['phono_diff_norm']) * -1)
                distract_num = 1
            elif distract_type == 'orth_sim':
                distract_score = Decimal(candidate['ortho_sim_norm']) + Decimal(candidate['phono_diff_norm'])
                distract_num = 2
            elif distract_type == 'phon_sim':
                distract_score = (Decimal(candidate['ortho_sim_norm']) * -1) + (Decimal(candidate['phono_diff_norm']) * -1)
                distract_num = 3
            elif distract_type == 'both_diff':
                distract_score = (Decimal(candidate['ortho_sim_norm']) * -1) + Decimal(candidate['phono_diff_norm'])
                distract_num = 4

            row = [
                origin[0],
                int(candidate['id']),
                distract_num,
                float(distract_score),
                candidate['ortho_sim'],
                candidate['phono_diff'],
                candidate['ortho_sim_norm'],
                candidate['phono_diff_norm'],
                abs(origin[6] - candidate['log_count'])
            ]

            processed.append(row)

            # if first:
            #     print(row, candidate['glyph'], candidate['pinyin_tone'])
            #     first = False

        return processed

    full_set += process_set(conditions['ortho_sim']['phono_sim'], 'both_sim')
    full_set += process_set(conditions['ortho_sim']['phono_diff'], 'orth_sim')
    full_set += process_set(conditions['ortho_diff']['phono_sim'], 'phon_sim')
    full_set += process_set(conditions['ortho_diff']['phono_diff'], 'both_diff')

    # # full_set = mixed_set
    # full_set = []
    # for i in range(len(mixed_set)):
    #     for j in range(len(mixed_set[i])):
    #     # for j in range(1):
    #         full_set.append(mixed_set[i][j])

    # stimuli = []
    # stimuli.append(origin)
    # print(origin)
    # for i in range(len(full_set)):
    #     full_set[i]['definition'] = full_set[i]['definitions'].split(' | ')[0]
    #
    #     row = [
    #         full_set[i]['glyph'], full_set[i]['definition'], full_set[i]['pinyin_tone'],
    #         full_set[i]['ortho_sim'], full_set[i]['phono_diff'], full_set[i]['ortho_sim_norm'],
    #         full_set[i]['phono_diff_norm'], full_set[i]['wpm'], full_set[i]['log_count']
    #     ]
    #     stimuli.append(row)
    #     print(row)
    print(str(time.perf_counter() - timestamp))
    return full_set
    # return stimuli

# stims = []

for i in range(tot_words):
    if i % 50 == 0:
        print('Total completed: {}'.format(i))

    # stims += generate_set(i)
    stims = generate_set(i)
    c.executemany(
        '''INSERT OR IGNORE INTO set_pool
            (critical, distractor, distract_type,
            distract_score, ortho_sim, phono_diff,
            ortho_sim_z, phono_diff_z, log_diff)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''', stims)
    conn.commit()

# stim_csv = pathlib.Path('stim_set_new.csv')
# with stim_csv.open('w', encoding='utf-8', newline='') as csvfile:
#     csvwriter = csv.writer(csvfile, lineterminator='\n')
#
#     csvwriter.writerows(stims)
