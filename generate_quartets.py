import sqlite3
import pathlib
import numpy as np
import pandas as pd
import box_muller
# import handata
from collections import deque

rand_dist = []
def next_rand():
    global rand_dist
    if len(rand_dist) == 0:
        rand_dist = box_muller.box_muller(mean=2000, std=750, lower_bound=0, upper_bound=51191)

    return rand_dist.pop()

dbpath = '/Volumes/Portable Expansion/nick/github/orthophonology/db/old-orthophon.db'

print('Opening database...')
conn = sqlite3.connect(dbpath)
c = conn.cursor()

c.execute('''SELECT id, word_count, wpm, log_count FROM subtlex_words ORDER BY log_count DESC''')

subtlex_words = pd.DataFrame(c.fetchall(), columns=['id', 'word_count', 'wpm', 'log_count'])

target_word = subtlex_words.iloc[next_rand()]

print(target_word)

# c.execute('''
#     SELECT ortho.word1, ortho.word2, ortho.similarity, phono.difference
#     FROM word_ortho_comps AS ortho
#         JOIN word_phono_comps As phono On ortho.word1 = phono.word1, ortho.word2 = phono.word2
#     WHERE ortho.similarity > 0.75, phono.difference < 0.25, ortho.word1 = 7
#     ORDER BY ortho.similarity DESC, phono.difference ASC
#     LIMIT 10
# ''')

c.execute('''
    SELECT word_ortho_comps.word1, word_ortho_comps.word2, word_ortho_comps.similarity, word_phono_comps.difference
    FROM word_ortho_comps JOIN word_phono_comps USING (word1, word2)
    ORDER BY word_ortho_comps.similarity DESC, word_phono_comps.difference ASC
    LIMIT 10
''')

print('Executed...')
for i in range(10):
    print(c.fetchone())
