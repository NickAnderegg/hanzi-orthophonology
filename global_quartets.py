import sqlite3
import numpy as np
import pandas as pd
import csv
import pathlib

dbpath = 'data/db/toy-orthophon.db'

print('Opening database...')
conn = sqlite3.connect(dbpath)
c = conn.cursor()
print('Database opened.')

c.execute('''
    SELECT id, glyph FROM characters
''')

glyphs = pd.DataFrame(c.fetchall(), columns=['id', 'glyph'])
glyphs = glyphs.set_index(['id'])

c.execute('''
    SELECT words_phono.id, syllables.pinyin_tone
    FROM words_phono
    JOIN syllables ON words_phono.syl1 = syllables.id
    WHERE words_phono.syl2 IS NULL
''')

syllables = pd.DataFrame(c.fetchall(), columns=['id', 'syllable'])
syllables = syllables.set_index(['id'])

c.execute('''
    SELECT id, word_count, log_count FROM subtlex_words
''')

subtlex_words = pd.DataFrame(c.fetchall(), columns=['id', 'word_count', 'log_count'])
subtlex_words = subtlex_words.set_index(['id'])

c.execute('''
    SELECT * FROM set_pool ORDER BY critical ASC, distract_type ASC, distract_score DESC
''')

candidates = pd.DataFrame(c.fetchall(),
    columns=['critical', 'distractor', 'distract_type',
        'distract_score', 'ortho_sim', 'phono_diff',
        'ortho_sim_z', 'phono_diff_z', 'log_diff']
)

candidates = candidates.set_index(['critical', 'distract_type'])

criticals = candidates.index.levels[0]

top_sets = pd.DataFrame(columns=['critical', 'aggregate',
    'both_sim_id', 'both_sim_score',
    'ortho_sim_id', 'ortho_sim_score',
    'phono_sim_id', 'phono_sim_score',
    'both_diff_id', 'both_diff_score']
)
top_sets = top_sets.set_index(['critical'])

def compute_aggregate(both_sim, ortho_sim, phono_sim, both_diff):
    distractor_mean = (
        both_sim +
        ortho_sim +
        phono_sim +
        both_diff
    ) / 4

    distractor_error = (
        abs(distractor_mean - both_sim) +
        abs(distractor_mean - ortho_sim) +
        abs(distractor_mean - phono_sim) +
        abs(distractor_mean - both_diff)
    ) / 4

    return distractor_mean / (1 + distractor_error)

for critical in criticals:
    pool = candidates.loc[critical]

    both_sim = pool.loc[1].iloc[0]
    ortho_sim = pool.loc[2].iloc[0]
    phono_sim = pool.loc[3].iloc[0]
    both_diff = pool.loc[4].iloc[0]

    row = {
        'both_sim_id': int(both_sim.distractor),
        'both_sim_score': both_sim.distract_score,
        'ortho_sim_id': int(ortho_sim.distractor),
        'ortho_sim_score': ortho_sim.distract_score,
        'phono_sim_id': int(phono_sim.distractor),
        'phono_sim_score': phono_sim.distract_score,
        'both_diff_id': int(both_diff.distractor),
        'both_diff_score': both_diff.distract_score,
        'aggregate': compute_aggregate(
            both_sim.distract_score,
            ortho_sim.distract_score,
            phono_sim.distract_score,
            both_diff.distract_score
        )
    }

    # distractor_mean = (
    #     both_sim.distract_score +
    #     ortho_sim.distract_score +
    #     phono_sim.distract_score +
    #     both_diff.distract_score
    # ) / 4
    #
    # distractor_error = (
    #     abs(distractor_mean - both_sim.distract_score) +
    #     abs(distractor_mean - ortho_sim.distract_score) +
    #     abs(distractor_mean - phono_sim.distract_score) +
    #     abs(distractor_mean - both_diff.distract_score)
    # ) / 4
    #
    # row['aggregate'] = distractor_mean / (1 + distractor_error)

    # print('Mean: {:.3f}\tError: {:.3f}\tAggregate: {:.3f}'.format(distractor_mean, distractor_error, row['aggregate']))

    top_sets.loc[critical] = row

    # if len(top_sets) % 500 == 0:
    #     break
    #     print(top_sets.sort_values('aggregate', ascending=False))

top_sets = top_sets.sort_values('aggregate', ascending=False)

def get_candidates(top_stims):
    critical_set = []
    distract_set = dict()

    for i in range(len(top_stims)):
        stim_set = top_stims.iloc[i]

        crit = stim_set.name
        critical_set.append(crit)

        # both = str(glyphs.loc[int(stim_set.both_sim_id)].glyph)
        # orth = str(glyphs.loc[int(stim_set.ortho_sim_id)].glyph)
        # phon = str(glyphs.loc[int(stim_set.phono_sim_id)].glyph)
        # diff = str(glyphs.loc[int(stim_set.both_diff_id)].glyph)

        both = int(stim_set.both_sim_id)
        orth = int(stim_set.ortho_sim_id)
        phon = int(stim_set.phono_sim_id)
        diff = int(stim_set.both_diff_id)

        for dist in [both, orth, phon, diff]:
            if dist in distract_set.keys():
                distract_set[dist].append(int(stim_set.name))
            else:
                distract_set[dist] = [int(stim_set.name)]

        # print('Critical:\t{} {}'.format(glyphs.loc[stim_set.name].glyph, syllables.loc[stim_set.name].syllable))
        # print('Both sim:\t{} {}'.format(glyphs.loc[int(stim_set.both_sim_id)].glyph, syllables.loc[int(stim_set.both_sim_id)].syllable))
        # print('Orth sim:\t{} {}'.format(glyphs.loc[int(stim_set.ortho_sim_id)].glyph, syllables.loc[int(stim_set.ortho_sim_id)].syllable))
        # print('Phon sim:\t{} {}'.format(glyphs.loc[int(stim_set.phono_sim_id)].glyph, syllables.loc[int(stim_set.phono_sim_id)].syllable))
        # print('Both dif:\t{} {}'.format(glyphs.loc[int(stim_set.both_diff_id)].glyph, syllables.loc[int(stim_set.both_diff_id)].syllable))
        #
        # print()
    return distract_set, critical_set

distract_set, critical_set = get_candidates(top_sets.iloc[:200])

print(len(distract_set))
# top_candidates = sorted(distract_set, key=distract_set.get)
top_candidates = list(distract_set.keys())
duplicates = dict()

for val in top_candidates:
    if len(distract_set[val]) > 1:
        duplicates[val] = distract_set[val]
        # print('{}: {}'.format(val, distract_set[val]))

changed = 0
for dup_char, dup_set in duplicates.items():

    to_modify = []
    for dup in dup_set:
        if top_sets.loc[dup].both_diff_id == dup_char:
            replacements = candidates.loc[dup].loc[4]

            for i in range(1, len(replacements)):
                if int(replacements.iloc[i].distractor) not in top_candidates:
                    top_sets.loc[dup].both_diff_id = int(replacements.iloc[i].distractor)
                    top_sets.loc[dup].both_diff_score = replacements.iloc[i].distract_score

                    top_sets.loc[dup].aggregate = compute_aggregate(
                        top_sets.loc[dup].both_sim_score,
                        top_sets.loc[dup].ortho_sim_score,
                        top_sets.loc[dup].phono_sim_score,
                        top_sets.loc[dup].both_diff_score
                    )

                    top_candidates.append(int(replacements.iloc[i].distractor))
                    changed += 1
                    break
        else:
            to_modify.append(dup_char)

    # if len(to_modify) > 1:
    #     new_delta = []
    #     for i in range(len(to_modify)):
    #         if top_sets.loc[dup].both_sim_id == to_modify:
    #             replacements = candidates.loc[dup].loc[1]
    #         elif top_sets.loc[dup].ortho_sim_id == to_modify:
    #             replacements = candidates.loc[dup].loc[2]
    #         elif top_sets.loc[dup].phono_sim_id == to_modify:
    #             replacements = candidates.loc[dup].loc[3]
    #
    #         for j in range(1, len(replacements)):
    #             if int(replacements.iloc[i].distractor) not in top_candidates:
    #                 break


print('changed: {}'.format(changed))
distract_set, critical_set = get_candidates(top_sets.iloc[:200])

print(len(distract_set))

outcsv = pathlib.Path('revised_stimuli_2.csv')
with outcsv.open('w', newline='', encoding='utf-8') as f:
    csvwriter = csv.writer(f, lineterminator='\n', delimiter='\t')

    final_set = top_sets.iloc[:200]
    for i in range(len(final_set)):
        out_stims = final_set.iloc[i]

        rows = []

        rows.append(['Critical:', glyphs.loc[out_stims.name].glyph, syllables.loc[out_stims.name].syllable, 'Aggregate: {}'.format(out_stims.aggregate), subtlex_words.loc[out_stims.name].log_count])
        rows.append(['Both sim:', glyphs.loc[int(out_stims.both_sim_id)].glyph, syllables.loc[int(out_stims.both_sim_id)].syllable, 'Score: {}'.format(out_stims.both_sim_score), subtlex_words.loc[int(out_stims.both_sim_id)].log_count])
        rows.append(['Ortho sim:', glyphs.loc[int(out_stims.ortho_sim_id)].glyph, syllables.loc[int(out_stims.ortho_sim_id)].syllable, 'Score: {}'.format(out_stims.ortho_sim_score), subtlex_words.loc[int(out_stims.ortho_sim_id)].log_count])
        rows.append(['Phono sim:', glyphs.loc[int(out_stims.phono_sim_id)].glyph, syllables.loc[int(out_stims.phono_sim_id)].syllable, 'Score: {}'.format(out_stims.phono_sim_score), subtlex_words.loc[int(out_stims.phono_sim_id)].log_count])
        rows.append(['Both diff:', glyphs.loc[int(out_stims.both_diff_id)].glyph, syllables.loc[int(out_stims.both_diff_id)].syllable, 'Score: {}'.format(out_stims.both_diff_score), subtlex_words.loc[int(out_stims.both_diff_id)].log_count])
        rows.append([])

        csvwriter.writerows(rows)
