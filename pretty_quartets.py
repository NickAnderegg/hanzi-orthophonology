import csv
import pathlib
import decimal
import simplejson as json
from decimal import Decimal

decimal.getcontext().prec = 4

def split_strip(line):
    return line.strip().split(',')

def truncate_float(num):
    '{:.4f}'.format(num)

def char_dict(entry, comp_type):
    entry_dict = {
        'glyph': entry[0],
        'definition': entry[1],
        'pinyin': entry[2],
        'ortho_sim': Decimal(entry[3]).normalize(),
        'phono_diff': Decimal(entry[4]).normalize()
    }

    if comp_type is 'both_sim':
        entry_dict['normalized'] = Decimal(entry[5]) + (Decimal(entry[6]) * -1)
    elif comp_type is 'orth_sim':
        entry_dict['normalized'] = Decimal(entry[5]) + Decimal(entry[6])
    elif comp_type is 'phon_sim':
        entry_dict['normalized'] = (Decimal(entry[5]) * -1) + (Decimal(entry[6]) * -1)
    elif comp_type is 'both_diff':
        entry_dict['normalized'] = (Decimal(entry[5]) * -1) + Decimal(entry[6])

    return entry_dict

quartets = dict()

stim_file = pathlib.Path('stim_set.csv')
with stim_file.open(mode='r', encoding='utf-8', newline='\n') as f:
    stim_set = csv.reader(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL, lineterminator='\n')

    for i in range(int(20585/5)):

        critical = next(stim_set)
        crit_char = critical[0]

        both_sim = char_dict(next(stim_set), 'both_sim')
        orth_sim = char_dict(next(stim_set), 'orth_sim')
        phon_sim = char_dict(next(stim_set), 'phon_sim')
        both_diff = char_dict(next(stim_set), 'both_diff')


        quartets[crit_char] = {
            'cumulative':
                (
                    both_sim['normalized'] +
                    orth_sim['normalized'] +
                    phon_sim['normalized'] +
                    both_diff['normalized']
                ),
            'info': {
                'definition': critical[1],
                'pinyin': critical[2]
            },
            'O+ P+': both_sim,
            'O+ P-': orth_sim,
            'O- P+': phon_sim,
            'O- P-': both_diff
        }

        # print(json.dumps(quartets[crit_char], indent=4, ensure_ascii=False))

        # if (i+1) % 100 == 0:
        #     print('Completed: {}'.format(i))

    # print('Completed: {}'.format(len(quartets)))

sorted_quartets = sorted(quartets.items(), key=lambda quartet: quartet[1]['cumulative'], reverse=True)
sorted_quartets = sorted_quartets[:150]
# print(json.dumps(sorted_quartets, indent=4, ensure_ascii=False))

pretty_out = pathlib.Path('pretty_stim_set.csv')
with pretty_out.open('w', newline='', encoding='utf-8') as f:
    stim_out = csv.writer(f, dialect='excel-tab')

    for row in sorted_quartets:
        critical = row[0]
        quartet = row[1]

        stim_out.writerow([
                critical,
                quartet['info']['pinyin'],
                'O+P+',
                quartet['O+ P+']['glyph'],
                quartet['O+ P+']['pinyin'],
                quartet['O+ P+']['normalized'],
                quartet['O+ P+']['definition']
            ])

        stim_out.writerow([
                '',
                quartet['info']['definition'],
                'O+P-',
                quartet['O+ P-']['glyph'],
                quartet['O+ P-']['pinyin'],
                quartet['O+ P-']['normalized'],
                quartet['O+ P-']['definition']
            ])

        stim_out.writerow([
                '',
                '',
                'O-P+',
                quartet['O- P+']['glyph'],
                quartet['O- P+']['pinyin'],
                quartet['O- P+']['normalized'],
                quartet['O- P+']['definition']
            ])
        stim_out.writerow([
                '',
                '',
                'O-P-',
                quartet['O- P-']['glyph'],
                quartet['O- P-']['pinyin'],
                quartet['O- P-']['normalized'],
                quartet['O- P-']['definition']
            ])

        stim_out.writerow([''])
