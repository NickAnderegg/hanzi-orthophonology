from lxml import etree
import pathlib
import json
import csv
import pprint
import handata

comps = []
count = 0
passcount = 0
for id1, s1 in enumerate(handata.syllables):
    for id2, s2 in enumerate(handata.syllables):
        if id2 < id1:
            passcount += 1
            # print('Pair skipped: {}, {}, {}'.format(s1, s2, count))
            continue
        if count % (2005*50) == 0:
            percentage = 100.0 * count / 2011015
            print('{} comparisons completed ({}%); {} passed'.format(count, percentage, passcount))
        distance = handata.syllables.syllable_distance(s1, s2)
        comps.append([s1, s2, distance])
        count += 1

    if count > 100000:
        break

print('Comparisons complete')
srt = sorted(comps, key=lambda comp: comp[2])

print('Sorting complete')
count = 0
phon_csv = pathlib.Path('utilities/distances.txt')
with phon_csv.open('w', encoding='utf-8', newline='') as csvfile:
    csvwriter = csv.writer(csvfile, lineterminator='\n')

    for row in srt:
        #if row[2] > 0:
        csvwriter.writerow(row)
        count += 1

print('{} comparisons in file'.format(count))
