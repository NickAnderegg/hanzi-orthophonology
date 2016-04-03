import csv
from pathlib import Path
import pathlib
import idstree
import random
import handata

random.seed()
rands = set()
for i in range(2500):
    rand = int(random.gauss(1500, 1500))
    while rand < 0 or rand in rands:
        rand = int(random.gauss(1500, 1500))
    rands.add(rand)

#print(rands)

dic = idstree.IDSDict()
subtlexcount = 0
hskcount = 0
cecount = 0
skipped = 0
infile = Path('./allids.csv')
with infile.open(mode='r', encoding='utf-8', newline='') as csvfile:
    csvreader = csv.reader(csvfile, delimiter='\t')
    for row in csvreader:
        # if random.randint(1,5) != 2:
        #     continue
        if row[1] in handata.subtlex['char']:
            dic.add_ids(row[1], row[2])
            subtlexcount += 1
        elif row[1] in handata.hsk['char']:
            dic.add_ids(row[1], row[2])
            hskcount += 1
        # elif row[1] in handata.cedictchars:
        #     dic.add_ids(row[1], row[2])
        #     cecount += 1
        else:
            skipped += 1

        if skipped > 1000:
             break

#print(str(len(dic.charlist)))
print('SUBTLEX: {}\tHSK: {}\tCEDICT: {}\tSkipped: {}'.format(subtlexcount, hskcount, cecount, skipped))
dic.compare_characters(25)
dic.output_char_comparisons(rev=False)
