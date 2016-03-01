import csv
from pathlib import Path
import idstree
import random

random.seed()
rands = set()
for i in range(2000):
    rand = int(random.gauss(1500, 1500))
    while rand < 0 or rand in rands:
        rand = int(random.gauss(1500, 1500))
    rands.add(rand)

#print(rands)

dic = idstree.IDSDict()
count = 0
infile = Path('./allids.csv')
with infile.open(mode='r', encoding='utf-8', newline='') as csvfile:
    csvreader = csv.reader(csvfile, delimiter='\t')
    for row in csvreader:
        count += 1
        if count in rands:
            dic.add_ids(row[1], row[2])

        if count > 15000:
            break

print(str(len(dic.charlist)))
dic.compare_characters(10)
dic.print_char_comparisons(rev=False)
