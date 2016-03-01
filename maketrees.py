import csv
from pathlib import Path
import idstree
import random

random.seed()

dic = idstree.IDSDict()
count = 0
infile = Path('./allids.csv')
with infile.open(mode='r', encoding='utf-8', newline='') as csvfile:
    csvreader = csv.reader(csvfile, delimiter='\t')
    for row in csvreader:
        count += 1
        dic.add_ids(row[1], row[2])

        if count > 500:
            break

print(str(len(dic.charlist)))
dic.compare_characters(10)
#dic.print_char_comparisons(rev=False)
