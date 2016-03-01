import csv
from pathlib import Path
import ideograph
import json

validchars = set(range(0x4E00,0x9FFF+1))
validchars = validchars.union(range(0x2E80,0x2EFF+1))
validchars = validchars.union(range(0x2F00,0x2FDF+1))
validchars = validchars.union(range(0x3000,0x303F+1))
validchars = validchars.union(range(0x31C0,0x31EF+1))
validchars = validchars.union(range(0x3400,0x4DBF+1))
validchars = validchars.union(range(0x20000,0x2A6DF+1))
validchars = validchars.union(range(0x2A700,0x2B73F+1))
validchars = validchars.union(range(0x2B740,0x2B81F+1))
validchars = validchars.union(range(0x2B820,0x2CEAF+1))
validchars = validchars.union(range(0xF900,0xFAFF+1))
validchars = validchars.union(range(0x2F800,0x2FA1F+1))
validchars = validchars.union(range(0x2FF0, 0x2ffb+1))

invalidlines = 0
charfile = []
functors = ideograph.Functors()
sep = {}
for func in functors.all:
    if func in functors.ternary:
        sep[func] = 3
    else:
        sep[func] = 2

tree = ideograph.IDSTree(sep)

p = Path('./ids/')
for x in p.iterdir():
    if x.is_file() and '.txt' in x.suffixes and 'UCS' in x.stem:
        with x.open() as csvfile:
            #print('Now reading file: {}'.format(x.stem))
            csvreader = csv.reader(csvfile, delimiter='\t')
            for row in csvreader:
                if len(row) < 3:
                    pass
                elif not set(ord(x) for x in row[2]).issubset(validchars):
                    invals = [x for x in row[2] if ord(x) not in validchars and ord(x) > 128]
                    #if len(invals) > 0:
                        #print(''.join(['Invalid characters: ', ' | '.join(invals)]))
                    invalidlines += 1
                else:
                    charfile.append(row)
                    #if len(charfile) < 100:
                        #tree.set_ids(row[2])
                        #print('{}\tTree:\n{}\n'.format('\t'.join(row), str(json.dumps(tree.tree, indent=4, ensure_ascii=False))))

print(''.join(['Valid lines: ', str(len(charfile)), '\tInvalid lines: ', str(invalidlines), '\tPercentage valid: ', str(len(charfile)/(len(charfile)+invalidlines)*100), '%']))

out = Path('./allids.csv')
out.touch()

with out.open(mode='w', encoding='utf-8', newline='') as csvfile:
    csvwriter = csv.writer(csvfile, lineterminator='\n', delimiter='\t')

    for row in charfile:
        csvwriter.writerow(row)
