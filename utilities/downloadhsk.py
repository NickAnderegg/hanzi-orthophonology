import pathlib
import csv

files = {
    1: "../data/hsk/L1_with_definitions.txt",
    2: "../data/hsk/L2_with_definitions.txt",
    3: "../data/hsk/L3_with_definitions.txt",
    4: "../data/hsk/L4_with_definitions.txt",
    5: "../data/hsk/L5_with_definitions.txt",
    6: "../data/hsk/L6_with_definitions.txt"
}

output = pathlib.Path("../data/hsk_words.csv")

with output.open('w', encoding='utf-8', newline='') as outcsv:
    csvwriter = csv.writer(outcsv, delimiter='\t', lineterminator='\n')

    for level, loc in files.items():
        levelfile = pathlib.Path(loc)
        with levelfile.open('r', encoding='utf-8', newline='') as levelcsv:
            csvreader = csv.reader(levelcsv, delimiter='\t')
            count = 0
            prevrow = None
            for row in csvreader:
                if row[2] == prevrow:
                    print(row)
                prevrow = row[2]
                csvwriter.writerow([level] + row)
                count += 1

            print('Length of Level {}: {}'.format(level, count))
