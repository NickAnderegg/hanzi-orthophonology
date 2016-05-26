import pathlib
import csv
import codecs

subtlex_folder = pathlib.Path('data/subtlex-ch')
for child in subtlex_folder.iterdir():
    if 'SUBTLEX' in child.stem:
        contents = []
        with codecs.open(str(child), 'rU', 'utf_16_le') as csvfile:
            reformatted = pathlib.Path(str(child.parent) + '/' + child.stem + '.csv')
            csvreader = csv.reader(csvfile, delimiter='\t')
            with reformatted.open('w', encoding='utf-8', newline='') as outfile:
                csvwriter = csv.writer(outfile, delimiter='\t', lineterminator='\n')
                for row in csvreader:
                    csvwriter.writerow(row)
