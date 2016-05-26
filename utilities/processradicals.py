from lxml import etree
import pathlib
import json
import csv

radical_table = str()
radical_input = pathlib.Path('data/radicals.htm')
with radical_input.open('r', encoding='utf-8') as f:
    radical_table = f.read()

radical_csv = pathlib.Path('data/radicals.csv')
with radical_csv.open('w', encoding='utf-8', newline='') as f:
    #csvwriter = csv.writer(csvfile, lineterminator='\n')
    f.write('\{\n')
    table = etree.XML(radical_table)
    rows = iter(table)
    headers = [col.text for col in next(rows)]
    #headers = [headers[0], headers[1]]
    #csvwriter.writerow(headers)
    count = 10
    for row in rows:
        if count < 10:
            count += 1
        else:
            count = 1
            f.write('\n   ')

        values = [col.text for col in row]
        if values[3] is not None:
            radical = values[3]
        else:
            radical = values[1]

        f.write(" {}: '{}',".format(values[0], radical))
        #csvwriter.writerow([values[0], radical])
