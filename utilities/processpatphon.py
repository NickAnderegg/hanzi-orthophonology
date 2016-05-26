from lxml import etree
import pathlib
import json
import csv
import pprint

vowels = {
    'ɑ': [0.100, 0.250, 0.444],
    'a': [0.100, 0.175, 0.444],
    'ɛ': [0.100, 0.100, 0.355],
    'e': [0.100, 0.100, 0.270],
    'o': [0.175, 0.250, 0.185],
    'ɤ': [0.100, 0.250, 0.185],
    'ə': [0.100, 0.175, 0.185],
    'u': [0.175, 0.250, 0.100],
    'ʊ': [0.175, 0.250, 0.100],
    'y': [0.175, 0.100, 0.100],
    'i': [0.100, 0.100, 0.100],
    'ɯ': [0.100, 0.030, 0.100],
    'ɨ': [0.100, 0.060, 0.100]
}

consonants = {
    't':    [1.000, 0.670, 0.733],
    'tʰ':   [0.925, 0.670, 0.733],
    'p':    [1.000, 0.450, 0.733],
    'pʰ':   [0.925, 0.450, 0.733],
    'k':    [1.000, 1.000, 0.733],
    'kʰ':   [0.925, 1.000, 0.733],
    'n':    [0.750, 0.670, 0.644],
    'm':    [0.750, 0.450, 0.644],
    'ŋ':    [0.750, 1.000, 0.644],
    'l':    [0.750, 0.670, 1.000],
    's':    [1.000, 0.670, 0.822],
    'f':    [1.000, 0.560, 0.822],
    'ɕ':    [1.000, 0.890, 0.822],
    'ʐ':    [0.750, 0.780, 0.822],
    'ʂ':    [1.000, 0.780, 0.822],
    'x':    [1.000, 1.000, 0.822],
    'ts':   [1.000, 0.670, 0.911],
    'tsʰ':  [0.925, 0.670, 0.911],
    'tɕ':   [1.000, 0.890, 0.911],
    'tɕʰ':  [0.925, 0.890, 0.911],
    'ʈʂ':   [1.000, 0.780, 0.911],
    'ʈʂʰ':  [0.925, 0.780, 0.911],
}

phonemes = dict(vowels)
phonemes.update(consonants)
phonemes[''] = [0, 0, 0]

glides = ['y', 'i', 'u']
codas = glides + ['n', 'ŋ', 'ʐ']

# glides = {
#     'i': [0.750, 0.950, 1.000],
#     'u': [0.750, 0.450, 1.000],
#     'y': [0.750, 0.950, 1.000]
# }

phon_table = str()
phon_input = pathlib.Path('data/patphon.html')
with phon_input.open('r', encoding='utf-8') as f:
    phon_table = f.read()

phon_csv = pathlib.Path('data/revsyllables.json')
with phon_csv.open('w', encoding='utf-8', newline='') as f:
    # csvwriter = csv.writer(csvfile, lineterminator='\n')
    #f.write('syllable_dict = ')
    table = etree.XML(phon_table)
    rows = iter(table)

    headers = [col.text for col in next(rows)]
    headers.insert(2, 'tone_ipa')
    headers[3] = 'tone_desc'

    headers[4:7] = []
    headers.insert(4, 'onset')

    headers[5:8] = []
    headers.insert(5, 'glide')

    headers[6:9] = []
    headers.insert(6, 'nucleus')

    headers[7:10] = []
    headers.insert(7, 'coda')

    headers[8:11] = []
    #headers.insert(8, 'coda')

    headers[8] = 'tone'

    headers = [x for x in headers]
    # csvwriter.writerow(headers)
    #count = 10
    syllables = dict()
    for row in rows:
        # if count < 10:
        #     count += 1
        # else:
        #     count = 1
        #     f.write('\n   ')

        values = [col.text for col in row]

        # Replace obsolete IPA symbols
        values[1] = values[1].replace('ʻ','ʰ')
        values[1] = values[1].replace('ɿ', 'ɯ')
        values[1] = values[1].replace('ʅ','ɨ')

        if 'o' in values[0] and 'u' not in values[0] and 'a' not in values[0] and 'w' not in values[0]:
            values[1] = values[1].replace('u','ʊ')

        # Reformat IPA
        values[1] = values[1].replace('tʂ', 'ʈʂ')

        if values[0][-1] == '1':
            values.insert(2, '˥')
        elif values[0][-1] == '2':
            values.insert(2, '˧˥')
        elif values[0][-1] == '3':
            values.insert(2, '˨˩˦')
        elif values[0][-1] == '4':
            values.insert(2, '˥˩')
        else:
            values.insert(2, '')

        output = values[0:4]

        coda = ''
        nucleus = ''
        glide = ''
        onset = ''
        ipa = list(output[1])
        for i in range(3, 0, -1):
            if ''.join(ipa[0:i]) in consonants:
                onset = ''.join(ipa[0:i])
                ipa[0:i] = []
                break

        if len(ipa) > 1 and ipa[-1] in codas:
            coda = ipa.pop(-1)
            #ipa[-1] = []
        # elif ipa[-1] in glides:
        #     glide = ipa.pop(-1)
        # else:
        #     nucleus = ipa.pop(-1)
        #     #ipa[-1] = []

        if len(ipa) > 1 and ipa[0] in glides:
            glide = ipa.pop(0)
            #ipa[0] = []

        if len(ipa) > 0 and ipa[0] in vowels:
            nucleus = ipa.pop(0)
            #ipa[0] = []

        if len(''.join([onset, glide, nucleus, coda])) != len(output[1]):
            print('Error: {}'.format(output[1]))

        # if coda == '':
        #     coda = nucleus

        # output.append(onset)
        # output.append(glide)
        # output.append(nucleus)
        # output.append(coda)
        # output.append(values[8])
        toneval = values[-1]

        # if output[0][-1] == '0':
        #     print('{}\t{}\t{}\t{}\t{}\t{}'.format(output[0][0:-1], output[1], onset, glide, nucleus, coda))

        # output.append([float(x) for x in values[4:7]])
        # output.append([float(x) for x in values[7:10]])
        # output.append([float(x) for x in values[10:13]])
        # output.append([float(x) for x in values[13:16]])
        # output.append([float(x) for x in values[16:19]])
        # output.append(float(values[19]))
        #
        # if output[6] == [0,0,0]:
        #     nucleus = [[0,0,0],[0,0,0],output[5]]
        # elif output[7] == [0,0,0]:
        #     nucleus = [[0,0,0], output[5], output[6]]
        # else:
        #     nucleus = [output[5], output[6], output[7]]
        formatted = {
            'ipa': output[1],
            'tone_ipa': output[2],
            'tone_desc': output[3],
            'syllable': [
                onset,
                glide,
                nucleus,
                coda
            ],
            'onset': phonemes[onset],
            'glide': phonemes[glide],
            'nucleus': phonemes[nucleus],
            'coda': phonemes[coda],
            'tone': toneval,
            'full_ipa': ''.join([output[1], output[2]])
        }

        if coda == '':
            formatted['coda'] = phonemes[nucleus]

        # formatted['rime'] = [x + y for x, y in zip(phonemes[nucleus], phonemes[coda])]
        # formatted['full_onset'] = [x + y for x, y in zip(phonemes[onset], phonemes[glide])]

        if onset == '' and glide != '':
            # formatted['syllable'][0] = glide
        #     #formatted['syllable'][1] = ''
        #
            formatted['onset'] = phonemes[glide]
        #     formatted['glide'] = [x/2 for x in phonemes[glide]]

        syllables[output[0]] = formatted
        #f.write(pprint.pprint(formatted, indent=4))

        # f.write("\t'{}' = {{\n".format(output[0]))
        # f.write("\t\t'ipa' = '{}',\n".format(output[1]))
        # f.write("\t\t'tone_ipa' = '{}',\n".format(output[2]))
        # f.write("\t\t'tone_desc' = '{}',\n".format(output[3]))
        # f.write("\t},\n")

        #f.write(" {}: '{}',".format(values[0], radical))
        # csvwriter.writerow(output)

    #pprint.pprint(syllables, stream=f, compact=True, indent=4)
    json.dump(syllables, f, ensure_ascii=False, sort_keys=True, indent=4)
