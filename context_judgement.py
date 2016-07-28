import csv
import pathlib
import operator
import tokenizer

test_words = [
    '狗',
    '猫',
    '屋',
    '书',
    '汽车',
    '小学'
]

stim_words = []
stim_set = pathlib.Path('stim_set.csv')
with stim_set.open('r', newline='', encoding='utf-8') as csvfile:
    csvreader = csv.reader(csvfile, delimiter=',', lineterminator='\n')

    line = 0
    for i in range(int(20585/5)):
        quartet = {
            'critical': next(csvreader),
            'both_sim': next(csvreader),
            'orth_sim': next(csvreader),
            'phon_sim':  next(csvreader),
            'both_diff': next(csvreader)
        }

        stim_words.append(quartet)

        line += 1
        if line > 30:
            break

# for word in test_words:
for row in stim_words:
    total_downloaded = 0
    word = row['critical'][0]
    definition = row['critical'][1]

    sentences = tokenizer.download_jukuu_word(word)
    possible_sentences = []

    for sentence in sentences:
        total_downloaded += 1
        tokenized = tokenizer.tokenizer(sentence['cn'])

        if word not in tokenized:
            # print('Word not tokenized: {}'.format(' | '.join(tokenized)))
            continue

        ix = tokenized.index(word)
        if ix < 2:
            # print('Token too early: {}'.format(sentence['en']))
            continue

        new_sentence = {
            'en': sentence['en'],
            'cn': sentence['cn'],
            'tokenized': tokenized,
            'transition_freqs': tokenizer.transition_frequencies(tokenized),
            'token_freqs': tokenizer.token_frequencies(tokenized)
        }


        possible_sentences.append(new_sentence)

    print('Word {} ({})\nAccepted: {}; Rejected: {}\n'.format(word, definition, len(possible_sentences), total_downloaded-len(possible_sentences)))

    possible_sentences = sorted(possible_sentences, key=lambda s: s['transition_freqs'], reverse=True)
    possible_sentences = possible_sentences[:3]

    for sentence in possible_sentences:
        print(sentence['en'])
        print(sentence['cn'])
        print(sentence['tokenized'])
        print('Avg. transition freq: {}'.format(sentence['transition_freqs']))
        print('Avg. token freq: {}'.format(sentence['token_freqs']))
        print()

        ix = sentence['tokenized'].index(word)

        print(sentence['tokenized'][ix-2:ix] + [row['both_sim'][0]])
        print('Both sim: {}\tTransition freq: {}'.format(row['both_sim'][0], tokenizer.transition_frequencies(sentence['tokenized'][ix-2:ix] + [row['both_sim'][0]])))
        print('Orth sim: {}\tTransition freq: {}'.format(row['orth_sim'][0], tokenizer.transition_frequencies(sentence['tokenized'][ix-2:ix] + [row['orth_sim'][0]])))
        print('Phon sim: {}\tTransition freq: {}'.format(row['phon_sim'][0], tokenizer.transition_frequencies(sentence['tokenized'][ix-2:ix] + [row['phon_sim'][0]])))
        print('Both diff: {}\tTransition freq: {}\n'.format(row['both_diff'][0], tokenizer.transition_frequencies(sentence['tokenized'][ix-2:ix] + [row['both_diff'][0]])))
