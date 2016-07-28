import sqlite3
import pandas as pd
import numpy as np
import lxml
from lxml import etree, html
from lxml.html.clean import clean_html
import requests
from contextlib import closing
import re
import operator
import csv
import pathlib

ngram_path = '../ngram downloader/ngram_database.db'

print('Opening database...')
ngram_conn = sqlite3.connect(ngram_path)
ngram_db = ngram_conn.cursor()

ngram_db.execute('''
    SELECT id, token, count
    FROM ngrams_1
    ORDER BY id ASC
''')

ngrams = {
    1 : None
}

ngrams[1] = pd.DataFrame(data=ngram_db.fetchall(), columns=['id', 'token', 'count'])
ngrams[1] = ngrams[1].set_index(['id'])
ngrams[1]['tpm'] = ngrams[1]['count'] / (ngrams[1].sum(numeric_only=True)['count'] / 1000000)
# print(ngrams_1.sort(columns=['count'], ascending=False))

def token_id(token):
    tok_id = ngrams[1].loc[ngrams[1]['token'] == token].index.tolist()

    if len(tok_id) > 0:
        return tok_id[0]
    else:
        return -1

ngram_counts = {
    1: None,
    2: None,
    3: None,
    4: None,
    5: None
}

def get_ngram_counts(n):
    # select_cols = ', '.join(['token{}'.format(x+1) for x in range(n)])
    # order_by = ', '.join(['token{} ASC'.format(x+1) for x in range(n)])
    # ngram_db.execute('''
    #     SELECT {}, count
    #     FROM ngrams_{}
    #     ORDER BY {}
    # '''.format(select_cols, n, order_by))

    ngram_db.execute('''
        SELECT SUM(count)
        FROM ngrams_{}
    '''.format(n))

    result = ngram_db.fetchone()
    ngram_counts[n] = int(result[0])

    # ix = ['token{}'.format(x+1) for x in range(n)]
    # cols = ix + ['count']
    # ngrams[n] = pd.DataFrame(data=ngram_db.fetchall(), columns=cols)
    # ngrams[n] = ngrams[n].set_index(ix)
    # ngrams[n]['tpm'] = ngrams[n]['count'] / (ngrams[n].sum(numeric_only=True)['count'] / 1000000)

def ngram_info(tokens=[]):
    n = len(tokens)
    for i in range(n):
        tokens[i] = token_id(tokens[i])

    if n == 1:
        try:
            info = ngrams[n].loc[tokens[0]]
            return {
                'count': info['count'],
                'tpm': info['tpm']
            }
        except KeyError:
            return {
                'count': 0,
                'tpm': 0
            }

    where_cols = []
    for ix, token in enumerate(tokens):
        where_cols.append('token{} = {}'.format(ix+1, token))

    where_cols = ' AND '.join(where_cols)

    # order_by = ', '.join(['token{} ASC'.format(x+1) for x in range(n)])
    ngram_db.execute('''
        SELECT count
        FROM ngrams_{}
        WHERE {}
    '''.format(n, where_cols))

    result = ngram_db.fetchone()
    if result:
        ngram_count = result[0]

        return {
            'count': ngram_count,
            'tpm': (ngram_count / (ngram_counts[n] / 1000000))
        }
    else:
        return {
            'count': 0,
            'tpm': 0
        }

# def ngram_info(tokens=[]):
#     if type(tokens) is not list:
#         raise ValueError('Tokens must be in list format')
#
#     n = len(tokens)
#     token_ids = []
#     for token in tokens:
#         token_ids.append(token_id(token))
#
#     if -1 in token_ids:
#         return {
#             'count': 0,
#             'tpm': 0
#         }
#
#
#     try:
#         info = ngrams[n].loc[tuple(token_ids)]
#         return {
#             'count': info['count'],
#             'tpm': info['tpm']
#         }
#     except KeyError:
#         return {
#             'count': 0,
#             'tpm': 0
#         }

def tokenizer(sentence):
    segmented = []
    i = 0
    while i < len(sentence):
        probable_word = ''
        for j in range(1, 5):
            probability = ngram_info([sentence[i:i+j].strip()])['tpm']

            if probability > 0:
                probable_word = sentence[i:i+j].strip()

        if probable_word != '':
            segmented.append(probable_word)
        i += max(len(probable_word), 1)

    return segmented

def test_tokenizer():
    test_sentences = [
        '经过训练的狗能担任盲人的向导',
        '猫讨厌狗,而狗也讨厌猫',
        '狗有着高度发达的嗅觉',
        '出版社接到雪片般飞来的订单',
        '城市边缘的新建房屋犹如雨后春笋',
        '他允许我们进入他的房屋检查仪表',
        '原来他没有给房屋投足保险',
        '每个小学生都走上讲台领取奖品',
        '老师叫这个小学生把手放下',
        '迄今为止，考古学界和历史学界都没有明确证实伊特鲁里亚人起源于何处'
    ]

    for test_sentence in test_sentences:
        print(test_sentence)
        segmented = []
        i = 0
        while i < len(test_sentence):
            probable_word = ''
            for j in range(1, 5):
                probability = ngram_info([test_sentence[i:i+j].strip()])['tpm']

                if probability > 0:
                    probable_word = test_sentence[i:i+j].strip()

                    # print('{}: {}'.format(probable_word, probability))

            segmented.append(probable_word)
            i += max(len(probable_word), 1)
            # print()

        print(' | '.join(segmented))

        # for i in range(len(segmented)-1):
        #     prob = ngram_info([segmented[i], segmented[i+1]])['tpm']
        #     print('{} | {}: {}'.format(segmented[i], segmented[i+1], prob))

        print()

        # if i == len(test_sentence)-3:
        #     break

def download_jukuu_page(word, page_no):
    request_url = ''.join([
        'http://www.jukuu.com/show-',
        str(word),
        '-',
        str(page_no),
        '.html'
    ])

    sentences = []
    with closing(requests.get(request_url)) as r:
        page_text = r.text
        if 'MySQL server error report' in page_text:
            return []

        page_html = html.document_fromstring(clean_html(page_text))

        defs_table = next(iter(next(iter(page_html.get_element_by_id('Table1')))))
        en = defs_table.find_class('e')
        cn = defs_table.find_class('c')

        for i in range(len(en)):
            en_sentence = en[i].text_content().strip()
            cn_sentence = cn[i].text_content().strip()

            en_sentence = en_sentence[en_sentence.index('.  ')+3:]

            if re.search(r'[，、,?\[\]()0-9？]', cn_sentence):
                continue
            # if '，' in cn_sentence:
            #     continue
            # elif '、' in cn_sentence:
            #     continue
            # elif ',' in cn_sentence:
            #     continue
            if cn_sentence.find('。', 0, -1) != -1:
                continue
            if cn_sentence.count(word) > 1:
                continue

            sentences.append({'en': en_sentence, 'cn': cn_sentence})
            # print(en_sentence)
            # print(cn_sentence)
            # print()

    return sentences

def download_jukuu_word(word):

    sentences = []
    for i in range(20):
        downloaded = download_jukuu_page(word, i)

        if downloaded == []:
            return sentences

        sentences += downloaded

    return sentences

def transition_frequencies(sentence):
    pairs = len(sentence) - 1
    if pairs < 1:
        return -1

    freq_sum = 0

    # print(sentence)

    w1 = None
    w2 = None

    for word in sentence:
        if not w2:
            w2 = word
            continue

        w1 = w2
        w2 = word

        ngram = ngram_info([w1, w2])
        tpm = ngram['tpm']

        # print('{}\t{} | {}'.format(w1, w2, tpm))

        freq_sum += tpm

    return (freq_sum/pairs)
    # print('Avg. transition freq: {}'.format(freq_sum/pairs))

def token_frequencies(sentence):
    freq_sum = 0

    for word in sentence:
        ngram = ngram_info([word])
        tpm = ngram['tpm']

        freq_sum += tpm

    return (freq_sum/len(sentence))
    # print('Avg. word freq: {}'.format(freq_sum/len(sentence)))

def token_contexts(n, token):
    ngram_db.execute('''
        SELECT *
        FROM ngrams_{0}
        WHERE ngrams_{0}.token{0} = {1}
    '''.format(n, token))

    cols = ['token{}'.format(x+1) for x in range(n-1)] + ['critical_{}'.format(token), 'count_{}'.format(token)]
    contexts = pd.DataFrame(ngram_db.fetchall(), columns=cols).set_index(cols[:-2])

    if len(contexts) == 0:
        return None

    contexts = contexts.drop('critical_{}'.format(token), 1)
    contexts['freq_{}'.format(token)] = contexts[cols[-1]] / contexts.sum(numeric_only=True)[cols[-1]] * 100
    # contexts = contexts.set_index(cols[:-1])

    # print(contexts)
    return contexts

def compare_context(n, t1, t2):
    context1 = token_contexts(n, t1)
    context2 = token_contexts(n, t2)

    if context1 is None and context2 is None:
        return 1
    elif context1 is None or context2 is None:
        return 0

    merged = context1.merge(context2, 'outer', left_index=True, right_index=True).fillna(0)

    # corr = abs(merged['freq_{}'.format(t1)].corr(merged['freq_{}'.format(t2)]))
    # print('Compared context. Correlation: {}'.format(corr))
    return abs(merged['freq_{}'.format(t1)].corr(merged['freq_{}'.format(t2)]))

    # overlap = merged.loc[merged['count_{}'.format(t1)] != 0].loc[merged['count_{}'.format(t2)] != 0]
    #
    #
    # print(merged['freq_{}'.format(t1)].corr(merged['freq_{}'.format(t2)]))
    # print(overlap.sum())

def compare_contexts(t1, t2):
    tot_corr = 0
    for i in range(2):
        tot_corr += compare_context(5-i, t1, t2)

    return tot_corr / 2


print('Loading 2grams...')
get_ngram_counts(2)

# print('Loading 3grams...')
# get_ngram_counts(3)
#
# print('Loading 4grams...')
# get_ngram_counts(4)
#
# print('Loading 5grams...')
# get_ngram_counts(5)

# print('Dog and book:')
# print(compare_contexts(token_id('狗'), token_id('书')))
#
# print('\nDog and blue:')
# print(compare_contexts(token_id('狗'), token_id('青')))
#
# print('\nDog and cat:')
# print(compare_contexts(token_id('狗'), token_id('猫')))
#
# print('\nDog and mouse:')
# print(compare_contexts(token_id('狗'), token_id('鼠')))
#
# print('\nDog and walk:')
# print(compare_contexts(token_id('狗'), token_id('走')))
# quit()
#
# test_words = [
#     '狗',
#     '猫',
#     '屋',
#     '书',
#     '汽车',
#     '小学'
# ]
#
# stim_words = []
# stim_set = pathlib.Path('stim_set.csv')
# with stim_set.open('r', newline='', encoding='utf-8') as csvfile:
#     csvreader = csv.reader(csvfile, delimiter=',', lineterminator='\n')
#
#     line = 0
#     for i in range(int(20585/5)):
#         quartet = {
#             'critical': next(csvreader),
#             'both_sim': next(csvreader),
#             'orth_sim': next(csvreader),
#             'phon_sim':  next(csvreader),
#             'both_diff': next(csvreader)
#         }
#
#         stim_words.append(quartet)
#
#         line += 1
#         if line > 30:
#             break
#
# # for word in test_words:
# for row in stim_words:
#     total_downloaded = 0
#     word = row['critical'][0]
#     definition = row['critical'][1]
#
#     sentences = download_jukuu_word(word)
#     possible_sentences = []
#
#     for sentence in sentences:
#         total_downloaded += 1
#         tokenized = tokenizer(sentence['cn'])
#
#         if word not in tokenized:
#             # print('Word not tokenized: {}'.format(' | '.join(tokenized)))
#             continue
#
#         ix = tokenized.index(word)
#         if ix < 2:
#             # print('Token too early: {}'.format(sentence['en']))
#             continue
#
#         new_sentence = {
#             'en': sentence['en'],
#             'cn': sentence['cn'],
#             'tokenized': tokenized,
#             'transition_freqs': transition_frequencies(tokenized),
#             'token_freqs': token_frequencies(tokenized)
#         }
#
#
#         possible_sentences.append(new_sentence)
#
#     print('Word {} ({})\nAccepted: {}; Rejected: {}\n'.format(word, definition, len(possible_sentences), total_downloaded-len(possible_sentences)))
#
#     possible_sentences = sorted(possible_sentences, key=lambda s: s['transition_freqs'], reverse=True)
#     possible_sentences = possible_sentences[:3]
#
#     for sentence in possible_sentences:
#         print(sentence['en'])
#         print(sentence['cn'])
#         print(sentence['tokenized'])
#         print('Avg. transition freq: {}'.format(sentence['transition_freqs']))
#         print('Avg. token freq: {}'.format(sentence['token_freqs']))
#         print()
#
#         ix = sentence['tokenized'].index(word)
#
#         print(sentence['tokenized'][ix-2:ix] + [row['both_sim'][0]])
#         print('Both sim: {}\tTransition freq: {}'.format(row['both_sim'][0], transition_frequencies(sentence['tokenized'][ix-2:ix] + [row['both_sim'][0]])))
#         print('Orth sim: {}\tTransition freq: {}'.format(row['orth_sim'][0], transition_frequencies(sentence['tokenized'][ix-2:ix] + [row['orth_sim'][0]])))
#         print('Phon sim: {}\tTransition freq: {}'.format(row['phon_sim'][0], transition_frequencies(sentence['tokenized'][ix-2:ix] + [row['phon_sim'][0]])))
#         print('Both diff: {}\tTransition freq: {}\n'.format(row['both_diff'][0], transition_frequencies(sentence['tokenized'][ix-2:ix] + [row['both_diff'][0]])))
