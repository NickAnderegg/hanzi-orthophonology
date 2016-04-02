import pathlib
import csv
import json
import re
import random
from collections import UserDict
import math
import decimal
from decimal import Decimal
import statistics
random.seed()

fields = {
    'kAccountingNumeric': 'numeric',
    'kBigFive':     'big_five',
    'kCangjie':     'cangjie',
    'kCantonese':   'cantonese',
    'kCCCII':       'cccii',
    'kCheungBauer': 'cheung_bauer',
    'kCheungBauerIndex':    'cheung_bauer_index',
    'kCihaiT': 'cihait',
    'kCNS1986': 'cns1986',
    'kCNS1992': 'cnd1992',
    'kCompatibilityVariant': 'compatibility_variant',
    'kCowles': 'cowles',
    'kDaeJaweon': 'dae_jaweon',
    'kDefinition': 'definition',
    'kEACC': 'eacc',
    'kFenn': 'fenn',
    'kFennIndex': 'fenn_index',
    'kFourCornerCode': 'four_corner',
    'kFrequency': 'frequency',
    'kGB0': 'gb0',
    'kGB1': 'gb1',
    'kGB3': 'gb3',
    'kGB5': 'gb5',
    'kGB7': 'gb7',
    'kGB8': 'gb8',
    'kGradeLevel': 'grade_level',
    'kGSR': 'gsr',
    'kHangul': 'hangul',
    'kHanYu': 'hanyu',
    'kHanyuPinlu': 'hanyu_pinlu',
    'kHanyuPinyin': 'hanyu_pinyin',
    'kHDZRadBreak': 'hdz_rad_break',
    'kHKGlyph': 'hk_glyph',
    'kHKSCS': 'hkscs',
    'kIBMJapan': 'ibm_japan',
    'kIICore': 'iicore',
    'kJapaneseKun': 'japanese_kun',
    'kJapaneseOn': 'japanese_on',
    'kJis0': 'jis0',
    'kJis1': 'jis1',
    'kJIS0213': 'jis2013',
    'kKangXi': 'kangxi',
    'kKarlgren': 'karlgren',
    'kKorean': 'korean',
    'kKPS0': 'kps0',
    'kKPS1': 'kps1',
    'kKSC0': 'ksc0',
    'kKSC1': 'ksc1',
    'kLau': 'lau',
    'kMainlandTelegraph': 'mainland_telegraph',
    'kMandarin': 'mandarin',
    'kMatthews': 'matthews',
    'kMeyerWempe': 'meyer_wempe',
    'kMorohashi': 'morohashi',
    'kNelson': 'nelson',
    'kOtherNumeric': 'numeric',
    'kPhonetic': 'phonetic',
    'kPrimaryNumeric': 'numeric',
    'kRSAdobe_Japan1_6': 'rs_adobe',
    'kRSJapanese': 'rs_japanese',
    'kRSKangXi': 'rs_kangxi',
    'kRSKanWa': 'rs_kanwa',
    'kRSKorean': 'rs_korean',
    'kRSUnicode': 'rs_unicode',
    'kSBGY': 'sbgy',
    'kSemanticVariant': 'semantic_variant',
    'kSimplifiedVariant': 'simplified_variant',
    'kSpecializedSemanticVariant': 'specialized_semantic',
    'kTaiwanTelegraph': 'taiwan_telegraph',
    'kTang': 'tang',
    'kTotalStrokes': 'total_strokes',
    'kTraditionalVariant': 'traditional_variant',
    'kVietnamese': 'vietnamese',
    'kXerox': 'xerox',
    'kXHC1983': 'xhc1983',
    'kZVariant': 'z_variant'
}

def load_unihan():
    unihan = dict()
    unihan_folder = pathlib.Path('data/unihan')
    for child in unihan_folder.iterdir():
        with child.open('r', encoding='utf-8', newline='') as csvfile:
            csvreader = csv.reader(csvfile, delimiter='\t')
            for row in csvreader:
                if len(row) <= 0:
                    pass
                elif row[0][0] == '#':
                    pass
                elif row[0][0:2] == 'U+':
                    char = chr(int(row[0][2:], 16))
                    if char not in unihan:
                        unihan[char] = {}

                    field = row[1]
                    value = row[2]

                    if field in fields:
                        unihan[char][fields[field]] = value

    return unihan

def load_cedict():
    entry = re.compile(r"^(?P<traditional>\w+)\s{1}(?P<simplified>\w+)\s{1}\[(?P<pinyin>.+)\]\s{1}(?P<definitions>.+)")

    cedict = dict()
    cedict_file = pathlib.Path('data/cedict_1_0_ts_utf-8_mdbg.txt')
    with cedict_file.open('r', encoding='utf-8') as dictreader:
        count = 0
        for line in dictreader:
            if line[0] == '#':
                pass
            else:
                match = entry.match(line)
                if match is not None:
                    simplified = match.group('simplified')
                    if simplified not in cedict:
                        cedict[simplified] = dict()
                        cedict[simplified]['simplified'] = match.group('simplified')
                        cedict[simplified]['traditional'] = match.group('traditional')
                        cedict[simplified]['pinyin'] = match.group('pinyin')
                        cedict[simplified]['definitions'] = match.group('definitions')[1:-1].split('/')

    cedictchars = list()
    for word in cedict:
        chars = tuple(word)
        for char in chars:
            if char not in cedictchars:
                cedictchars.append(char)
    return cedict, cedictchars

def load_hsk():
    hsk = dict()
    hskwords = dict()
    hsk['word'] = hskwords
    hsk_file = pathlib.Path('data/hsk_words.csv')
    with hsk_file.open('r', encoding='utf-8', newline='') as csvfile:
        csvreader = csv.reader(csvfile, delimiter='\t')
        for row in csvreader:
            if len(row) <= 0:
                pass
            elif row[0][0] == '#':
                pass
            else:
                level, simp, trad, pinyin_num, pinyin_mark, definition = tuple(row)
                if simp not in hskwords:
                    hskwords[simp] = {
                        'level':        level,
                        'simplified':   simp,
                        'traditional':  trad,
                        'pinyin':       pinyin_num,
                        'pinyin_num':   pinyin_num,
                        'pinyin_mark':  pinyin_mark,
                        'definition':   definition
                    }

    hskchars = dict()
    hsk['char'] = hskchars
    for word in hskwords:
        chars = tuple(word)
        for char in chars:
            if char not in hskchars:
                hskchars[char] = {
                    'level':        hskwords[word]['level']
                }

    return hsk

def load_subtlex():
    subtlex = dict()
    subtlex_folder = pathlib.Path('data/subtlex-ch')
    for child in subtlex_folder.iterdir():
        if 'csv' not in child.suffix:
            continue
        with child.open('r', encoding='utf-8', newline='') as csvfile:
            corpus_type = None
            if 'CHR' in child.stem:
                corpus_type = 'char'
            elif 'WF' in child.stem:
                corpus_type = 'word'
            else:
                raise ValueError('Incompatible or unknown SUBTLEX-CH corpus type')

            corpus = dict()
            subtlex[corpus_type] = corpus
            csvreader = csv.reader(csvfile, delimiter='\t')

            token_string = next(csvreader)[0]
            tokens = int(''.join([x for x in token_string if x.isdigit()]))
            corpus['tokens'] = tokens

            context_string = next(csvreader)[0]
            contexts = int(''.join([str(x) for x in context_string if x.isdigit()]))
            corpus['contexts'] = contexts
            # Skip headers
            next(csvreader)

            # Process rows
            for row in csvreader:
                token, count, tpm, log_count, cd, cd_percent, log_cd = tuple(row)

                corpus[token] = {
                    'token':    token,
                    'count':    count,
                    'tpm':      tpm,
                    'log_count':log_count,
                    'contexts': cd,
                    'contexts_percent': cd_percent,
                    'log_contexts':     log_cd
                }

    return subtlex

def load_radicals():
    radical_dict = {
        1: '一', 2: '丨', 3: '丶', 4: '丿', 5: '乙', 6: '亅', 7: '二', 8: '亠', 9: '人', 10: '儿',
        11: '入', 12: '八', 13: '冂', 14: '冖', 15: '冫', 16: '几', 17: '凵', 18: '刀', 19: '力', 20: '勹',
        21: '匕', 22: '匚', 23: '匸', 24: '十', 25: '卜', 26: '卩', 27: '厂', 28: '厶', 29: '又', 30: '口',
        31: '囗', 32: '土', 33: '士', 34: '夂', 35: '夊', 36: '夕', 37: '大', 38: '女', 39: '子', 40: '宀',
        41: '寸', 42: '小', 43: '尢', 44: '尸', 45: '屮', 46: '山', 47: '川', 48: '工', 49: '己', 50: '巾',
        51: '干', 52: '幺', 53: '广', 54: '廴', 55: '廾', 56: '弋', 57: '弓', 58: '彐', 59: '彡', 60: '彳',
        61: '心', 62: '戈', 63: '戶', 64: '手', 65: '支', 66: '攴', 67: '文', 68: '斗', 69: '斤', 70: '方',
        71: '无', 72: '日', 73: '曰', 74: '月', 75: '木', 76: '欠', 77: '止', 78: '歹', 79: '殳', 80: '毋',
        81: '比', 82: '毛', 83: '氏', 84: '气', 85: '水', 86: '火', 87: '爪', 88: '父', 89: '爻', 90: '爿',
        91: '片', 92: '牙', 93: '牛', 94: '犬', 95: '玄', 96: '玉', 97: '瓜', 98: '瓦', 99: '甘', 100: '生',
        101: '用', 102: '田', 103: '疋', 104: '疒', 105: '癶', 106: '白', 107: '皮', 108: '皿', 109: '目', 110: '矛',
        111: '矢', 112: '石', 113: '示', 114: '禸', 115: '禾', 116: '穴', 117: '立', 118: '竹', 119: '米', 120: '纟',
        121: '缶', 122: '网', 123: '羊', 124: '羽', 125: '老', 126: '而', 127: '耒', 128: '耳', 129: '聿', 130: '肉',
        131: '臣', 132: '自', 133: '至', 134: '臼', 135: '舌', 136: '舛', 137: '舟', 138: '艮', 139: '色', 140: '艸',
        141: '虍', 142: '虫', 143: '血', 144: '行', 145: '衣', 146: '襾', 147: '见', 148: '角', 149: '讠', 150: '谷',
        151: '豆', 152: '豕', 153: '豸', 154: '贝', 155: '赤', 156: '走', 157: '足', 158: '身', 159: '车', 160: '辛',
        161: '辰', 162: '辵', 163: '邑', 164: '酉', 165: '釆', 166: '里', 167: '金', 168: '长', 169: '门', 170: '阜',
        171: '隶', 172: '隹', 173: '雨', 174: '青', 175: '非', 176: '面', 177: '革', 178: '韦', 179: '韭', 180: '音',
        181: '页', 182: '风', 183: '飞', 184: '饣', 185: '首', 186: '香', 187: '马', 188: '骨', 189: '高', 190: '髟',
        191: '鬥', 192: '鬯', 193: '鬲', 194: '鬼', 195: '鱼', 196: '鸟', 197: '鹵', 198: '鹿', 199: '麦', 200: '麻',
        201: '黃', 202: '黍', 203: '黑', 204: '黹', 205: '黾', 206: '鼎', 207: '鼓', 208: '鼡', 209: '鼻', 210: '齐',
        211: '齿', 212: '龙', 213: '龟', 214: '龠'
    }
    return radical_dict

class _Syllable():

    def __init__(self, pinyin, syllable):
        self.pinyin = pinyin
        self.syllable = syllable

    def __getattr__(self, key):
        if key in self.syllable:
            return self.syllable[key]

    def __getitem__(self, key):
        if key in self.syllable:
            return self.syllable[key]
        elif key is 'pinyin':
            return self.pinyin

    def __repr__(self):
        return str(self.syllable)

    # def count_vowels(self):
    #     return len([1 for x in self.syllable['nucleus'] if x != [0,0,0]])

class _Syllables():

    def __init__(self, syllables):
        self.syllables = {key: _Syllable(key, value) for (key, value) in syllables.items()}

    def __getitem__(self, key):
        if key in self.syllables:
            return self.syllables[key]

    def __iter__(self):
        return iter(self.syllables)

    def slot_distance(self, slot, s1, s2):
        phoneme_slots = {
            1: 'onset',
            2: 'glide',
            3: 'nucleus',
            4: 'coda',
            5: 'tone'
        }

        if type(slot) is int:
            slot = phoneme_slots[slot]

        phoneme1 = self.syllables[s1][slot]
        phoneme2 = self.syllables[s2][slot]

        # if phoneme1 == [0.0, 0.0, 0.0] and phoneme2 == [0.0, 0.0, 0.0]:
        #     return None

        return self._compute_distance(phoneme1, phoneme2)

    # def onset_distance(self, s1, s2):
    #     onset1 = self.syllables[s1].onset
    #     onset2 = self.syllables[s2].onset
    #     return self._compute_distance(onset1, onset2)
    #
    # def coda_distance(self, s1, s2):
    #     coda1 = self.syllables[s1].coda
    #     coda2 = self.syllables[s2].coda
    #     return self._compute_distance(coda1, coda2)
    #
    # def vowel_distances(self, s1, s2):
    #     vowels1 = self.syllables[s1].nucleus
    #     vowels2 = self.syllables[s2].nucleus
    #     distances = []
    #     for i in range(3):
    #         distances.append(self._compute_distance(vowels1[i], vowels2[i]))
    #
    #     return distances
    #
    # def rime_distances(self, s1, s2):
    #     rime1 = self.syllables[s1].nucleus + [self.syllables[s1].coda]
    #     rime2 = self.syllables[s2].nucleus + [self.syllables[s2].coda]
    #     distances = []
    #     for i in range(len(rime1)):
    #         distances.append(self._compute_distance(rime1[i], rime2[i]))
    #
    #     return distances
    #
    # def nucleus_distance(self, s1, s2):
    #     vowels1 = self.syllables[s1].nucleus
    #     vowels2 = self.syllables[s2].nucleus
    #     nucleus1 = []
    #     nucleus2 = []
    #     for i in range(3):
    #         nucleus1.append(statistics.mean([Decimal(x) for x in vowels1[i]]))
    #         nucleus2.append(statistics.mean([Decimal(x) for x in vowels2[i]]))
    #
    #     return self._compute_distance(nucleus1, nucleus2)

    # def tone_distance(self, s1, s2):
    #     tone1 = self.syllables[s1].tone
    #     tone2 = self.syllables[s2].tone
    #     return self._compute_distance(tone1, tone2)

    def all_distances(self, s1, s2):
        distances = []
        for i in range(1,6):
            distance = self.slot_distance(i, s1, s2)
            #if distance is not None:
            distances.append(distance)

        return distances
        # return ([
        #     self.slot_distance('onset', s1, s2),
        #     self.slot_distance('glide', s1, s2),
        #     self.slot_distance('nucleus', s1, s2),
        #     self.slot_distance('coda', s1, s2),
        #     self.slot_distance('tone', s1, s2)
        # ])

    def syllable_distance(self, s1, s2):
        decimal.getcontext().prec = 5
        distances = self.all_distances(s1, s2)
        distances = [Decimal(x) for x in distances]
        # for i in range(5):
        #     # if distances[i] is not None:
        #     distances[i] = Decimal(distances[i])

        # if distances[0] is None and distances[1] is None:
        #     return float(statistics.mean(distances[2:]))
        # elif distances[0] is None:
        #     onset = distances[1]
        # elif distances[1] is None:
        #     onset = distances[0]
        # else:
        if self.syllables[s1]['onset'] == '' and self.syllables[s2]['onset'] == '':
            onset = distances[1]
        else:
            onset = distances[0] + (distances[1]/2)

        # onset = statistics.mean(distances[0:1])

        if self.syllables[s1]['coda'] == '' and self.syllables[s2]['coda'] == '':
            rime = distances[2]
        # elif self.syllables[s1]['coda'] == '' or self.syllables[s2]['coda'] == '':
        #     rime = statistics.mean([distances[2], (distances[3]*2)])
        else:
            rime = statistics.mean(distances[2:4])
        return float(statistics.mean([onset, rime, distances[4]]))

    def _compute_distance(self, l1, l2):
        decimal.getcontext().prec = 5
        if type(l1) is list:
            return float((
                decimal.getcontext().power((Decimal(l2[0]) - Decimal(l1[0])), 2)
                + decimal.getcontext().power((Decimal(l2[1]) - Decimal(l1[1])), 2)
                + decimal.getcontext().power((Decimal(l2[2]) - Decimal(l1[2])), 2)
            ).sqrt())
        else:
            return float(decimal.getcontext().abs(Decimal(l1) - Decimal(l2)))
            # val = float(decimal.getcontext().abs(Decimal(l1) - Decimal(l2)))
            # print('{}, {} = {}'.format(Decimal(l1), Decimal(l2), val))
            # return val

def load_syllables():
    syllables_file = pathlib.Path('data/revsyllables.json')
    with syllables_file.open('r', encoding='utf-8') as f:
        return _Syllables(json.load(f))

# unihan = load_unihan()
# # print(len(unihan))
# #
# cedict, cedictchars = load_cedict()
# # print(len(cedict))
# # print(len(cedictchars))
#
# hsk = load_hsk()
# # print(len(hsk))
# # print(len(hsk['word']))
# # print(len(hsk['char']))
#
# subtlex = load_subtlex()
# # print(len(subtlex['char']))
# # print(len(subtlex['word']))
#
# radicals = load_radicals()

syllables = load_syllables()
