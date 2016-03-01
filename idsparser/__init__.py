from pyparsing import Word, Group, Or, OneOrMore, Forward, Dict, ParseException
import unicodedata

bnf = None
def _BNF():
    """
    char        :: all valid CJK characters/symbols/supplements
    binary      :: '0x2FF0' | '0x2FF1' | '0x2FF4'..'0x2FFB'
    ternary     :: '0x2FF2' | '0x2FF3'
    subgroup    :: tergroup | bigroup | char
    bigroup     :: binary subgroup subgroup
    tergroup    :: ternary subgroup subgroup subgroup
    expr        :: [ subgroup ]+
    """
    global bnf
    if not bnf:
        validchars = set(range(0x4E00,0x9FFF+1))                # CJK Unified Ideographs
        validchars = validchars.union(range(0x2E80,0x2EFF+1))   # CJK Radicals Supplement
        validchars = validchars.union(range(0x2F00,0x2FDF+1))   # KangXi Radicals
        validchars = validchars.union(range(0x3000,0x303F+1))   # CJK Symbols and Punctuation
        validchars = validchars.union(range(0x31C0,0x31EF+1))   # CJK Strokes
        validchars = validchars.union(range(0x3400,0x4DBF+1))   # CJK Unified Ideographs Extension A
        validchars = validchars.union(range(0x20000,0x2A6DF+1)) # CJK Unified Ideographs Extension B
        validchars = validchars.union(range(0x2A700,0x2B73F+1)) # CJK Unified Ideographs Extension C
        validchars = validchars.union(range(0x2B740,0x2B81F+1)) # CJK Unified Ideographs Extension D
        validchars = validchars.union(range(0x2B820,0x2CEAF+1)) # CJK Unified Ideographs Extension E
        validchars = validchars.union(range(0xF900,0xFAFF+1))   # CJK Compatibility Ideographs
        validchars = validchars.union(range(0x2F800,0x2FA1F+1)) # CJK Compatibility Ideographs Supplement
        validchars = [chr(x) for x in validchars if _char_exists(chr(x))]

        char = Word(''.join(validchars), exact=1)
        binfunctors = [chr(0x2FF0), chr(0x2FF1)] + [chr(x) for x in range(0x2ff4, 0x2ffb+1)]
        terfunctors = [chr(0x2FF2), chr(0x2FF3)]
        binary = Word(''.join(binfunctors), exact=1)
        ternary = Word(''.join(terfunctors), exact=1)
        subgroup = Forward()

        bigroup = (binary + Group(subgroup + subgroup))
        tergroup = (ternary + Group(subgroup + subgroup + subgroup))

        subgroup <<= (Group(tergroup) | Group(bigroup) | char)
        bnf = subgroup
    return bnf

def _char_exists(char):
    try:
        unicodedata.name(char)
    except:
        return False
    return True

def _pretty(d, indent=0):
    for x in d:
        if type(x) is str and indent == 0:
            print(x)
        elif type(x) is str:
            print('    ' * (indent) + '└---' + x)
        else:
            print('    ' * (indent) + '└---' + x[0])
            _pretty(x[1], indent+1)


parser = _BNF()
def parse(char, ids):
    parsed = parser.parseString(ids)
    parsed = parsed.asList()
    parsed.append(parsed[0])
    parsed[0] = char

    return parsed

def pretty_parse(char, ids):
    _pretty(parse(char, ids))

def unparse(parse_tree):
    flat_form = []
    for x in parse_tree:
        if isinstance(x, str):
            flat_form.append(x)
        else:
            flat_form.append(unparse(x))

    return ''.join(flat_form)
