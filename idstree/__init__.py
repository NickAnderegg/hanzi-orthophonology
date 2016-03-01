from ideograph import functors
import idsparser
import operator
import threading

class TreeCompare():

    def __init__(self):
        self.weights = {
            '⿹': (1/3, 2/3),
            '⿴': (1/3, 2/3),
            '⿸': (1/3, 2/3),
            '⿶': (1/3, 2/3),
            '⿵': (1/3, 2/3),
            '⿷': (1/3, 2/3),
            '⿺': (1/3, 2/3),
            '⿻': (1/2, 1/2),
        }

        for x in functors.binary:
            self.weights[x] = (1/2, 1/2)
        for x in functors.ternary:
            self.weights[x] = (1/3, 1/3, 1/3)

        self.node_comparisons = {}
        self.char_comparisons = {}

    def compare_nodes(self, a, b):
        if frozenset([a.ids,b.ids]) in self.node_comparisons:
            return self.node_comparisons[frozenset([a.ids,b.ids])]
        if a is b:
            self._add_node_comparison(a.ids, b.ids, 1)
            return 1
        elif a.head != b.head:
            self._add_node_comparison(a.ids, b.ids, 0)
            return 0
        else:
            total = 0
            if a.children and b.children:
                for i in range(len(a.children)):
                    if a.children[i] is b.children[i]:
                        total += self.weights[a.head][i]
                    else:
                        total += (self.compare_nodes(a.children[i], b.children[i]) * self.weights[a.head][i])

            self._add_node_comparison(a.ids, b.ids, total)
            return total

    def _add_node_comparison(self, a, b, score):
        comparison = frozenset([a, b])
        if comparison not in self.node_comparisons:
            self.node_comparisons[comparison] = score
            return True
        else:
            if self.node_comparisons[comparison] != score:
                raise ValueError('Attempt to set a node comparison score different from already-existing score')
            else:
                return False

    def _add_char_comparison(self, a, b, score):
        comparison = frozenset([a, b])
        if comparison not in self.char_comparisons:
            self.char_comparisons[comparison] = score
            return True
        else:
            if self.char_comparisons[comparison] != score:
                raise ValueError('Attempt to set a character comparison score different from already-existing score')
            else:
                return False
class IDSDict():

    def __init__(self):
        self.charlist = {}
        self._nodes = {}

        self.comparer = TreeCompare()

    def add_ids(self, char, ids):
        if char in self.charlist and ids == self.charlist[char].ids:
            return False
        else:
            self.charlist[char] = IDSTree(idsparser.parse(char, ids), dictionary=self)
            return self.charlist[char]

    def _compare_nodes(self, chunks=1):
        keys = sorted(list(self._nodes.keys()))
        if chunks <= 1:
            self._compare_nodes_thread(keys)
        else:
            threads = []
            per_thread = int(len(keys) / chunks)
            for i in range(chunks):
                for j in range(i, chunks):
                    a_beg = i*per_thread
                    b_beg = j*per_thread
                    if i == chunks - 1:
                        a_end = len(keys)
                    else:
                        a_end = (i+1)*per_thread
                    if j == chunks - 1:
                        b_end = len(keys)
                    else:
                        b_end = (j+1)*per_thread
                    thread = threading.Thread(target=self._compare_nodes_thread, name='Node comparison: {}-{} to {}-{}'.format(a_beg, a_end, b_beg, b_end), args=[keys, a_beg, a_end, b_beg, b_end])
                    thread.start()
                    print('Thread started: {}'.format(thread.name))
                    threads.append(thread)

            #print('Number of threads: {}'.format(threading.active_count()))
            for thread in threads:
                #print('Number of threads: {}'.format(threading.active_count()))
                thread.join()
                print('Node thread "{}" finished'.format(thread.name))
            #print('Number of threads running: {}'.format(threading.active_count()))
            #print('Exiting')

    def _compare_nodes_thread(self, keys, a_beg=0, a_end=-1, b_beg=0, b_end=-1):
        if a_end == -1:
            a_end = len(keys)
        if b_end == -1:
            b_end = len(keys)
        for i in range(a_beg, a_end):
            for j in range(b_beg, b_end):
                self.comparer.compare_nodes(self._nodes[keys[i]], self._nodes[keys[j]])

    def compare_characters(self, chunks=1):
        keys = sorted(list(self.charlist.keys()))
        if chunks <= 1:
            self._compare_nodes(1)
            self._compare_characters_thread(keys)
        else:
            self._compare_nodes(chunks)
            threads = []
            per_thread = int(len(keys) / chunks)
            for i in range(chunks):
                for j in range(i, chunks):
                    a_beg = i*per_thread
                    b_beg = j*per_thread
                    if i == chunks - 1:
                        a_end = len(keys)
                    else:
                        a_end = (i+1)*per_thread
                    if j == chunks - 1:
                        b_end = len(keys)
                    else:
                        b_end = (j+1)*per_thread
                    thread = threading.Thread(target=self._compare_characters_thread, name='Character comparison: {}-{} to {}-{}'.format(a_beg, a_end, b_beg, b_end), args=[keys, a_beg, a_end, b_beg, b_end])
                    thread.start()
                    print('Thread started: {}'.format(thread.name))
                    threads.append(thread)

            #print('Number of threads: {}'.format(threading.active_count()))
            for thread in reversed(threads):
                #print('Number of threads: {}'.format(threading.active_count()))
                thread.join()
                print('Char thread "{}" finished'.format(thread.name))
            #print('Number of threads running: {}'.format(threading.active_count()))
            #print('Exiting')


    def _compare_characters_thread(self, keys, a_beg=0, a_end=-1, b_beg=0, b_end=-1):
        if a_end == -1:
            a_end = len(keys)
        if b_end == -1:
            b_end = len(keys)
        for i in range(a_beg, a_end):
            for j in range(b_beg, b_end):
                similarity = self.comparer.compare_nodes(self.charlist[keys[i]].tree, self.charlist[keys[j]].tree)
                self.comparer._add_char_comparison(keys[i], keys[j], similarity)

    def print_char_comparisons(self, rev=True):
        comparisons = self.comparer.char_comparisons.items()
        sorted_comps = sorted(comparisons, key=operator.itemgetter(1), reverse=rev)

        if rev:
            sorted_comps = reversed(sorted_comps)

        sorted_comps = sorted_comps[(len(sorted_comps)-int(len(sorted_comps)/5)):]
        for comp in sorted_comps:
            if len(comp[0]) > 1:
                print('{}\t{} | {}'.format(list(comp[0])[0], list(comp[0])[1], comp[1]))

class IDSTree():

    def __init__(self, parse_tree, dictionary=None):
        self.head = parse_tree[0]
        self.ids = idsparser.unparse(parse_tree[1])

        self._dictionary = dictionary

        self.tree = IDSNode(parse_tree[1], dictionary=self._dictionary)
        if self._dictionary:
            self._dictionary._nodes[self.head] = self.tree

    def get_depth(self):
        return self.tree.get_depth()

    def print_tree(self):
        print('Character: {}\tIDS: {}'.format(self.head, self.ids))
        self.tree.print_tree()


class IDSNode():

    def __init__(self, parse_tree, dictionary=None):
        self.head = None
        self.ids = None
        self.children = None

        self._dictionary = dictionary

        if len(parse_tree) == 1 and parse_tree not in functors.all:
            if self._dictionary and parse_tree in self._dictionary._nodes:
                self.head = self._dictionary._nodes[parse_tree].head
                self.ids = self._dictionary._nodes[parse_tree].ids
                self.children = self._dictionary._nodes[parse_tree].children
            else:
                self.head = parse_tree
                self.ids = self.head
                if self._dictionary:
                    self._dictionary._nodes[self.head] = self
        elif parse_tree[0] in functors.all:
            self.ids = idsparser.unparse(parse_tree)
            if self._dictionary and self.ids in self._dictionary._nodes:
                self = self._dictionary._nodes[self.ids]
            else:
                self.head = parse_tree[0]
                self.children = []
                for idx, child in enumerate(parse_tree[1]):
                    child_ids = idsparser.unparse(child)
                    if self._dictionary and child_ids in self._dictionary._nodes:
                        self.children.append(self._dictionary._nodes[child_ids])
                    else:
                        self.children.append(IDSNode(child, self._dictionary))

                if self._dictionary and self.ids not in self._dictionary._nodes:
                    self._dictionary._nodes[self.ids] = self

    def get_depth(self):
        if self.children is None:
            return 1
        else:
            return max([x.get_depth() for x in self.children]) + 1

    def print_tree(self, depth=-1):
        if depth == -1:
            print(self.head)
        else:
            print(''.join(['    ' * depth, '└---', self.head]))

        if self.children:
            for child in self.children:
                child.print_tree(depth+1)
