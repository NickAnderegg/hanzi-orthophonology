from ideograph import functors
import idsparser
import operator
import threading
import multiprocessing as mp
import time
from collections import deque
import handata
import pathlib
import csv
import statistics
import random
random.seed()

class TreeCompare:

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

    # def start_manager(self):
    #     self.manager = mp.Manager()
    #     #self.manager.start()
    #     self.node_comparisons = self.manager.dict()
    #     self.char_comparisons = self.manager.dict()

    def compare_nodes(self, a, b):
        """Compare two IDSNodes and return their similarity value"""

        if frozenset([a.ids,b.ids]) in self.node_comparisons:
            """If the nodes have already been compared, return the comparison"""
            return self.node_comparisons[frozenset([a.ids,b.ids])]

        if a is b:
            """If the nodes point to the same node object, return 1"""
            self._add_node_comparison(a.ids, b.ids, 1)
            return 1
        elif a.head != b.head:
            """
            If the heads are different...
                - Catches IDS sequences beginning with different functors
                - Catches two different individual Characters
                - Catches the comparison of an IDS tree to an individual character
            """

            if a.children and b.children:
                """
                If both nodes have children...
                    - Catches IDS sequences beginning with different functors
                """
                if a in b.children or b in a.children:
                    """If one node is a child of the other node"""
                    if a in b.children:
                        child = a
                        parent = b
                    else:
                        child = b
                        parent = a

                    stroke_proportions = self._stroke_proportions(child, parent)

                    total = 0
                    for i in range(len(parent.children)):
                        if child is parent.children[i]:
                            weighted_similarity = self._weighted_similarity(
                                self.weights[parent.head][i],
                                1,
                                stroke_proportions['b'][i]
                            )
                            total += weighted_similarity
                        else:
                            weighted_similarity = self._weighted_similarity(
                                self.weights[parent.head][i],
                                0,
                                stroke_proportions['b'][i]
                            )
                            total += weighted_similarity

                    self._add_node_comparison(a.ids, b.ids, total)
                    # print('PARENT/CHILD SUBSET: {}\t{}'.format(a.ids, b.ids))
                    # print('{}\t{}\t|{}'.format(a.ids, b.ids, total))
                    return total
                elif {a.head, b.head} <= functors.parallel:
                    """
                    Handle the comparison of a binary and ternary pair
                        - This top level statement catches them parallel or perpendicular
                    """
                    if {a.head, b.head} <= functors.horizontal or {a.head, b.head} <= functors.vertical:
                        """
                        Handle a binary and ternary pair going in the same direction
                        """
                        if len(a.children) > len(b.children):
                            bin_node = b
                            ter_node = a
                        elif len(a.children) < len(b.children):
                            bin_node = a
                            ter_node = b
                        else:
                            print('ERROR! {}\t{}'.format(a.ids,b.ids))

                        total = 0
                        stroke_proportions = self._stroke_proportions(bin_node, ter_node)
                        if bin_node.children in [ter_node.children[0:2], ter_node.children[1:3]]:
                            sub_start = [ter_node.children[0:2], ter_node.children[1:3]].index(bin_node.children)
                            for i in range(3):
                                if sub_start <= i <= (sub_start+1):
                                    weighted_similarity = self._weighted_similarity(
                                        self.weights[ter_node.head][i],
                                        stroke_proportions['b'][i],
                                        stroke_proportions['b'][i]
                                    )
                                    total += weighted_similarity
                                else:
                                    weighted_similarity = self._weighted_similarity(
                                        self.weights[ter_node.head][i],
                                        0,
                                        stroke_proportions['b'][i]
                                    )
                                    total += weighted_similarity

                            self._add_node_comparison(bin_node.ids, ter_node.ids, total)
                            print('{}\t{}\t|{}'.format(bin_node.ids, ter_node.ids, total))
                            return total
                        return 0
                    else:
                        """Mismatched vertical and horizontal functors"""
                        total = 0
                        stroke_proportions = self._stroke_proportions(a, b)
                        for i in range(len(a.children)):
                            for j in range(len(b.children)):
                                weighted_similarity = self._weighted_similarity(
                                    ((self.weights[a.head][i] * self.weights[b.head][j])),
                                    stroke_proportions['a'][i],
                                    stroke_proportions['b'][j]
                                )
                                total += (self._compare_base_components(a.children[i].head, b.children[j].head) * weighted_similarity)
                                # total += (self.compare_nodes(a.children[i], b.children[j]) * weighted_similarity)
                        # print('{}\t{}\t|{}'.format(a.ids, b.ids, total))
                        self._add_node_comparison(a.ids, b.ids, total)
                        return total
                elif {a.head, b.head} <= functors.surrounding.difference('⿻'):
                    """Compare two nodes with surrounding functors"""
                    total = 0
                    stroke_proportions = self._stroke_proportions(a, b)
                    for i in range(2):
                        weighted_similarity = self._weighted_similarity(
                            self.weights[a.head][i],
                            stroke_proportions['a'][i],
                            stroke_proportions['b'][i]
                        )
                        total += (self.compare_nodes(a.children[i], b.children[i]) * weighted_similarity)
                    # print('{}\t{}\t|{}'.format(a.ids, b.ids, total))
                    self._add_node_comparison(a.ids, b.ids, total)
                    return total
            elif a.head in functors.all or b.head in functors.all:
                """
                Elimination of (for now) uncomparable node types:
                    - A tree and an individual character
                """
                self._add_node_comparison(a.ids, b.ids, 0)
                return 0
            else:
                """Compare two individual characters/base components"""
                similarity = self._compare_base_components(a.head, b.head)
                self._add_node_comparison(a.ids, b.ids, similarity)
                return similarity
        else:
            """Compare two IDSNode trees with the same functor"""

            total = 0
            if a.children and b.children:
                # stroke_proportions = {
                #     'a': [],
                #     'b': []
                # }
                #
                # a_strokes = a.get_strokes()
                # b_strokes = b.get_strokes()

                stroke_proportions = self._stroke_proportions(a, b)
                for i in range(len(a.children)):
                    # stroke_proportions['a'].append(a.children[i].get_strokes() / a_strokes)
                    # stroke_proportions['b'].append(b.children[i].get_strokes() / b_strokes)

                    weighted_similarity = self._weighted_similarity(
                        self.weights[a.head][i],
                        stroke_proportions['a'][i],
                        stroke_proportions['b'][i]
                    )
                    if a.children[i] is b.children[i]:
                        total += weighted_similarity
                        # total += self._weighted_similarity(
                        #     self.weights[a.head][i],
                        #     stroke_proportions['a'][i],
                        #     stroke_proportions['b'][i]
                        # )
                    else:
                        total += (self.compare_nodes(a.children[i], b.children[i]) * weighted_similarity)

                if (max(sum(stroke_proportions['a']), sum(stroke_proportions['b'])) > 1.02 or
                    min(sum(stroke_proportions['a']), sum(stroke_proportions['b'])) < 0.98):
                    print(stroke_proportions)
                    raise ValueError('Stroke proportion calculation failed')
            self._add_node_comparison(a.ids, b.ids, total)
            return total

        if random.randint(1,100) == 5:
            print('Uncomparable: {}\t|\t{}'.format(a.ids, b.ids))
        return 0

    def _stroke_proportions(self, a, b):
        stroke_proportions = {
            'a': [],
            'b': []
        }

        a_strokes = a.get_strokes()
        b_strokes = b.get_strokes()
        for child in a.children:
            stroke_proportions['a'].append(child.get_strokes() / a_strokes)
        for child in b.children:
            stroke_proportions['b'].append(child.get_strokes() / b_strokes)

        return stroke_proportions

    def _weighted_similarity(self, weight, proportion_a, proportion_b):
        max_prop = max(proportion_a, proportion_b)
        min_prop = min(proportion_a, proportion_b)

        return weight * (min_prop / max_prop)

    def _compare_base_components(self, a, b):
        if a not in handata.unihan or b not in handata.unihan:
            return 0
        elif 'rs_kangxi' in handata.unihan[a] and 'rs_kangxi' in handata.unihan[b]:
            rs_a = [int(x) for x in handata.unihan[a]['rs_kangxi'].split('.')]
            rs_b = [int(x) for x in handata.unihan[b]['rs_kangxi'].split('.')]

            radical_a = handata.radicals[rs_a[0]]
            radical_b = handata.radicals[rs_b[0]]

            if rs_a[0] == rs_b[0]:
                return 0.25 + 0.25*(min(rs_a[1], rs_b[1])/max(rs_a[1], rs_b[1], 1))
            elif 'total_strokes' in handata.unihan[radical_a] and 'total_strokes' in handata.unihan[radical_b]:
                a_strokes = int(handata.unihan[radical_a]['total_strokes'])
                b_strokes = int(handata.unihan[radical_b]['total_strokes'])

                radical_prop    = min(a_strokes, b_strokes)/max(a_strokes, b_strokes, 1)
                additional_prop = min(rs_a[1], rs_b[1]) / max(rs_a[1], rs_b[1], 1)

                return 0.25*radical_prop + 0.25*additional_prop
        else:
            return 0

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
        #keys = [x[0] for x in sorted([list(x) for x in list(self._nodes.items())], key=lambda node: node[1].get_depth())]
        keys = sorted(list(self._nodes.keys()))
        #print([x.get_depth() for x in sorted([list(x)[1] for x in list(self._nodes.items())], key=operator.methodcaller('get_depth'))])
        if chunks <= 1:
            self._compare_nodes_process(keys)
        else:
            #self.comparer.start_manager()
            processes = deque()
            per_process = int(len(keys) / chunks)
            for i in range(chunks):
                for j in range(i+1):
                    a_beg = i*per_process
                    b_beg = j*per_process
                    if i == chunks - 1:
                        a_end = len(keys)
                    else:
                        a_end = (i+1)*per_process
                    if j == chunks - 1:
                        b_end = len(keys)
                    else:
                        b_end = (j+1)*per_process
                    process = threading.Thread(target=self._compare_nodes_process, name='Node comparison: {}-{} to {}-{}'.format(a_beg, a_end, b_beg, b_end), args=[keys, a_beg, a_end, b_beg, b_end])
                    processes.append(process)
                    if threading.active_count() < (chunks * 2):
                        process = processes.popleft()
                        process.start()
                        #print('{} started'.format(process.name))

            while len(processes) > 0:
                if threading.active_count() < chunks:
                    process = processes.popleft()
                    process.start()
                    #print('{} started'.format(process.name))
                else:
                    time.sleep(0.25)

            while next((x for x in threading.enumerate() if x != threading.current_thread()), False):
                time.sleep(0.5)

            # cpu_count = mp.cpu_count()
            # for i in range(len(processes)):
            #     processes[i].start()
            #     print('{} started'.format(processes[i].name))
            #     children = mp.active_children()
            #     if len(children) >= (cpu_count * 3):
            #         child = next((x for x in children if type(x) == mp.Process), None)
            #         if child:
            #             child.join()
            #             print('{} finished'.format(child.name))
            #
            # child = next((x for x in mp.active_children() if type(x) == mp.Process), None)
            # while not child == None:
            #     child.join()
            #     print('{} finished'.format(child.name))
            #     child = next((x for x in mp.active_children() if type(x) == mp.Process), None)
            #
            # print('Node processing done')

            #print('Number of threads: {}'.format(threading.active_count()))
            #for thread in threads:
                #print('Number of threads: {}'.format(threading.active_count()))
                #thread.join()
                #print('Node thread "{}" finished'.format(thread.name))
            #print('Number of threads running: {}'.format(threading.active_count()))
            #print('Exiting')

    def _compare_nodes_process(self, keys, a_beg=0, a_end=-1, b_beg=0, b_end=-1):
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
            t1 = time.perf_counter()
            self._compare_nodes(chunks)
            t2 = time.perf_counter()
            print('Process time: {}s'.format((t2-t1)))
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
                    #print('Thread started: {}'.format(thread.name))
                    threads.append(thread)

            #print('Number of threads: {}'.format(threading.active_count()))
            for thread in reversed(threads):
                #print('Number of threads: {}'.format(threading.active_count()))
                thread.join()
                #print('{} finished'.format(thread.name))
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
        print('Printing character comparisons...')
        comparisons = self.comparer.char_comparisons.items()
        sorted_comps = [x for x in sorted(comparisons, key=operator.itemgetter(1), reverse=rev) if x[1] > 0]
        print('Characters sorted...')
        sorted_comps = sorted_comps[(len(sorted_comps)-int(len(sorted_comps)/20)):]
        print('Dividing sorted comparisons...')
        for comp in sorted_comps:
            if len(comp[0]) > 1:
                print('{}\t{} | {}'.format(list(comp[0])[0], list(comp[0])[1], comp[1]))

    def output_char_comparisons(self, rev=True):
        print('Outputting character comparisons to file...')
        comparisons = self.comparer.char_comparisons.items()
        sorted_comps = [x for x in sorted(comparisons, key=operator.itemgetter(1), reverse=rev)]
        print('Characters sorted')
        count = 0
        char_comps = pathlib.Path('data/character_comparisons.csv')
        with char_comps.open('w', encoding='utf-8', newline='') as csvfile:
            csvwriter = csv.writer(csvfile, lineterminator='\n')

            for row in sorted_comps:
                if len(row[0]) > 1:
                    row = [list(row[0])[0], list(row[0])[1], row[1]]
                    csvwriter.writerow(row)
                    count += 1

        print('{} comparisons in file'.format(count))

class IDSTree():

    def __init__(self, parse_tree, dictionary=None):
        self.head = parse_tree[0]
        self.ids = idsparser.unparse(parse_tree[1])

        self._dictionary = dictionary

        self.tree = IDSNode(parse_tree[1], dictionary=self._dictionary)
        if self._dictionary:
            self._dictionary._nodes[self.head] = self.tree
            self._dictionary._nodes[self.ids] = self.tree

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

    def get_strokes(self):
        """Calculate the number of strokes in this node tree"""
        if self.children is None:
            if self.head not in handata.unihan:
                return 1
            elif 'total_strokes' not in handata.unihan[self.head]:
                return 1
            else:
                return int(handata.unihan[self.head]['total_strokes'])
        else:
            return sum([x.get_strokes() for x in self.children])

    def print_tree(self, depth=-1):
        if depth == -1:
            print(self.head)
        else:
            print(''.join(['    ' * depth, '└---', self.head]))

        if self.children:
            for child in self.children:
                child.print_tree(depth+1)
