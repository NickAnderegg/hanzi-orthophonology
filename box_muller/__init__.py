import math
import requests
from collections import deque

def _get_random():
    randparams = {
        'num':'50',
        'min':'1',
        'max':'1000',
        'col':'1',
        'base':'10',
        'format':'plain',
        'rnd':'new'
    }
    r = requests.get('https://www.random.org/integers/', params=randparams)
    resp = r.text.split('\n')
    print('Got random numbers...')
    try:
        resp = [int(x) for x in resp[0:-1]]
        return resp
    except ValueError:
        print('Error retrieving numbers from random.org.')

def box_muller(uniform=None, mean=0, std=1, lower_bound=None, upper_bound=None, integers=True):
    if uniform is None:
        uniform = _get_random()

    dist_min = min(uniform)
    dist_max = max(uniform)

    if dist_min <= 0:
        raise ValueError('Minimum value of uniform distribution must be greater than 0')
    elif dist_max > 1:
        dist = deque([(float(x)/dist_max) for x in uniform])
    else:
        dist = deque(uniform)

    transformed = []
    while len(dist) > 0:
        val1 = dist.pop()
        val2 = dist.pop()
        trans1 = std*(math.sqrt(-2 * math.log(val1)) * math.cos(2 * math.pi * val2)) + mean
        trans2 = std*(math.sqrt(-2 * math.log(val1)) * math.sin(2 * math.pi * val2)) + mean

        if integers:
            trans1 = round(trans1)
            trans2 = round(trans2)

        transformed.append(trans1)
        transformed.append(trans2)

    if lower_bound is not None:
        transformed = [x for x in transformed if x > lower_bound]
    if upper_bound is not None:
        transformed = [x for x in transformed if x <= upper_bound]

    return transformed
