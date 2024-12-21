from datetime import datetime
from lib.expr import Marker
from lib.datestr import num as datenum, month
from lib.datestr import MREGEX
import re

class Query:

  URLalike=r'(?:http[s]?://(?:\w|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+)'
  codealike, marktarget = ['alnum', 'num', 'series'], {
    "month":  MREGEX,
    "alnum":  r"(?<!\w)(?:[^\W\d]+\d|\d+[^\W\d])\w*(?!\w)",
    "series": r"(?:" + r'|'.join([rf"(?:\d+\s*{s}+\s*)+" for s in [r'\.', r'\-', r'/', r'\\', r'\s']]) + r")\s*\d+",
    "num":    r"(?<!\w)\d+(?!\w)", "space":  r"[\s\-\/\\\.]+",
    "braced": '|'.join([rf"\{a}\w{{1,4}}\{b}" for a,b in ['()','{}','[]', '""', '<>']]),
    "abbr":   r"(?<!\w)[^\W\d]{1,4}[\.,]?(?!\w)",
  }

  patentalike = r'(?P<country>(?<![^\W])[a-zA-Z]{2})' + \
                r'(?P<prefix>(?:[\W\s]{0,5}[a-zA-Z]{0,2}[\W\s]{0,5}))?' + \
                r'(?P<number>(?:\d\W?\s?){5,})(?!\d)' + \
                r'(?P<suffix>[\W\s]{,3}[^\w\s]*[0123abuABUXY][^\w\s\)\}\]]*[0123a-zA-Z]?[^\w\s]*)?'

  codemarker = Marker(marktarget, codealike)

  def __init__(self, codes:list, numbers:list[str],
               dates:list[int], years:list[int],
               words:list[str]):

    self.codes = codes
    self.numbers = numbers
    self.dates = dates
    self.years = years
    self.words = words

  def Parse(query:str):

    q = re.sub(Query.URLalike, '', query)

    W = [w for w in ' '.join(re.findall(r'(\w+)', q)).strip().upper().split(' ') if len(w) >= 3]

    X = [(x) for x, _, _, m in Query.codemarker.union(q) if m == True]
    P0 = [m.groupdict() for v in X for m in re.finditer(Query.patentalike, v)]
    P = [''.join(re.findall(r'(\d+)', d['number'])) for d in P0]
    D0 = [(y, m, d) for x in X for _, _, x, d, m, y in datenum(x)] + \
         [(y, m, d) for x in X for _, _, x, d, m, y in month(x)]
    D = [datetime(y, m, d).strftime('%Y-%m-%d')
         for y, m, d in D0 if y is not None and m is not None and d is not None]

    Y = []
    for y, m, d in D0:
      try: Y.append(int(y))
      except: continue

    return Query(P0, P, D, list(set(Y)), W)