import asyncio, pickle, re
from pandas import DataFrame, read_csv, concat
from lib.expr import Marker
from lib.datestr import num as datenum, month
from lib.datestr import MREGEX
from lib.log import log, notify
try:

  log("✨")
  notify(__file__, "✨")

  qURL=r'(?:http[s]?://(?:\w|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+)'
  Q0, Q = ['alnum', 'num', 'series'], {
    "month":  MREGEX,
    "alnum":  r"(?<!\w)(?:[^\W\d]+\d|\d+[^\W\d])\w*(?!\w)",
    "series": "(?:" + '|'.join([rf"(?:\d+\s*{s}+\s*)+" for s in ['\.', '\-', '/', '\\', '\s']]) + ")\s*\d+",
    "num":    r"(?<!\w)\d+(?!\w)", "space":  r"[\s\-\/\\\.]+",
    "braced": '|'.join([rf"\{a}\w{{1,4}}\{b}" for a,b in ['()','{}','[]', '""', '<>']]),
    "abbr":   r"(?<!\w)[^\W\d]{1,4}\.?(?!\w)",
  }

  p0 = r'(?P<country>(?<![^\W])[a-zA-Z]{2}(?![^\W\d]))' + \
      r'(?P<prefix>(?:[^\W\d]|[\.\s]){,5})?' + \
      r'(?P<number>(?:\d\W?\s?){5,})(?!\d)' + \
      r'(?P<suffix>[\W\s]{,3}[^\w\s]*[0123abuABUXY][^\w\s\)\}\]]*[0123a-zA-Z]?[^\w\s]*)?'

  M = Marker(Q, Q0)

  X = read_csv('raport.uprp.gov.pl.csv').reset_index()\
    .rename(columns={'docs':'query', 'index':'entry'})[['entry', 'query']]\
    .query('~ query.duplicated(keep="first")')

  C = X.progress_apply(lambda x: [(x['entry'], t, q) for t, _, _, q in M.union(x['query'])], axis=1)
  C = DataFrame(C.explode().tolist(), columns=['entry', 'query', 'numerical'])\
    .dropna().query('numerical==True').drop(columns="numerical").convert_dtypes()

  D0 = C.progress_apply(lambda x: [(x['entry'], t, d, m, y) for _, _, t, d, m, y in datenum(x['query'])], axis=1) + \
      C.progress_apply(lambda x: [(x['entry'], t, d, m, y) for _, _, t, d, m, y in month(x['query'])], axis=1)
  D = DataFrame([y for u in D0 if u for y in u], columns=['entry', 'query', 'day', 'month', 'year'])\
    .convert_dtypes().drop_duplicates(subset=['entry', 'day', 'month', 'year'])

  P = C.progress_apply(lambda x: [{ "entry": x['entry'], **m.groupdict() } for m in re.finditer(p0, x['query'])], axis=1)
  P = DataFrame(P.explode().dropna().tolist()).convert_dtypes()

  with open('queries.pkl', 'wb') as f: 
    pickle.dump({ 'raw': X, 'codes': P, 'dates': D }, f)

  notify("✅")

except Exception as e: log("❌", e); notify("❌")