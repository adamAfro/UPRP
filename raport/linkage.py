from pandas import read_csv, DataFrame, merge
import re

def date(x:str):
  from pandas import NA as missing
  from itertools import product
  import re
  Q1 = re.findall(r'(?<!\d)(\d{1})(?!\d)', x)
  Q1 = [int(q) for q in Q1]
  Q2 = re.findall(r'(?<!\d)(\d{2})(?!\d)', x)
  Q2 = [int(q) for q in Q2]
  D = [q for q in Q1+Q2 if 0 < q < 32]  
  M = [month(x)]
  if (not M) or (M[0] is None):
    M = [d for d in D if 0 < d < 13]
  Q4 = (re.findall(r'(?<!\d)(\d{4})(?!\d)', x))
  Y = Q4 = [int(q) for q in Q4]  
  if not Y: Y = [q for q in Q2]
  Y = [y if y > 99
       else y + 1900 if y < 25
       else y + 2000 for y in Y]
  if D and M:
    n = dict()
    for m in M: n[m] = n.get(m, 0) + 1
    U = product(D, M)
    U = [(a,b) for (a,b) in U if (a != b) or (n[a] > 1) or (n[b] > 1)]
    return [{"d": d, "m": m, "y": y} for d, m in U for y in Y]
  if M: return [{"d": missing, "m": m, "y": y} for m in M for y in Y]
  return [{"d": missing, "m": missing, "y": y} for y in Y]  
def month(x:str):
  for i, M in enumerate(pl):
    if any((m in x) for m in M): return i+1
  for i, M in enumerate(en):
    if any((m in x) for m in M): return i+1
  return None
pl = [["styczeń", "stycznia", "stycz", "sty"],
      ["luty", "lutego", "lut"],
      ["marzec", "marca", "marz", "mar"],
      ["kwiecień", "kwietnia", "kwiec", "kwi", "kw"],
      ["maj", "maja"],
      ["czerwiec", "czerwca", "czerw"],
      ["lipiec", "lipca", "lip"],
      ["sierpień", "sierpnia", "sierp"],
      ["wrzesień", "września", "wrześ"],
      ["październik", "października", "paźdz"],
      ["listopad", "listopada", "list"],
      ["grudzień", "grud"]]
en = [["jan", "january"], 
      ["feb", "february"], 
      ["mar", "march"], 
      ["apr", "april"], 
      ["may"], 
      ["jun", "june"], 
      ["jul", "july"], 
      ["aug", "august"], 
      ["sep", "september"], 
      ["oct", "october"], 
      ["nov", "november"], 
      ["dec", "december"]]

class R:
  X = read_csv('recog/docs.csv', dtype=str)
  X['index'] = X.index.astype(str)
  X.rename(columns={"P": "raport"}, inplace=True)
  E = read_csv('extract/docs.chunks.csv', dtype=str)

  D = E[ E['name'] == "date" ].apply(axis=1, func=lambda r: 
    [{**d, "docs": r['docs'], "raport": r['raport'] } for d in date(r['content'])])
  D = DataFrame([d for l in D for d in l])

  P = E[E['name'] == "patent"].apply(axis=1, func=lambda r: 
    { "P": ''.join(re.findall(r"\d", r['content'])), "docs": r['docs'], "raport": r['raport'] })
  P = DataFrame(P.tolist())

class M:
  P = read_csv('../meta/frames/XML/root/df.csv', dtype=str)["P"].replace("-A", "")
  D = read_csv('../meta/frames/dates.csv', dtype=str)
  D['y'] = D['y'].astype(int)
  D['m'] = D['m'].astype(int)
  D['d'] = D['d'].astype(int)
  N = read_csv('../meta/frames/names.csv', dtype=str)
  O = read_csv('../meta/frames/assignment.csv', dtype=str)

class L:
  D = merge(R.D, M.D, on=['d', 'm', 'y'], how="left").dropna()
  P = merge(R.P, M.P, on='P', how="left").dropna()
  X = merge(D, P, on=['docs', 'P', 'raport'], how="inner").drop_duplicates()
  Y = merge(X.rename(columns={'docs': 'index'}),
            R.X.drop(columns=['raport']), on='index',
            how="left", suffixes=('', '^')).drop(columns=['index'])

L.Y.to_csv('linkage.date.csv', index=False)