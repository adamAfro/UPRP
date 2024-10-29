from pandas import read_csv, DataFrame, Series
from tqdm import tqdm as progress; progress.pandas()
import re

def split(x:str,  open = ['[', '(', '{'],
                  close = [']', ')', '}', ',', ';', ':'],
                  shift = ['"']):
  i, y, Y = 0, '', []
  for i in range(len(x)):
    if x[i] in open:
      if len(y) > 0: Y.append(y)
      y = x[i]
    elif x[i] in close:
      Y.append(y + x[i])
      y = ''
    elif x[i] in shift:
      if (len(y) > 0) and (y[0] != x[i]): 
        Y.append(y); y = x[i]
      else: Y.append(y + x[i]); y = ''
    else: y += x[i]
  if len(y) > 0: Y.append(y)
  return Y

def pattern(v:Series):
  v = v.copy()
  
  protocol = r'(\bhttps?://|www\.)'
  v = v.str.replace(protocol, "//", regex=True)
  alpha = r'([^\W\d]+)'
  v = v.str.replace(alpha, lambda m:"C" if len(m.group(1)) > 8 else
                                    "B" if len(m.group(1)) > 3 else
                                    "A" if len(m.group(1)) > 1 else "L", regex=True)
  
  v = v.str.replace('L.', "I").str.replace('A.', "I")
  v = v.str.replace(r'\d+', lambda m: str(len(m.group(0))), regex=True)
  
  webstart = r'//\S+'
  v = v.str.replace(webstart, "U", regex=True)

  v = v.str.replace(r'[\W\s]', " ", regex=True)
  v = v.str.replace(r'\s{2,}', " ", regex=True)
  
  return v

class PL:
  m = [["styczeń", "stycznia", "stycz", "sty"],
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
  q = [x for l in m for x in l]
  
class EN:
  m = [["jan", "january"], 
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
  q = [x for l in m for x in l]

def alphanumcut(r:Series, p:str, name:str=None):
  import re
  try: 
    if r['name'] != 'unmatched': return [r.to_dict()]
  except: pass
  x = r["content"].lower()
  x = re.sub(r'[^\w\.]', " ", x)
  x = re.sub(r'(\d)(\.)', lambda m: m.group(1) + " "*len(m.group(2)), x)
  F = list(re.finditer(p, x))
  I = [i for m in F for i in m.span()]
  Q = [i for i in I if any((i == m.start()) for m in F)]
  if len(I) == 0:
    y = r.to_dict()
    y["name"] = "unmatched"
    return [y]
  if I[0] != 0: I = [0] + I
  if I[-1]+1 != len(x): I = I + [len(x)]
  U = [r["content"][I[i]:I[i+1]] for i in range(len(I)-1)]
  Y = []
  for i in range(len(U)):
    y = r.copy().to_dict()
    y["content"] = U[i]
    if name is not None:
      y['name'] = name if I[i] in Q else "unmatched"
    Y.append(y)
  return Y

X = read_csv('../docs.csv')

E = X.apply(lambda x: [(ch, x.name, x["P"]) for ch in split(x["docs"])], axis=1)
E = DataFrame(E.explode().tolist(), columns=["content", "docs", "raport"])

yQ = ["(\d{2}|\d{4})\s+(" + '|'.join(PL.q) + ")\s+(\d{2}|\d{4})",
      "(\d{2}|\d{4})\s+(" + '|'.join(EN.q) + ")\s+(\d{2}|\d{4})",
      '\d{4}\s+\d{1,2}\s+\d{1,2}',
      '\d{1,2}\s+\d{1,2}\s+\d{4}',
      '\d{1,2}\s+\d{1,2}\s+\d{2}',
      '\d{2}\s+\d{1,2}\s+\d{1,2}']
yQ = re.compile('|'.join(yQ))

E = E.progress_apply(lambda r: alphanumcut(r, yQ, "date"), axis=1)
E = DataFrame(E.explode().tolist())

pQ = ['((((patent|p)[\srplu\W]{1,5})|(numer|nr)\D{1,5})\s*\d*\d{2}\s*\d{3})']
pQ = re.compile('|'.join(pQ))

E = E.progress_apply(lambda r: alphanumcut(r, pQ, "patent"), axis=1)
E = DataFrame(E.explode().tolist())

E = E.progress_apply(lambda r: alphanumcut(r, "[^\W\d]{1,3}\d{7,}", "foreign"), axis=1)
E = DataFrame(E.explode().tolist())

E = E.progress_apply(lambda r: alphanumcut(r, "[^\W\d]+", "word"), axis=1)
E = DataFrame(E.explode().tolist())

E["normal"] = E["content"].progress_apply(lambda x: 
  ''.join(filter(lambda c: c.isalpha() or c == '.', x.upper()))).fillna("")

E = E[ E["content"].str.len() > 0 ]
E = E.convert_dtypes()
E.to_csv("docs.chunks.csv", index=False)