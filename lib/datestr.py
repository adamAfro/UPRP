from datetime import date
import re

def valid(d:str|None, m:str|None, y:str):

  "Używa modułu `datetime` do weryfikacji poprawności daty."

  d0, m0, y0 = d is None, m is None, y is None
  if y0: return None
  if d0: d = 1
  elif m0: return None
  if m0: m = 1
  d = int(re.sub(r"[\D\s]", "", str(d)))
  m = int(re.sub(r"[\D\s]", "", str(m)))
  y = re.sub(r"[\D\s]", "", str(y))
  if len(y) == 2: 
    y = int(f"20{y}" if y[0] in "012" else f"19{y}")
  elif len(y) == 4: y = int(y)
  elif len(y) == 1: y = int(f"200{y}")
  else: return None
  try:
    D = date(y, m, d)
    if D.year > 2024: return None
    return (None if d0 else D.day, None if m0 else D.month, D.year)
  except ValueError: return None

def num(x:str, Q = [q for s, qd, qY4, qNd in [(
  r"[\W\s]{1,3}", r"\b(?:3[01]|[012]?\d)\b", r"\b(?:20[012]\d|19\d\d)\b", r"\b(?:3[2-9]|[4-9]\d)\b")] 
  for q in [rf"(?<!\d)(?P<A>{qd})?{s}(?P<B>{qd}){s}(?P<Y>{qY4}|{qNd}|{qd})(?!\d)",
            rf"(?<!\d)(?P<Y>{qY4}|{qNd}){s}(?P<A>{qd}){s}(?P<B>{qd})?(?!\d)",
            rf"(?<!\d)(?P<Y>{qY4})(?!\d)",]]):

  M = [(y.span(), y.group(), y.groupdict()) for q in Q for y in re.finditer(q, x, flags=re.IGNORECASE)]
  V = [(r,x,valid(d.get('A', None), d.get('B', None), d.get('Y'))) for r,x,d in M] +\
      [(r,x,valid(d.get('B', None), d.get('A', None), d.get('Y'))) for r,x,d in M]

  return [(a,b,x,*D) for (a,b),x,D in V if D]

MONTH = {
  "PL": [
    ["styczeń", "stycznia", "stycz", "sty"],
    ["luty", "lutego", "lut"],
    ["marzec", "marca", "marz", "mar"],
    ["kwiecień", "kwietnia", "kwiec", "kwi", "kw"],
    ["maj", "maja"],
    ["czerwiec", "czerwca", "czerw", "czer", "cze"],
    ["lipiec", "lipca", "lip"],
    ["sierpień", "sierpnia", "sierp", "sier", "sie"],
    ["wrzesień", "września", "wrześ", "wrz"],
    ["październik", "października", "paźdz", "paź"],
    ["listopad", "listopada", "list", "lis"],
    ["grudzień", "grudnia", "grud", "gru"]
  ],
  "EN": [
    ["january", "jan"],
    ["february", "feb"],
    ["march", "mar"],
    ["april", "apr"],
    ["may"],
    ["june", "jun"],
    ["july", "jul"],
    ["august", "aug"],
    ["september", "sep"],
    ["october", "oct"],
    ["november", "nov"],
    ["december", "dec"]
  ]
}

mo = '|'.join([ m for M in MONTH['PL'] + MONTH['EN'] for m in M ])
MREGEX = rf"(?:(?<=\d\s|.\d)(?:{mo})(?!\w)|(?<!\w)(?:{mo})(?=\s*\d))"

def month(x:str, Q = [(i,q) for s0, s, qYd, M in [(r"[\W\s]{0,3}", r"\b[\W\s]{1,3}\b", r"\b(?:20[012]\d|19\d\d|\d\d)\b", 
  [(i, '|'.join(a+b)) for i, a, b in zip(range(1, 12+1), MONTH['PL'], MONTH['EN'])])] for i, q in
  [(i, rf"((?P<A>{qYd}){s})?(?P<B>{qYd}){s0}(?P<M>{q})") for i, q in M]+
  [(i, rf"(?P<M>{q}){s0}(?P<A>{qYd})({s}(?P<B>{qYd}))?") for i, q in M]+
  [(i, rf"(?P<A>{qYd}){s0}(?P<M>{q}){s0}(?P<B>{qYd})") for i, q in M]]):

  Mm = [(y.span(), y.group(), y.groupdict(), m) for m, q in Q for y in re.finditer(q, x, flags=re.IGNORECASE)] 
  V = [(r,x,valid(d.get('A', None), m, d.get('B'))) for r,x,d, m in Mm] +\
      [(r,x,valid(d.get('B', None), m, d.get('A'))) for r,x,d, m in Mm]

  return [(a,b,x,*D) for (a,b),x,D in V if D]