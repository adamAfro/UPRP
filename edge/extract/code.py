from pandas import read_csv, DataFrame, Series, concat
from tqdm import tqdm as progress; progress.pandas()
import os, json, re

# Wyciąganie kodów
# ----------------
#
# Wyszukiwanie kodów z tekstu polega na użyciu wyrażeń regularnych.
# Z racji, że są to wyrażenia alfanumeryczne i kombinacji jest wiele
# to trzeba zdefiniować kilka klas wyrażeń regularnych, które
# później są łączone i odpowiednio oznaczane jako poprawne lub nie.
# Niektóre wyr. są pożądane same w sobie - np. cyfry, inne tylko jeśli
# są obok innych, np. skrót krajowy przy cyfrze: PL 000000.
# Po wyszukaniu wyrażeń te są łączone na podstawie sąsiedztwa, 
# a później oznaczane jako oczekiwane na podstawie tego, czy
# zawierają jeden z wcześniej określonych wzorców (patrz: klasa `Expr`)
class Expr:
  "Przechowuje wyr. re. `Q` z nazwanymi grupami i nazwy `K0` oczekiwanych grup."
  def __init__(self, Q:dict, K0:list=[]): self.Q, self.K0 = Q, K0
  def union(self, x:str):
    "->(złączone znajdy `Q`, początek, koniec, czy nazwa znajdy jest w `K0`)"
    import re
    I = re.finditer(self.Q, x)
    S = [(m.group(), m.start(), m.end(), m.groupdict()) for m in I]
    if not S: return []
    S = [(x, a, b, any(m[k] is not None for k in self.K0)) for x, a, b, m in S]
    S.sort(key=lambda x: x[1])
    M = [(S[0])]
    for x, a, b, m in S[1:]:
      x0, a0, b0, m0 = M[-1]
      if a > b0: M.append((x, a, b, m))
      else:
        M[-1] = (x0 + x, a0, max(b0, b), m0 or m)
    return M

# Wyszukiwanie dat
# ----------------
# 
# Znalezione kody mogą być datami, ale nawet jeśli je przypominają
# wcale nie oznacza, że są one poprawne: liczba określająca
# dzień (o ile określony) musi być koniecznie w przedziale 1-31, 
# to można sprawdzic wyrażeniami regularnymi, ale to czy rzeczywiście
# 31 jest pradziwym dniem wcale nie jest pewne, ze wzlgędu na różnice
# w miesiącach czy latach przestępnych (28/29-luty).
# Zadanie wykonuje funkcja `date`, która umożliwia dodatkowo
# testowanie niepełnych dat, np. 12.08 może dotyczyć grudnia 2008r.
def date(d:str|None, m:str|None, y:str):
  "Używa modułu `datetime` do weryfikacji poprawności daty."
  from datetime import date as f
  import re
  
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
    D = f(y, m, d)
    if D.year > 2024: return None
    return (None if d0 else D.day, None if m0 else D.month, D.year)
  except ValueError: return None

with open(os.path.join('country.json'), 'r') as fN: Co = json.load(fN)
with open(os.path.join('month.json'), 'r') as fN: Mo = json.load(fN)
mo = '|'.join([ m for M in Mo['PL'] + Mo['EN'] for m in M ])

# Daty numeryczne
# ---------------
#
# Daty numeryczne mogą być zapisane w różnych formatach, gdzie
# liczba z rokiem jest po lewe albo prawej stronie. Może być też
# sam rok, może zawierać dzień, może tylko miesiąc. Dodatkowo 
# ułożenie dzień/miesiąc/rok wcale nie jest pewne, np. data
# 2010.10.09 może dotyczyć 9 października albo 10 września.
# Funkcja `datenum` znajduje wszystkie możliwe daty numeryczne
# i używa funkcji `date` do weryfikacji poprawności.
def datenum(x:str, Q = [q for s, qd, qY4, qNd in [(
  r"[\W\s]{1,3}", r"\b(?:3[01]|[012]?\d)\b", r"\b(?:20[012]\d|19\d\d)\b", r"\b(?:3[2-9]|[4-9]\d)\b")] 
  for q in [rf"(?<!\d)(?P<A>{qd})?{s}(?P<B>{qd}){s}(?P<Y>{qY4}|{qNd}|{qd})(?!\d)",
            rf"(?<!\d)(?P<Y>{qY4}|{qNd}){s}(?P<A>{qd}){s}(?P<B>{qd})?(?!\d)",
            rf"(?<!\d)(?P<Y>{qY4})(?!\d)",]]):
  import re
  M = [(y.span(), y.group(), y.groupdict()) for q in Q for y in re.finditer(q, x, flags=re.IGNORECASE)]
  V = [(r,x,date(d.get('A', None), d.get('B', None), d.get('Y'))) for r,x,d in M] +\
      [(r,x,date(d.get('B', None), d.get('A', None), d.get('Y'))) for r,x,d in M]
  return [(a,b,x,*D) for (a,b),x,D in V if D]

# Daty zapisane słownie
# ---------------------
#
# Daty zapisane słownie są najczęściej zapisane po polsku albo
# angielsku. Położenie lat wcale nie jest pewne, np. 11 12 wrzesień
# może być 2012 albo 2011 rokiem i odpowiednio 12 albo 11 dniem.
# Funkcja `month` znajduje wszystkie możliwe daty słowne i używa
# funkcji `date` do weryfikacji poprawności.
def month(x:str, Q = [(i,q) for s0, s, qYd, M in [(r"[\W\s]{0,3}", r"\b[\W\s]{1,3}\b", r"\b(?:20[012]\d|19\d\d|\d\d)\b", 
  [(i, '|'.join(a+b)) for i, a, b in zip(range(1, 12+1), Mo['PL'], Mo['EN'])])] for i, q in
  [(i, rf"((?P<A>{qYd}){s})?(?P<B>{qYd}){s0}(?P<M>{q})") for i, q in M]+
  [(i, rf"(?P<M>{q}){s0}(?P<A>{qYd})({s}(?P<B>{qYd}))?") for i, q in M]+
  [(i, rf"(?P<A>{qYd}){s0}(?P<M>{q}){s0}(?P<B>{qYd})") for i, q in M]]):
  import re
  Mm = [(y.span(), y.group(), y.groupdict(), m) for m, q in Q for y in re.finditer(q, x, flags=re.IGNORECASE)] 
  V = [(r,x,date(d.get('A', None), m, d.get('B'))) for r,x,d, m in Mm] +\
      [(r,x,date(d.get('B', None), m, d.get('A'))) for r,x,d, m in Mm]
  return [(a,b,x,*D) for (a,b),x,D in V if D]

X = read_csv('../docs.csv').reset_index()
X = X.rename(columns={'docs':'text', 'index':'docs'})[['docs', 'text']]

X['text'].str.len().plot.hist(title='Hist. dłg. tekstu', bins=100, 
                              xlabel='dłg. tekstu', ylabel='n-dokumentów');

# Usuwanie linków
# ---------------
# Linki są zwyczajnie usuwane i zastępowane spacjami, aby
# zachować spójność pozostałych informacji.
qURL=r'(?:http[s]?://(?:\w|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+)'
nURL = X['text'].str.contains(qURL).sum()
X['text'] = X['text'].progress_apply(lambda x:
  re.sub(qURL, lambda m: ' '*len(m.group()), x))

Series([X.shape[0]-nURL, nURL], index=['brak URL', 'URL'])\
  .plot.pie(title='Wystąpienia adresów URL');

# Wyciąganie kodów z danych
# -------------------------
#
# Poniżej znajduje się zastosowanie w.w. metody do wyszukiwania
# kodów alfanumerycznych.
eN = Expr(Q=re.compile('(?:' + '|'.join(rf'(?P<{k}>{q})' for k, q in {
  "month":  rf"(?:(?<=\d\s|.\d)(?:{mo})(?!\w)|(?<!\w)(?:{mo})(?=\s*\d))",
  "alnum":  r"(?<!\w)(?:[^\W\d]+\d|\d+[^\W\d])\w*(?!\w)",
  "series": "(?:" + '|'.join([rf"(?:\d+\s*{s}+\s*)+" for s in ['\.', '\-', '/', '\\', '\s']]) + ")\s*\d+",
  "num":    r"(?<!\w)\d+(?!\w)",
  "braced": '|'.join([rf"\{a}\w{{1,4}}\{b}" for a,b in ['()','{}','[]', '""', '<>']]),
  "abbr":   r"(?<!\w)[^\W\d]{1,4}\.?(?!\w)",
  "space":  r"[\s\-\/\\\.]+",
}.items()) + ')'), K0=['alnum', 'num', 'series'])
fN = lambda x: [(x['docs'], *c) for c in eN.union(x['text'])]
U = DataFrame(X.progress_apply(fN, axis=1).explode().tolist(),
               columns=['docs', 'text', 'start', 'end', 'numerical'])\
               .dropna().query('numerical==True')

Series([X.shape[0], U.shape[0]], ['cytowania', 'kody'])\
  .plot.bar(title='Wyszukiwanie kodów w cytowaniach');

# Wyciąganie dat z danych
# -----------------------
fD = lambda x: [(x['docs'], x['start'], x['end'], *c) for c in datenum(x['text'])]
D = DataFrame([y for u in U.progress_apply(fD, axis=1) if u for y in u],
              columns=['docs', 'start', 'end', 'numstart', 'numend', 'text', 'day', 'month', 'year'])\
              .convert_dtypes().drop_duplicates(subset=['docs', 'day', 'month', 'year'])

D['start'], D['end'] = D['start'] + D['numstart'], D['start'] + D['numend']
D = D.sort_values(['day', 'month', 'year'], ascending=False)

fMo = lambda x: [(x['docs'], x['start'], x['end'], *c) for c in month(x['text'])]
M = DataFrame([y for u in U.progress_apply(fMo, axis=1) if u for y in u],
              columns=['docs', 'start', 'end', 'numstart', 'numend', 'text', 'day', 'month', 'year'])\
              .convert_dtypes().drop_duplicates(subset=['docs', 'day', 'month', 'year'])

M['start'], M['end'] = M['start'] + M['numstart'], M['start'] + M['numend']
M = M.sort_values(['day', 'month', 'year'], ascending=False)

Series([X.shape[0], U.shape[0], D.shape[0], M.shape[0]],
       ['cytowania', 'kody', 'daty numeryczne', 'daty słowne'])\
  .plot.bar(title='Wyszukiwanie dat w cytowaniach')

# Kody patentowe
# --------------
#
# W znalezionych kodach, część jest kodami patentowymi. Te
# zaczynają się skrótem krajowym, a później jest numer.
# Na końcu może być dodatkowy skrót dot. samego dokumentu.
P = U.progress_apply(lambda u:
  [{ "start": u['start'], "end": u['end'], "docs": u['docs'], "text": u['text'],
    **m.groupdict(), "codestart": m.start(), "codeend": m.end()} for m in re.finditer(
    r'(?P<country>' + '|'.join(['(?<![^\W\d])' + r'[\W\s]*'.join(k) for k in Co.keys()]) + '(?![^\W\d]))' + \
    r'(?P<prefix>(?:[^\W\d]|[\.\s]){,5})?' + \
    r'(?P<number>(?:\d\W?\s?){5,})(?!\d)' + \
    r'(?P<suffix>[\W\s]{,3}[^\w\s]*[0123abuABUXY][^\w\s\)\}\]]*[0123a-zA-Z]?[^\w\s]*)?', u['text'])], axis=1)
P = DataFrame(P.explode().dropna().tolist()).convert_dtypes()
P['start'], P['end'] = P['start'] + P['codestart'], P['start'] + P['codeend']

Series([X.shape[0], U.shape[0], P.shape[0]],
       ['cytowania', 'kody', 'kody patentowe'])\
  .plot.bar(title='Wyszukiwanie patentów w cytowaniach')

# Usuwanie dat z kodów z patentami
# --------------------------------
#
# Przez mechanizm skryptu niektóre daty są powiązane z kodami,
# bo między nimi nie było żadnego jednoznacznego separatora, np.
# przecinka, ew. był taki, który spełnia inne funkcji, np. kropka.
P['country'] = P['country'].str.replace('\W', '', regex=True).str.upper()
P['number'] = P['number'].str.replace('\D', '', regex=True)
P['numlen'] = P['number'].str.len()
P['numlenmed'] = P.groupby('country')['numlen'].transform('median')

c0 = P['country'].value_counts().to_frame().query('count > 1000').index
P.query('country in @c0').groupby('numlen')['country']\
 .value_counts().unstack().plot.bar(title='Ilość cyfr w kodzie przy najliczniejszych państwach',
                                     ylabel='n-kodów', xlabel='ilość cyfr').legend()

P.query('(country in @c0) and (numlen >= numlenmed + 6)').groupby('numlen')['country']\
 .value_counts().unstack().plot.bar(title='Ilość cyfr w kodzie przy najliczniejszych państwach',
                                    ylabel='n-kodów', xlabel='ilość cyfr dla n-cyfr ≥ mediana grupy + 6').legend()

# W takich przypadkach, daty są usuwane z końców kodów.
# To tam są z reguły. Dla uproszczenia brane są pod uwagę wyłącznie
# daty zawierające pełną informację o dniu: dzień, miesiąc, rok.
# Do weryfikacji używane są wszystkie daty po odfiltrowaniu tych,
# które nie zaczynają się w kodach patentowych.
Dp = D.dropna(subset=['day', 'month'])\
      .drop_duplicates(subset=['docs', 'start'])\
      .merge(P, 'inner', on='docs', suffixes=('D', ''))\
      .query('(start <= startD < end)')\
      [['startD', 'endD', 'docs', 'start', 'textD']]

# Usuwanie dat to zwyczajne zastąpienie ich pustym ciągiem.
P = P.merge(Dp, on=['docs', 'start'], how='left')
P['textD'] = P['textD'].fillna("").str.replace("\D", "", regex=True).fillna("")
P['number'] = P.progress_apply(lambda x: x['number'].replace(x['textD'], ""), axis=1)
P['numlen'] = P['number'].str.len()
P['numlenmed'] = P.groupby('country')['numlen'].transform('median')

Ppignore = True
if not Ppignore:
  Pp = U['text'].str.extract(r'(?P<C>(?<!\w)p\.?)' + \
    r'[\W\s]*(?P<X>(?:\d\W?\s?){5,})(?!\d)' + \
    r'[\W\s]{,3}(?P<S>[^\w\s]*[0123abuABU][^\w\s]*[0123a-zA-Z])?')\
    .dropna(subset=['C', 'X']).join(U.drop('numerical', axis=1))\
    .rename(columns={'C':'country', 'X':'number', 'S':'suffix'})

P[["docs", "start", "end", "text", "country", "prefix", "number", "suffix"]]\
  .convert_dtypes().to_csv('patent.csv', index=False)
# Pp.convert_dtypes().to_csv('patent.p-.csv', index=False)
concat([D, M]).convert_dtypes().to_csv('date.csv', index=False)