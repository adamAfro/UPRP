import pickle, yaml, re
from pandas import DataFrame, Index, concat, to_datetime
from lib.expr import Marker
from lib.datestr import num as datenum, month
from lib.datestr import MREGEX

class Loader:

  "klasa do wczytywania danych z repozytorium"

  def __init__(self, name:str,
               data:dict[str, DataFrame],
               assignment:dict[str, dict[str, dict[str, str]]]):

    self.name = name
    self.data = data
    self.assignment = assignment

  def Within(path:str, name:str|None=None):

    "wczytuje dane z katalogu (pliki `data.pkl` i `assignement.yaml`)"

    f0 = path
    k = name if name is not None else f0.split('/')[-1]

    A: dict[str, dict[str, str]] = dict()
    with open(f'{f0}/assignement.yaml', 'r') as f:
      A = yaml.load(f, Loader=yaml.FullLoader)

    H: dict[str, DataFrame] = dict()
    with open(f'{f0}/data.pkl', 'rb') as f:
      H = pickle.load(f)

    for h, X in H.items():
      X.set_index('doc', inplace=True)

    return Loader(k, H, A)

  def melt(self, assignement:str):

    """
    dane w formacie długim dla przyporządkowanych kolumn, kolumny:'repo', 'frame', 'col', 'doc', 'value'`
    """

    A = assignement

    return concat((self.data[h][k].to_frame().pipe(Loader._melt, self.name, h)
      for h, k in self._assigned(A)))

  def _melt(frame:DataFrame, repo:str, name:str):

    "funk. wewn. do tworzenia ramki danych w formacie długim"

    X = frame

    Y = X.reset_index(drop=False)\
        .melt(id_vars='doc', var_name='col')\
        .assign(repo=repo, frame=name)\
        .dropna(subset=['value'])

    return Y[['repo', 'frame', 'col', 'doc', 'value']]

  def _assigned(self, target:str):

    "zwraca przyporządkowane kolumny dla danego celu"

    return ((h, k)
      for h, X in self.assignment.items()
      for k, v in X.items()
      if v == target)

  def __getitem__(self, name:str):

    "zwraca ramkę danych dla przyporządkowanego celu"

    k = name

    X = self.melt(name)

    if k == 'date': X = X\
    .eval('value = @to_datetime(value, errors="coerce")')\
    .assign(year=lambda x: x['value'].dt.year)\
    .assign(month=lambda x: x['value'].dt.month)\
    .assign(day=lambda x: x['value'].dt.day)\
    .drop(columns=['value'])
    elif k == 'number': X = X\
    .assign(value=lambda x: x['value'].str.replace(r"\D", "", regex=True))\
    .drop_duplicates(subset=['value', 'doc'])

    return X

class Searcher:

  "klasa do wyszukiwania danych w repozytorium"

  URLalike=r'(?:http[s]?://(?:\w|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+)'
  codealike, marktarget = ['alnum', 'num', 'series'], {
    "month":  MREGEX,
    "alnum":  r"(?<!\w)(?:[^\W\d]+\d|\d+[^\W\d])\w*(?!\w)",
    "series": "(?:" + '|'.join([rf"(?:\d+\s*{s}+\s*)+" for s in ['\.', '\-', '/', '\\', '\s']]) + ")\s*\d+",
    "num":    r"(?<!\w)\d+(?!\w)", "space":  r"[\s\-\/\\\.]+",
    "braced": '|'.join([rf"\{a}\w{{1,4}}\{b}" for a,b in ['()','{}','[]', '""', '<>']]),
    "abbr":   r"(?<!\w)[^\W\d]{1,4}\.?(?!\w)",
  }

  patentalike = r'(?P<country>(?<![^\W])[a-zA-Z]{2}(?![^\W\d]))' + \
                r'(?P<prefix>(?:[^\W\d]|[\.\s]){,5})?' + \
                r'(?P<number>(?:\d\W?\s?){5,})(?!\d)' + \
                r'(?P<suffix>[\W\s]{,3}[^\w\s]*[0123abuABUXY][^\w\s\)\}\]]*[0123a-zA-Z]?[^\w\s]*)?'

  codemarker = Marker(marktarget, codealike)

  def __init__(self):

    self.number = DataFrame(index=Index([], name='value'))
    self.dates = DataFrame(index=Index([], name=('year', 'month', 'day')))

  @staticmethod
  def basic_score(results:DataFrame):
    return results.value_counts(['doc', 'repo'])

  def add(self, loader:Loader):

    L = loader

    if not self.dates.empty:
      self.dates = concat([self.dates.reset_index(), L['date']]).set_index(['year', 'month', 'day'])
    else:
      self.dates = L['date'].set_index(['year', 'month', 'day'])

    if not self.number.empty:
      self.number = concat([self.number.reset_index(), L['number']]).set_index('value')
    else:
      self.number = L['number'].set_index('value')

  def search(self, query:str):

    q = query

    X = [(x) for x, _, _, m in self.codemarker.union(q) if m == True]

    C = [m.groupdict() for v in X for m in re.finditer(Searcher.patentalike, v)]

    P0 = [(c['number']) for c in C]
    P = self.number.loc[self.number.index.intersection(P0)]

    D0 = [(y, m, d) for x in X for _, _, x, d, m, y in datenum(x)] + \
         [(y, m, d) for x in X for _, _, x, d, m, y in month(x)]
    D = self.dates.loc[self.dates.index.intersection(D0)]

    Y0 = [U for U in [P, D] if not U.empty]
    if not Y0: return None

    Y = concat(Y0)
    s = Searcher.basic_score(Y)
    s = s[s > 0]
    if s.empty: return None
    i, r = s.sort_values(ascending=False).head(1).index[0]
    T = Y.query('doc == @i and repo == @r')\
    .pivot_table(index=['doc'], columns=['repo', 'frame', 'col'], aggfunc='size', fill_value=0)

    return T if not T.empty else None