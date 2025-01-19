from pandas import DataFrame, concat

class Storage:

  def __init__(self, name:str, data:dict[str, DataFrame],
               assignement:dict[str, dict[str, dict[str, str]]]|None=None):

    self.name = name
    self.data = data
    self.assignement = assignement

    self.docs = concat([X.index.get_level_values('doc').to_series()
      for X in self.data.values()]).drop_duplicates()\
      .reset_index(drop=True)

  def getdocs(self, docs:list):

    Y = dict()
    for h, X in self.data.items():
      try: Y[h] = X.loc[ X.index.get_level_values('doc').isin(docs) ]
      except KeyError: continue
    return Y

  def melt(self, name:str):

    'Uwaga: tworzy poziom "repo", które nie jest używany'

    a = name
    H0 = [self.data[h][k].to_frame().pipe(Storage._melt, self.name, h) for h, k in self._assigned(a)]
    if not H0: return DataFrame(columns=['repo', 'frame', 'col', 'assignement', 'doc', 'id', 'value'])

    H = concat(H0)
    H['assignement'] = a

    if any(k in a for k in ['name', 'city']):
      H['value'] = H['value'].str.upper()

    return H

  @staticmethod
  def _melt(frame:DataFrame, repo:str, name:str):

    'Uwaga: tworzy poziom "repo", które nie jest używany'

    X = frame
    K = ['repo', 'frame', 'col', 'doc', 'id', 'value']
    X.columns = [k if k not in K else k+'*' for k in X.columns]

    Y = X.reset_index(drop=False)\
    .melt(id_vars=['doc', 'id'], var_name='col')\
    .assign(repo=repo, frame=name)\
    .dropna(subset=['value'])

    return Y[K]

  def _assigned(self, target:str):

    return ((h, k)
      for h, X in self.assignement.items()
        for k, v in X.items()
          if (isinstance(v, str) and v == target) or
             (isinstance(v, list) and target in v))

  def str(self):
    def underline(x:str): return '\n\n\n'+x+'\n'+''.join(['=' for i in x])+'\n\n'

    Y = ''

    for k, X in self.data.items():
      if X.empty: continue
      Y += underline(k) + str(X)

    return Y + '\n\n\n'

  def strdocs(self, docs=[]):
    def underline(x:str): return '\n\n\n'+x+'\n'+''.join(['-' for i in x])+'\n\n'

    Y = ''
    D = docs

    for d in D:

      for k, X in self.data.items():
        Y += underline(f"{k}/{d}")
        U = X.reset_index().query(f'doc == "{d}"')
        if U.empty: continue
        Y += str(U)

    return Y + '\n\n\n'