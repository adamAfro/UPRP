import os, glob, pickle, json

class Ghost:

  def __getattribute__(self, name):
    Y = super().__getattribute__(name)
    if isinstance(Y, list):
      if Y and isinstance(Y[0], Step):
        return [y.footprint() for y in Y]

    return Y.footprint() if isinstance(Y, Step) else Y

  def run(self, *args, **kwargs):
    raise NotImplementedError()

  def endpoint(self, *args, **kwargs):
    return self.run(*args, **kwargs)

class Step(Ghost):

  EXT = ['pkl', 'json', 'csv']

  def __init__(self, outpath:str, skipable=True):

    super().__init__()

    self.skipable: bool = skipable
    self.outpath = outpath
    self._ext = outpath.split('.')[-1].lower()
    if self._ext in self.EXT: 
       self.outpath = outpath[:-len(self._ext)-1]
    else: self._ext = 'pkl'

    self._loaded = None

  def endpoint(self, *args, **kwargs):
    return self.footprint(True, *args, **kwargs)

  def footprint(self, force=False, *args, **kwargs):

    if (not force) and self.skipable:
      Y = self._load()
      if Y is not None:
        return Y

    Y = self.run(*args, **kwargs)
    self._dump(Y)
    self._loaded = Y
    return Y

  def _dump(self, Y):
    p = f"{self.outpath}.{self._ext}"
    if self._ext == 'pkl':
      with open(p, 'wb') as f: pickle.dump(Y, f)
    elif self._ext == 'json':
      with open(p, 'w') as f: json.dump(Y, f)
    elif self._ext == 'csv':
      Y.to_csv(p)

  def _load(self):
    if self._loaded is not None:
      return self._loaded
    try:
      p = f"{self.outpath}.{self._ext}"
      if self._ext == 'pkl':
        with open(p, 'rb') as f: Y = pickle.load(f)
      elif self._ext == 'json':
        with open(p, 'r') as f: Y = json.load(f)
      elif self._ext == 'csv':
        import pandas
        Y = pandas.read_csv(p)
        if self.outpath.split('.')[-1] == 's':
          Y = Y.set_index(Y.columns[0])[Y.columns[1]]

      self._loaded = Y

    except FileNotFoundError: 
      Y = None

    return Y