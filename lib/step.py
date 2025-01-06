import os, pickle, json, inspect
from .log import log

def trail(cls):

  def decorator(func):
    def wrapper(*args, **kwargs):

      S = inspect.signature(func)
      P = list(S.parameters.keys())

      K = {k: v for k, v in kwargs.items() if k in P}
      A = args[:len(P)]

      K0 = {k: v for k, v in kwargs.items() if k not in K}
      A0 = args[len(P):]

      class _Datum(cls):

        def __init__(self):
          super().__init__(*A0, **K0)

        def run(self):
          return func(*[lazy(x) for x in A], **{k: lazy(v) for k, v in K.items()})

      _Datum.__name__ = func.__name__
      return _Datum()

    return wrapper
  return decorator

def lazy(x):
  if isinstance(x, list):
    if x and isinstance(x[0], Step):
      return [y.footprint() for y in x]
  if isinstance(x, dict):
    if x and isinstance(x[next(iter(x))], Step):
      return {k: v.footprint() for k, v in x.items()}
  return x.footprint() if isinstance(x, Step) else x

class Trace:

  def __getattribute__(self, name):
    return lazy(super().__getattribute__(name))

  def run(self):
    raise NotImplementedError()

  def endpoint(self):
    log(f"🔚 {self.__class__.__name__}")
    Y = self.run()
    return Y

class Step(Trace):

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

  def endpoint(self):
    log(f"🔚 {self.__class__.__name__}")
    Y = self.footprint(True)
    return Y

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

    log(f"💾 {self.__class__.__name__} {self.outpath}.{self._ext}")
    p = f"{self.outpath}.{self._ext}"
    if self._ext == 'pkl':
      with open(p, 'wb') as f: pickle.dump(Y, f)
    elif self._ext == 'json':
      with open(p, 'w') as f: json.dump(Y, f)
    elif self._ext == 'csv':
      Y.to_csv(p)
    else:
      raise ValueError()

    log(f"✅ {self.outpath}.{self._ext}")

  def _load(self):

    log(f"📂 {self.__class__.__name__} {self.outpath}.{self._ext}")
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
      log(f"✅ {self.outpath}.{self._ext}")

    except FileNotFoundError: 
      Y = None

    return Y

  def restart(self):

    log(f"🗑 {self.__class__.__name__} {self.outpath}.{self._ext}")
    p = f"{self.outpath}.{self._ext}"
    if os.path.exists(p):
      os.remove(p)

    log(f"✅ {self.__class__.__name__}")