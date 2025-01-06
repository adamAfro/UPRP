import os, pickle, json, inspect
from .log import log

def trail(Tracealike):

  def decorator(func):
    def wrapper(*args, **kwargs):

      S = inspect.signature(func)
      P = list(S.parameters.keys())

      K = {k: v for k, v in kwargs.items() if k in P}
      A = args[:len(P)]

      K0 = {k: v for k, v in kwargs.items() if k not in K}
      A0 = args[len(P):]

      class Leg(Tracealike):
        name = ''
        def __repr__(self):
          if self.name:
            return f"{self.name}: {Tracealike.__name__}({func.__name__})"
          else:
            return f"{Tracealike.__name__}({func.__name__})"

      Leg.__name__ = func.__name__
      Leg.__doc__ = func.__doc__

      return Leg(*A0, callback=func, input=list(A), kwinput=K, **K0)

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

def walk(x, f):

  if isinstance(x, list):
    if x and isinstance(x[0], Trace):
      for v in x: f(v)

  if isinstance(x, dict):
    if x and isinstance(x[next(iter(x))], Trace):
      for k, v in x.items(): f(v)

  if isinstance(x, Trace): f(x)

class Trace:

  def __init__(self, callback, input, kwinput):

    self.callback = callback
    self.input = input
    self.kwinput = kwinput

  @property
  def steps(self):
    return self.input + [v for v in self.kwinput.values()]

  def run(self):
    return self.callback( *[lazy(x) for x in self.input], 
                         **{k: lazy(v) for k, v in self.kwinput.items()})

  def endpoint(self):
    log(f"🔚")
    Y = self.run()
    return Y

class Step(Trace):

  def __init__(self, outpath:str, skipable=True, *args, **kwargs):

    super().__init__(*args, **kwargs)

    self.skipable: bool = skipable
    self.outpath = outpath
    self._ext = outpath.split('.')[-1].lower()
    if self._ext in ['pkl', 'json', 'csv']: 
       self.outpath = outpath[:-len(self._ext)-1]
    else: self._ext = 'pkl'

    self._loaded = None

  def endpoint(self):
    log(f"🔚 {self.outpath}.{self._ext}")
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

    log(f"💾 {self.outpath}.{self._ext}")
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

    log(f"📂 {self.outpath}.{self._ext}")
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

    log(f"🗑 {self.outpath}.{self._ext}")
    p = f"{self.outpath}.{self._ext}"
    if os.path.exists(p):
      os.remove(p)

    log(f"✅")