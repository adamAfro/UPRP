class Flow():

  def __init__(self, name=str(None), callback=lambda x: x,
               args=[], kwargs={}, mapping=None):

    self.name = name
    self.callback = callback
    if self.name == str(None):
      self.name = self.callback.__name__

    self.args = args
    self.kwargs = kwargs
    self.mapping = mapping
    self.output = None
    self.verbose = True
    self.triggered:list[Flow] = []

  def trigger(self, func=lambda X: X):

    if isinstance(func, Flow):
      self.triggered.append(func)
      return func

    f = Flow(callback=func, args=[self])
    self.triggered.append(f)
    return f
  
  def __getitem__(self, key):
    return Flow(name=f'{self.name}[{key}]', callback=lambda x: x[key], args=[self])

  def notify(self, *message, sep=" "):

    import datetime, requests

    message = self.name+sep+str(datetime.datetime.now())+sep+sep.join(message)
    D = dict(data=message.encode(encoding='utf-8'))
    requests.post( "https://ntfy.sh/uprp_dev", **D )
    self.info("notify", message)

  def info(self, *x):
    if self.verbose: print(*x, f'({self.name})', flush=True)

  def __call__(self, forced=False): return self.call(forced)

  def call(self, forced=False):

    if not forced:
      if self.output is not None: return self.output
      self.info(f'no cache found')
      if (self.mapping is not None):
        try: self.load()
        except FileNotFoundError: self.output = None
      if self.output is not None: return self.output

    self.info(f'computing args')
    args = [Flow.lazyload(x) for x in self.args]
    kwargs = {k: Flow.lazyload(v) for k, v in self.kwargs.items()}

    self.info(f'call')
    try:
      self.notify('start')
      self.output = self.callback(*args, **kwargs)
    except Exception as e:
      self.notify(f'error: {e}')
      raise e

    self.dump()
    for f in self.triggered:
      f.call(forced=True)

    return self.output

  def load(self):

    if isinstance(self.mapping, dict):
      self.output = {}
      for k, f0 in self.mapping.items():
        if f0 is None: raise FileNotFoundError()
        self.output[k] = self.fload(f0)

    if isinstance(self.mapping, tuple):
      self.output = []
      for f0 in self.mapping:
        if f0 is None: raise FileNotFoundError()
        self.output.append(self.fload(f0))

    if isinstance(self.mapping, str):
      self.output = self.fload(self.mapping)

  def fload(self, f0):

    import pickle as pkl

    if f0.endswith('.pkl'):
      with open(f0, 'rb') as f:
        Y = pkl.load(f)
        self.info(f'loaded {f0}')
        return Y

    if f0.endswith('.png'):
      return None

    raise NotImplementedError()

  def dump(self):

    if isinstance(self.mapping, dict):
      for k, f0 in self.mapping.items():
        if f0 is None: continue
        self.fdump(f0, self.output[k])

    if isinstance(self.mapping, tuple):
      for i, f0 in enumerate(self.mapping):
        if f0 is None: continue
        self.fdump(f0, self.output[i])

    if isinstance(self.mapping, str):
      return self.fdump(self.mapping, self.output)

  def fdump(self, f0, x):

    import pickle as pkl, os, altair as alt

    os.makedirs(os.path.dirname(f0), exist_ok=True)

    if f0.endswith('.pkl'):
      with open(f0, 'wb') as f:
        pkl.dump(x, f)
        self.info(f'saved {f0}')
        return

    if f0.endswith('.png'):

      if any(isinstance(x, y) for y in [alt.Chart, 
                                        alt.LayerChart, 
                                        alt.ConcatChart,
                                        alt.VConcatChart,
                                        alt.FacetChart,
                                        alt.HConcatChart]):
        x.save(f0)
        self.info(f'saved {f0}')
        return

    raise NotImplementedError()

  def lazyload(x):

    if isinstance(x, list): return Flow.lazyloadlist(x)
    if isinstance(x, dict): return Flow.lazyloaddict(x)
    else: return x() if isinstance(x, Flow) else x
  
  def lazyloaddict(x):
    return {k: Flow.lazyload(v) if isinstance(v, Flow) else v for k, v in x.items()}

  def lazyloadlist(x):
    return [Flow.lazyload(y) if isinstance(y, Flow) else y for y in x]

  def map(self, mapping):
    self.mapping = mapping
    return self

  def copy(self, args=None, kwargs=None, mapping=None):

    if args is None: args = self.args
    if kwargs is None: kwargs = self.kwargs
    if mapping is None: mapping = self.mapping

    return Flow(self.name, self.callback, args, kwargs, mapping)

  def From(name=str(None), cls=None):

    import inspect

    if cls is None: cls = Flow

    def decorator(func):
      def wrapper(*args, **kwargs):

        S = inspect.signature(func)
        P = list(S.parameters.keys())
        K = {k: v for k, v in kwargs.items() if k in P}
        A = args[:len(P)]

        return cls(name, callback=func, args=A, kwargs=K)

      return wrapper
    return decorator

  def Forward(args=[], callback=lambda x: x):
    return Flow(callback=callback, args=args)

def forward(arg, func):
  return Flow(callback=func, args=[arg])

def make(name=str(None), cls=None):

  import inspect

  if cls is None: cls = Flow

  def decorator(func):
    def wrapper(*args, **kwargs):

      S = inspect.signature(func)
      P = list(S.parameters.keys())
      K = {k: v for k, v in kwargs.items() if k in P}
      A = args[:len(P)]

      return cls(name, callback=func, args=A, kwargs=K)

    return wrapper
  return decorator