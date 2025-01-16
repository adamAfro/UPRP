def notify(*message, sep=" "):#TODO: impl in Flow

  import datetime, requests

  message = str(datetime.datetime.now())+sep+sep.join(message)
  D = dict(data=message.encode(encoding='utf-8'))
  requests.post( "https://ntfy.sh/uprp_dev", **D )
  print("notify", message, flush=True)

class Flow():

  def __init__(self, name=str(None), callback=lambda x: x,
               args=[], kwargs={}, mapping=None):

    self.name = name
    self.callback = callback
    self.args = args
    self.kwargs = kwargs
    self.mapping = mapping
    self.output = None
    self.verbose = True

  def info(self, *x):
    if self.verbose: print(*x, f'({self.name})')

  def __call__(self, forced=False): return self.call(forced)

  def call(self, forced=False):

    if not forced:
      if self.output is not None: return self.output
      self.info(f'no cache found')
      if self.mapping is not None:
        try: self.load()
        except FileNotFoundError: self.output = None
      if self.output is not None: return self.output

    self.info(f'call {self.callback.__name__}')
    args = [Flow.lazyload(x) for x in self.args]
    kwargs = {k: Flow.lazyload(v) for k, v in self.kwargs.items()}
    self.output = self.callback(*args, **kwargs)
    self.dump()
    return self.output

  def load(self):

    if isinstance(self.mapping, dict):
      self.output = {}
      for k, f0 in self.mapping.items():
        self.output[k] = self.fload(f0)

    if isinstance(self.mapping, tuple):
      self.output = []
      for f0 in self.mapping:
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

    raise NotImplementedError()

  def dump(self):

    if isinstance(self.mapping, dict):
      for k, f0 in self.mapping.items():
        self.fdump(f0, self.output[k])

    if isinstance(self.mapping, tuple):
      for i, f0 in enumerate(self.mapping):
        self.fdump(f0, self.output[i])

    if isinstance(self.mapping, str):
      return self.fdump(self.mapping, self.output)

  def fdump(self, f0, x):

    import pickle as pkl

    if f0.endswith('.pkl'):
      with open(f0, 'wb') as f:
        pkl.dump(x, f)
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

  def From(name=str(None)):

    import inspect

    def decorator(func):
      def wrapper(*args, **kwargs):

        S = inspect.signature(func)
        P = list(S.parameters.keys())
        K = {k: v for k, v in kwargs.items() if k in P}
        A = args[:len(P)]

        return Flow(name, callback=func, args=A, kwargs=K)

      return wrapper
    return decorator