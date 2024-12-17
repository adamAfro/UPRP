import networkx
from pandas import DataFrame

class Pathalias:

  """
  Na podstawie grafu tworzy informacyjne i unikalne aliasy ścieżek:

  - każdy wierzchołek to `(fragment ścieżki, [indeksy do ścieżek])`,
  - krawędź mówi o tym, że jeden wierzchołek jest poprzednikiem drugiego,
    dla przynajmniej jednej ścieżki.
  """

  def __init__(self, paths:list[str], maxlen:int=3, sep:str="/", norm:callable=None):
    self.sep = sep
    self.paths = paths
    self.norm = norm if norm is not None else lambda x: x
    self.maxlen = maxlen  #maksymalna długość aliasu
    self.short = dict()   #najkrótsze aliasy od ścieżek
    self.length = dict()   #długości i ścieżki jakie zawierają
    self.graph = None     #graf fragmentów ścieżek
    self._graph()
    self._short()

  def _graph(self):

    s = self.sep
    P0 = [(q, p.strip(s).split(s)) for q, p in enumerate(self.paths)]
    G = networkx.DiGraph()

    for q, p0 in P0:

      P = [(i, s.join(p0[:i+1])) for i in range(len(p0))]

      for i, p in P:
        if p not in G.nodes:
          G.add_node(p, chunk=p.split(s)[-1], paths=[])
        G.nodes[p]['paths'].append(q)
      for i in range(1, len(P)):
        G.add_edge(P[i-1][1], P[i][1])

    L, L0 = {}, [(q, len(p)) for q, p in P0]
    for q, l in L0:
      if l not in L.keys():
        L[l] = []
      L[l].append(q)

    self.length = { l: L[l] for l in sorted(L.keys()) }
    self.graph = G

  @property
  def unique(self):
    """
    Najkrótsze możliwe fragmenty ścieżki z liczbowym sufiksem,
    jeśli się powtarzają
    """
    A = self.short
    r = dict()    #odwrotne map. aliasów
    for k0, k in [k for k in A.items()] + [('id', 'id'), ('doc', 'doc')]:
      r[k] = r[k] + [k0] if k in r.keys() else [k0]

    u = dict()    #unikalne aliasy
    for k, K0 in r.items():
      if len(K0) == 1: continue
      for i0, i in enumerate(K0):
        u[i] = self.norm(f"{k}{self.sep}{i0+1}")

    return { **A, **u }

  def _short(self):

    """
    Najkrótsze możliwe fragmenty ścieżki niedłuższe niż `maxlen`
    Dążą do unikalności ale jej nie gwarantują, patrz `unique`.
    """

    G = self.graph
    L = self.length
    M = self.maxlen

    for l, Q in L.items():
      for q in Q:
        A0 = self.short.values()
        B = self._branch(q)
        N = [G.nodes[b] for b in B]
        p = self.norm(N[-1]["chunk"])
        N = sorted(N[:-1], key=lambda x: len(x['paths']))
        for i in range(0, min( len(N), M ) - 1):
          if (p not in A0) and (p not in ['id', 'doc']): break
          p = self.norm(f"{N[i]['chunk']}{self.sep}" + p)
        self.short[self.paths[q]] = p

  def _branch(self, pathq:int):

    G = self.graph
    N = [n for n, D in G.nodes(data=True) if pathq in D['paths']]
    G0 = G.subgraph(N)
    l = [n for n in N if G0.in_degree(n) == 0][0]
    r = [n for n in N if G0.out_degree(n) == 0][0]
    if l == r: return [l]
    y = list(networkx.all_simple_paths(G0, source=l, target=r))[0]

    return y

def simplify(frames:dict[DataFrame], norm=lambda x:x) -> dict:
  """
  Uproszcza nazwy kolumn w ramkach danych zachowując spójność,
  szczególnie dla `"id"` i nazw poprzedzonych `"&"` - relacje.
  """

  def colfilter(c): return (c != "id") and (not c.startswith('&'))

  H = frames
  K0 = [h for h in H.keys()]
  K = Pathalias(K0, norm=norm).unique
  K = { k0: norm(k) for k0, k in K.items() }

  Q = dict()
  for h, X in H.items():
    U0 = [c for c in X.columns if colfilter(c)]
    U = { k0: norm(k) for k0, k in Pathalias(U0, norm=norm).unique.items() }
    R = [c for c in X.columns if c.startswith("&")]

    U['id'] = 'id'
    for r in R: U[r] = "&" + K[r[1:]]

    Q[h] = U

  return { "frames": K, "columns": Q }