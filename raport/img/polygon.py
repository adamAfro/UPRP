class Linesearch:
  
  class Filter:
    def __init__(self, minlength=0.05, maxgap=10):
      self.minlength, self.maxgap = minlength, maxgap

  class Kernel:
    def __init__(self, resolution=1, angle=0.0174, treshold=1/3, spectrum=[0, 250]):
      self.resolution, self.angle, self.treshold = resolution, angle, treshold
      self.spectrum = spectrum

  def __init__(self, img, F=Filter(), K=Kernel()):
    from cv2 import Canny as all, HoughLinesP as straight
    h, w = img.shape[0], img.shape[1]
    p = dict(minLineLength=F.minlength*w, maxLineGap=F.maxgap)#params
    L = all(img, K.spectrum[0], K.spectrum[1])
    L = straight(L, K.resolution, K.angle, round(K.treshold*w), **p)
    self.lines = [X[0] for X in L]
    
  def plot(self, ax=None):
    import matplotlib.pyplot as plt
    if ax is None: ax = plt.gca()
    for l in self.lines:
      plt.plot([l[0], l[2]], [l[1], l[3]], 'r-')
    return ax

class Graphsearch:
  
  from pandas import DataFrame
  from networkx import Graph
  from shapely import geometry

  class Kernel:
    def __init__(self, segment=0.015, distmax=2.66):
      self.segment, self.distmax = segment, distmax

  def __init__(self, lines:list[float], width:float, K=Kernel()):
    from networkx import from_pandas_edgelist as graph
    Pt, E = Graphsearch.frame(lines, width*K.segment)
    Pt, E = Graphsearch.merge(Pt, E, distmax=width*K.segment*K.distmax)
    E = E.sort_values(by=['start', 'end']).drop_duplicates()
    G = graph(E.rename(columns={'start': 'source', 'end': 'target'}))
    self.points, self.edges = Pt, Graphsearch.cycle(G)
    
  def frame(lines:list[float], segment:float):
    from pandas import DataFrame
    Pt, E, Ptmemory = [], set(), set()
    for l in lines: Graphsearch.segmentify(l, segment, Pt, E, Ptmemory)
    return (DataFrame(Pt, columns=['x', 'y']),
            DataFrame(E, columns=['start', 'end']))

  def merge(Pt:DataFrame, E:DataFrame, distmax:float):
    from scipy.spatial.distance import pdist as dist
    from scipy.cluster.hierarchy import linkage, fcluster as cluster
    D = dist(Pt[['x', 'y']], metric='euclidean')
    Z = linkage(D, method='ward')
    C = cluster(Z, distmax, criterion='distance')
    PtC = Pt.groupby(C).mean()
    Pt2PtC = dict(zip(Pt.index, C))
    EC = E[['start', 'end']]
    EC['start'] = E['start'].map(Pt2PtC)
    EC[ 'end' ] = E[ 'end' ].map(Pt2PtC)
    EC = EC[EC['start'] != EC['end']]
    return PtC, EC

  def cycle(G:Graph, minlen=4):
    from pandas import DataFrame
    from networkx import simple_cycles
    C, E = list(simple_cycles(G)), []
    Cmemory = [set(c) for c in C]
    Csuper = [any(subset < c for subset in Cmemory if subset != c) for c in Cmemory]
    for ic, (c, hassub) in enumerate(zip(C, Csuper)):
      if hassub or (len(c) < minlen): continue
      start = c[0]
      for i in range(1, len(c)):
        E.append((ic, start, c[i]))
        start = c[i]
      E.append((ic, start, c[0]))
    return DataFrame(E, columns=['cycle', 'start', 'end'])

  def segmentify(line:list, lmax:float, Pt:list, E:set, Ptmemory:set) -> None:
    from numpy import abs, sqrt
    start = (line[0], line[1])
    if start in Ptmemory: idstart = Pt.index(start)
    else:
      idstart = len(Pt)#added 
      Ptmemory.add(start), Pt.append(start)
    
    end = (line[2], line[3])
    vertical = abs(end[0] - start[0]) < 1e-6
    if vertical:
      l = abs(end[1] - start[1])
      inbetween = round(l / lmax)
      for i in range(1, inbetween):
        x = start[0]
        y = (end[1] - start[1]) * i / inbetween + start[1]
        pt = (round(x), round(y))
        if pt in Ptmemory: idpt = Pt.index(pt)
        else:
          idpt = len(Pt)
          Ptmemory.add(pt), Pt.append(pt)
        E.add((idstart, idpt))
        start, idstart = pt, idpt

    else:
      acoef = (end[1] - start[1]) / (end[0] - start[0])
      bcoef = start[1] - acoef * start[0]
      l = sqrt((end[0] - start[0])**2 + (end[1] - start[1])**2)
      inbetween = round(l / lmax)
      for i in range(1, inbetween):
        x = (end[0] - start[0]) * i / inbetween + start[0]
        y = acoef * x + bcoef
        pt = (round(x), round(y))
        if pt in Ptmemory: idpt = Pt.index(pt)
        else:
          idpt = len(Pt)#added 
          Ptmemory.add(pt), Pt.append(pt)
        E.add((idstart, idpt))
        start, idstart = pt, idpt

    if end in Ptmemory: idend = Pt.index(end)
    else:
      idend = len(Pt)#added 
      Ptmemory.add(end), Pt.append(end)

    E.add((idstart, idend))
    
  def plot(self, ax=None):
    import matplotlib.pyplot as plt
    if ax is None: ax = plt.gca()
    for i, e in self.edges.iterrows():
      xstart, ystart = self.points.loc[e['start'], ['x', 'y']].values
      xend, yend = self.points.loc[e['end'], ['x', 'y']].values
      plt.plot([xstart, xend], [ystart, yend], 'b-')
    return ax

class Polysearch:
  
  from pandas import DataFrame
  from networkx import Graph
  from shapely import geometry
  
  class Filter:
    def __init__(self, areamin=0.01, innermax=0.9, cyclemin=4):
      self.areamin, self.innermax, self.cyclemin = areamin, innermax, cyclemin
      
  def __init__(self, Pt:DataFrame, E:DataFrame, F=Filter()):
    from shapely.geometry import Polygon
    P = []
    for _, c in E.groupby('cycle'):
      xstart, ystart = Pt.loc[c['start'], ['x', 'y']].values.T
      p = Polygon([(x, y) for x, y in zip(xstart, ystart)])
      P.append(p if p.is_valid else p.buffer(0))
    self.polygons = Polysearch.downsize(P, F.innermax)

  def downsize(P:list[geometry.Polygon], tres:float):
    return [p for i, p in enumerate(P) 
            if not Polysearch.downsizecount(i, P, tres)]

  def downsizecount(i:int, shps: list[geometry.Polygon], tres:float):
    from shapely.geometry import Point
    for j, shptest in enumerate(shps):
      if i == j: continue
      pts = [Point(pt) for pt in shptest.exterior.coords]
      contained = sum(1 for pt in pts if shps[i].contains(pt) or shps[i].touches(pt))
      if contained / len(shptest.exterior.coords) > tres:
        return True
    return False
  
  def plot(self, ax=None, annotate=False):
    import matplotlib.pyplot as plt
    if ax is None: ax = plt.gca()
    for i, p in enumerate(self.polygons):
      exterior_coords = p.exterior.coords
      ax.fill(*zip(*exterior_coords), color='blue', alpha=0.1)
      for interior in p.interiors:
        interior_coords = interior.coords
        ax.fill(*zip(*interior_coords), color='white', alpha=0.2)
      if annotate:
        x, y = p.centroid.xy
        ax.text(x[0], y[0], f'{i}', fontsize=8, color='black')
    return ax