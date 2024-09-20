"wyszukiwanie kształtów tabel za pomocą linii prostych"
from typing import Set, Tuple
import os, cv2, pandas, networkx, shapely, re
os.chdir("/home/adam/Projekty/UPRP/raport/img")

def shapesearch(P:str):
  "zwraca graf punktów linii z pliku"
  from numpy import pi
  from cv2 import imread
  from cv2 import Canny, HoughLinesP
  
  img = imread(P)
  height, width = img.shape[0], img.shape[1]
  edges = Canny(img, 0, 250)
  mx = HoughLinesP(edges, 1, pi/180, round(width/3), minLineLength=0.05*width, maxLineGap=10)
  if mx is None: return None
  
  return shapesearchmx(mx, 0.015*width, yscale=2)

def shapesearchmx(mx, segsize:float, yscale=1, mincyclelen=4, containertres=0.9):
  "zwraca graf punktów na podstawie macierzy, gdzie krawędzie to linie"
  from pandas import DataFrame
  from networkx import from_pandas_edgelist
  
  pts, eds, ptset = [], set(), set()
  for line in [m[0] for m in mx]:
    segmentify(line, segsize, pts, eds, ptset)

  P = DataFrame(pts, columns=['x', 'y'])
  E = DataFrame(eds, columns=['start', 'end'])
  P['y'] *= yscale
  P, E = ptsmerge(P, E, distmax=segsize*2.66)
                                          #^powinno wystarczyć do wyszukiwania 
                                          # przecinających się linii
  # P, E = ptsmerge(P, E, distmax=segsize*2.66)# poprawka
  P['y'] /= yscale
  
  E.sort_values(by=['start', 'end'], inplace=True)
  E.drop_duplicates(inplace=True)
  G = from_pandas_edgelist(E.rename(columns={'start': 'source', 
                                             'end': 'target'}))   
  E = cyclesearch(G, minlen=mincyclelen)
  polys = mkpolys(P, E)
  polys = rmcontainers(polys, tres=containertres)

  return polys

def cyclesearch(G:networkx.Graph, minlen=4):
  "szuka cykli w grafie"
  from pandas import DataFrame
  C = list(networkx.simple_cycles(G))
  Cset = [set(c) for c in C]
  Csuper = [any(subset < c for subset in Cset if subset != c) for c in Cset]
  edgs = []
  for ic, (c, hassub) in enumerate(zip(C, Csuper)):
    if hassub: continue
    if len(c) < minlen: continue
    start = c[0]
    for i in range(1, len(c)):
      edgs.append((ic, start, c[i]))
      start = c[i]
    edgs.append((ic, start, c[0]))
  
  return DataFrame(edgs, columns=['shape', 'start', 'end'])

def mkpolys(pts: pandas.DataFrame, edgs: pandas.DataFrame):
  "zwraca wielokąty z grafu"
  from shapely.geometry import Polygon, Point

  shapes = []
  for num, S in edgs.groupby('shape'):
    xstart, ystart = pts.loc[S['start'], ['x', 'y']].values.T
    xend, yend = pts.loc[S['end'], ['x', 'y']].values.T
    poly = Polygon([(x, y) for x, y in zip(xstart, ystart)])
    for x, y in zip(xend, yend):
      poly = poly.union(Point(x, y))
    shapes.append(poly)
    
  return shapes

def rmcontainers(shps: list[shapely.geometry.Polygon], tres:float):
  "usuwa wielokąty które zawierają przynajmniej `tres`-punktów innego"      
  return [shp for i, shp in enumerate(shps) 
           if not countinner(i, shps, tres)]

def countinner(i:int, shps: list[shapely.geometry.Polygon], tres:float):
  "sprawdza czy wielokąt zawiera przynajmniej `tres`-punktów jakiegoś innego"
  from shapely.geometry import Point
  for j, shptest in enumerate(shps):
    if i == j: continue
    pts = [Point(pt) for pt in shptest.exterior.coords]
    contained = sum(1 for pt in pts if shps[i].contains(pt) or shps[i].touches(pt))
    if contained / len(shptest.exterior.coords) > tres:
      return True
  return False

def segmentify(line:list, lmax:float, 
               pts:list[Tuple[int, int]],
               eds:Set[Tuple[int, int] ],
               ptset:Set[Tuple[int, int]]) -> None:

  "dodaje punkty i krawędzie długości `lmax` do grafu; bez powtórzeń"
  from numpy import abs, sqrt

  start = (line[0], line[1])
  if start in ptset: idstart = pts.index(start)
  else:
    idstart = len(pts)#added 
    ptset.add(start), pts.append(start)
  
  end = (line[2], line[3])
  vertical = abs(end[0] - start[0]) < 1e-6
  if vertical:
    l = abs(end[1] - start[1])
    inbetween = round(l / lmax)
    for i in range(1, inbetween):
      x = start[0]
      y = (end[1] - start[1]) * i / inbetween + start[1]
      pt = (round(x), round(y))
      if pt in ptset: idpt = pts.index(pt)
      else:
        idpt = len(pts)
        ptset.add(pt), pts.append(pt)
      eds.add((idstart, idpt))
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
      if pt in ptset: idpt = pts.index(pt)
      else:
        idpt = len(pts)#added 
        ptset.add(pt), pts.append(pt)
      eds.add((idstart, idpt))
      start, idstart = pt, idpt

  if end in ptset: idend = pts.index(end)
  else:
    idend = len(pts)#added 
    ptset.add(end), pts.append(end)

  eds.add((idstart, idend))

def ptsmerge(pts: pandas.DataFrame, eds: pandas.DataFrame, distmax=32):
  "łączy pobliskie punkty w grafie i dostosowuje krawędzie"

  from scipy.spatial.distance import pdist
  from scipy.cluster.hierarchy import linkage, fcluster
  dists = pdist(pts[['x', 'y']], metric='euclidean')
  Z = linkage(dists, method='ward')
  C = fcluster(Z, distmax, criterion='distance')
  Cpts = pts.groupby(C).mean()
  pts2Cpts = dict(zip(pts.index, C))

  Ceds = eds[['start', 'end']]
  Ceds['start'] = eds['start'].map(pts2Cpts)
  Ceds['end'] = eds['end'].map(pts2Cpts)
  Ceds = Ceds[Ceds['start'] != Ceds['end']]
  
  return Cpts, Ceds

def polyplotsolo(polygon, ax, color='blue', index=None):
  from shapely.geometry import Polygon

  if isinstance(polygon, Polygon):
    exterior_coords = polygon.exterior.coords
    ax.fill(*zip(*exterior_coords), color=color, alpha=0.1)
    for interior in polygon.interiors:
      interior_coords = interior.coords
      ax.fill(*zip(*interior_coords), color='white', alpha=0.2)

    if index is not None:
      centroid = polygon.centroid
      ax.text(centroid.x, centroid.y, str(index), ha='center', va='center', fontsize=12, color='black')

def polyplot(polygons, colors=None):
  import matplotlib.pyplot as plt
  fig, ax = plt.subplots()
  for i, polygon in enumerate(polygons):
    color = colors[i] if colors else 'blue'
    polyplotsolo(polygon, ax, color, index=i+1)
  ax.set_aspect('equal')
  plt.show()

recog = pandas.read_csv('../recog/paddle.csv')
dirs = os.listdir('./')
paths = [os.listdir(D) for D in dirs if os.path.isdir(D)]
paths = [os.path.join(dirs[i], paths[i][j])
         for i in range(len(paths))
         for j in range(len(paths[i]))]

shps = shapesearch(paths[0])
polyplot(shps)