data = { 'UPRP': 'api.uprp.gov.pl',
         'Lens': 'api.lens.org',
         'USPG': 'developer.uspto.gov/grant',
         'USPA': 'developer.uspto.gov/application',
         'Google': 'patents.google.com' }

def strnorm(x, dropinter:bool, dropdigit:bool):

  import unicodedata, re

  if not isinstance(x, str): return None
  x = x.upper()
  n = unicodedata.normalize('NFKD', x)
  y = ''.join([c for c in n if not unicodedata.combining(c)])
  
  for a, b in {'Ą': 'A', 'Ć': 'C', 'Ę': 'E', 
               'Ł': 'L', 'Ń': 'N', 'Ó': 'O', 
               'Ś': 'S', 'Ź': 'Z', 'Ż': 'Z'}.items():

      y = y.replace(a, b)

  if dropdigit:
    y = re.sub(r'[\s\d]+', ' ', y)

  if dropinter:
    y = re.sub(r'[\s\W]+', ' ', y).strip()

  return y

class Colr:

  neutral = 'blue'
  good = 'green'
  mid = 'yellow'
  attention = 'orange'
  warning = 'red'
  
class Cmap:

  neutral = 'Blues'
  good = 'Greens'
  mid = 'YlOrBr'
  attention = 'Oranges'
  warning = 'Reds'

import matplotlib.pyplot as plt
plt.rcParams['axes.spines.top'] = False
plt.rcParams['axes.spines.right'] = False

class Annot:

  def bar(ax, nbarfix=2, rbarfix=0.02, fixh = 12):
    H = []
    R = ax.get_ylim()[1] - ax.get_ylim()[0]
    for p in ax.patches:
      h = p.get_height()
      z = -1 if h > 0.5*R else 1
      if h == 0: continue
      T = (0, z*fixh)
      for h0 in H:
        if abs(h - h0) < rbarfix*R:
          T = (0+T[0], fixh+T[1])
      H = [h] + H[:nbarfix]
      ax.annotate(f'{h}', (p.get_x() + p.get_width() / 2., h),
                  bbox=dict(boxstyle="round,pad=0.3", edgecolor='black', facecolor='white', alpha=0.7),
                  ha='center', va='center', xytext=T, textcoords='offset points',
                  arrowprops=dict(arrowstyle='-', color='black', shrinkA=0, shrinkB=0))
