import pandas, matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator

plt.rcParams['axes.spines.top'] = False
plt.rcParams['axes.spines.right'] = False

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


def monthly(X:pandas.DataFrame, by:str, title='W danym miesiącu', x='miesiąc'):

  d = X['year'].str.zfill(2)+'-'+X['month'].str.zfill(2)

  M = X.groupby([d, by]).size().unstack(fill_value=0).sort_index()

  f, A = plt.subplots(len(M.columns), constrained_layout=True,
                      sharex=True, sharey=True, figsize=(8, 1+len(M.columns)))

  for i, k in enumerate(M.columns):
    M[k].plot.bar(xlabel='miesiąc', ax=A[i], ylabel=k, rot=0)
    A[i].xaxis.set_major_locator(MaxNLocator(integer=True, prune='both'))

  A[0].set_title(title)

  return f

def NA(X: pandas.DataFrame):

  Y = pandas.DataFrame({'Dane': X.notna().sum(), 'Braki': X.isna().sum()})

  f, A = plt.subplots(1, figsize=(8, 4), tight_layout=True)

  Y.plot(kind='bar', stacked=True, color=[Colr.good, Colr.warning], ax=A)
  A.set_ylabel('Liczba wartości')
  A.set_title('Braki danych w kolumnach')
  A.legend()

  return f