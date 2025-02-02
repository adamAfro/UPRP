import pandas
from statsmodels.tsa.stattools import adfuller, kpss

def stationary(X:pandas.DataFrame, obs:str, date:str, trend=True):

  N = X.value_counts([obs]+[date]).reset_index()
  N = N.set_index(obs)

  def tonumpy(v):
    y = pandas.DataFrame(index=pandas.date_range(v.index.min(), v.index.max()))
    return y.join(v).fillna(0)

  Y = []
  for g in N.index.unique():

    V = tonumpy(N.loc[g].set_index(date)['count'])

    ADF = adfuller(V, regression='ct' if trend else 'c')
    Y.append({ obs: g, 'test': 'ADF', 'p': ADF[1] })

    KPSS = kpss(V, regression='ct')
    Y.append({ obs: g, 'test': 'KPSS', 'p': KPSS[1] })

  V = tonumpy(N.groupby(date).sum())
  ADF = adfuller(V, regression='ct' if trend else 'c')
  Y.append({ obs: None, 'test': 'ADF', 'p': ADF[1] })

  KPSS = kpss(V, regression='ct')
  Y.append({ obs: None, 'test': 'KPSS', 'p': KPSS[1] })

  Y = pandas.DataFrame(Y)

  return Y