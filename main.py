import sys, altair
altair.data_transformers.enable('vegafusion')

flow = dict()

import endo, patent, registry, subject, clst, grph, corr, raport, exog

for d in [endo, patent, registry, subject, clst, grph, corr, raport, exog]:
  try: flow[d.__name__] = d.FLOW
  except AttributeError: pass

for a in sys.argv[1:]:

  f = None

  if ('.' in a):
    k, h = a.split('.')
    if (k in flow.keys()) and (h in flow[k].keys()):
      f = flow[k][h]
  elif a in flow.keys():
    f = flow[a]

  f(forced=True)