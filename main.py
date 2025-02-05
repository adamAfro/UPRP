import sys, pandas, re, os, asyncio, aiohttp
from lib.flow import Flow

import altair
altair.data_transformers.enable('vegafusion')

@Flow.From()
def Fetch(queries:pandas.Series, URL:str, outdir:str):

  "Pobieranie pełnych stron HTML z wynikami wyszukiwania."

  async def scrap(P:list):

    t = 1
    H = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
    async with aiohttp.ClientSession(headers=H) as S:
      for i, p in P.iterrows():
        j, d = p['country'].upper(), ''.join(re.findall(r'\d+', p['number']))
        o = f"{outdir}/{j}{d}.html"
        if os.path.exists(o): continue
        x = f"{URL}/{j}{d}"
        async with S.get(x) as y0:
          y = await y0.text()
          await asyncio.sleep(t)
          with open(o, "w") as f: f.write(y)

  _, P = queries
  asyncio.run(scrap(P))

from util import data as D
from profiling import flow as f0
from patent import flow as fP
from registry import flow as fA
from subject import flow as fS

import endo, patent, registry, subject, clst, grph, corr, grav

import raport

flow = dict()
for f in [f0, fP, fA, fS, { 'plot': { 'all': Flow('plot all', lambda *X: X, args=[F
  for d in [endo.plots, raport.plots, registry.plots, patent.plots] for F in d.values()]) } },
    { 'plot-endo': endo.plots }, 
    { 'plot-rprt': raport.plots }, 
    { 'plot-subj': subject.plots }, 
    { 'plot-rgst': registry.plots }, 
    { 'plot-patt': patent.plots }, 
    { 'plot-corr': corr.plots }, 
    { 'plot-grav': grav.plots },
    { 'plot-grph': grph.plots }, 
    { 'plot-clst': clst.plots }]:

  for k in f.keys():

    if isinstance(f[k], dict):
      if not k in flow.keys():
        flow[k] = dict()
      for h in f[k].keys():
        flow[k][h] = f[k][h]
    else:
      flow[k] = f[k]

flow['Google']['fetch'] = Fetch(raport.drop, 'https://patents.google.com/patent', outdir=D['Google']+'/web')

for a in sys.argv[1:]:

  f = None

  if ('.' in a):
    k, h = a.split('.')
    if (k in flow.keys()) and (h in flow[k].keys()):
      f = flow[k][h]
  elif a in flow.keys():
    f = flow[a]

  f(forced=True)