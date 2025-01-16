import sys, pandas, re, os, asyncio, aiohttp, zipfile
from lib.flow import Flow

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

@Flow.From()
def Bundle(dir:str,

           matches:   dict[str, pandas.DataFrame],
           pull:      dict[str, tuple[pandas.DataFrame]],

                                                         ):

  "Zapisuje połączone wyniki przetwarzania do plików CSV."

  M0, U = matches, pull
  Z0 = { k: v[0] for k, v in U.items() }
  G0 = { k: v[1] for k, v in U.items() }
  T0 = { k: v[2] for k, v in U.items() }
  C0 = { k: v[3] for k, v in U.items() }
  P0A = { k: v[4][0] for k, v in U.items() }
  P0B = { k: v[4][1] for k, v in U.items() }

  #reindex
  for M in M0.values():
    if M.empty: continue
    M.columns = ['::'.join(map(str, k)).strip('::') for k in M.columns.values]

  for G in G0.values():
    if G.empty: continue
    G.set_index('doc', inplace=True)

  for C in C0.values(): 
    if C.empty: continue
    C.reset_index(inplace=True)
    C.set_index('doc', inplace=True)
    C.drop(columns='id', inplace=True)

  for T in T0.values():
    if T.empty: continue
    pass

  #merge
  for k in M0.keys():

    M = M0[k]
    M['docrepo'] = k
    M.set_index('docrepo', append=True, inplace=True)

    try:
      Z, G, T, C, PA, PB = Z0[k], G0[k], T0[k], C0[k], P0A[k], P0B[k]
      for X in [Z, G, T, C, PA, PB]:
        X['docrepo'] = k
        X.set_index('docrepo', append=True, inplace=True)
    except KeyError: continue

  Z0 = [X for X in Z0.values() if not X.empty]
  M0 = [X for X in M0.values() if not X.empty]
  G0 = [X for X in G0.values() if not X.empty]
  T0 = [X for X in T0.values() if not X.empty]
  C0 = [X for X in C0.values() if not X.empty]
  P0A = [X for X in P0A.values() if not X.empty]
  P0B = [X for X in P0B.values() if not X.empty]

  Z0 = pandas.concat(Z0, axis=0)
  M0 = pandas.concat(M0, axis=0)
  G0 = pandas.concat(G0, axis=0)
  T0 = pandas.concat(T0, axis=0)
  C0 = pandas.concat(C0, axis=0)
  P0A = pandas.concat(P0A, axis=0)
  P0B = pandas.concat(P0B, axis=0)

  Z0.to_csv(f'{dir}/pat.csv')
  M0.to_csv(f'{dir}/pat:pat-raport-ocr.csv')
  G0.to_csv(f'{dir}/spatial:pat.csv')
  T0.to_csv(f'{dir}/event:pat.csv')
  C0.to_csv(f'{dir}/classification:pat.csv')
  P0A.to_csv(f'{dir}/people:pat-signed.csv')
  P0B.to_csv(f'{dir}/people:pat-named.csv')

  with zipfile.ZipFile(f'{dir}/dist.zip', 'w') as z:
    z.write(f'{dir}/pat.csv', 'pat.csv')
    z.write(f'{dir}/pat:pat-raport-ocr.csv', 'pat:pat-raport-ocr.csv')
    z.write(f'{dir}/spatial:pat.csv', 'spatial:pat.csv')
    z.write(f'{dir}/event:pat.csv', 'event:pat.csv')
    z.write(f'{dir}/classification:pat.csv', 'classification:pat.csv')
    z.write(f'{dir}/people:pat-signed.csv', 'people:pat-signed.csv')
    z.write(f'{dir}/people:pat-named.csv', 'people:pat-named.csv')
  
from dirs import data as D
from profiling import flow as f0
from pull import flow as f1
from raport import flow as f2
from bundle import bplot

flow = dict()
flow['plot'] = bplot
for f in [f0, f1, f2]:
  for k in f.keys():

    if isinstance(f[k], dict):
      if not k in flow.keys():
        flow[k] = dict()
      for h in f[k].keys():
        flow[k][h] = f[k][h]
    else:
      flow[k] = f[k]

flow['Google']['fetch'] = Fetch(flow['drop'], 'https://patents.google.com/patent', outdir=D['Google']+'/web')

flow['bundle'] = Bundle('bundle', matches={ k: flow[k]['narrow'] for k in D.keys() },
                        pull={ k: flow[k]['pull'] for k in D.keys() if '-' not in k })

for a in sys.argv[1:]:

  if '.' in a:
    k, h = a.split('.')
    flow[k][h](forced=True)
  else:
    flow[a](forced=True)