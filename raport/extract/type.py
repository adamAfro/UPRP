# Obsługa jąder Jupyter
# ---------------------
#
# * `OLLAMA_HOST` wymaga ręcznego wpisania aktywnego portu OLLAMA.
import sys
ipynb = hasattr(sys, 'ps1') or 'ipykernel' in sys.modules
OLLAMA_HOST = None

# OBSŁUGA TERMINALA
# -----------------
#
# `cd ~/..?/UPRP`
# `python raport/extract/type.py nohup ../../ollama/bin 10`
if not ipynb:
  import os, subprocess, time, tqdm, json
  if len(sys.argv) >= 2:
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    if sys.argv[1] == "nohup":
      if len(sys.argv) > 3:

        OLLAMA_DIR, OLLAMA_N = sys.argv[2], int(sys.argv[3])
        OLLAMA_HOST = [f"127.0.0.1:{P}" for P in range(11434, 11434+OLLAMA_N)]
        for h in OLLAMA_HOST:
          L_PROC = subprocess.Popen(f"nohup {OLLAMA_DIR}/ollama serve > {OLLAMA_DIR}/../log/{h}.log 2>&1 &", 
            env={ "CUDA_VISIBLE_DEVICES": "0", "OLLAMA_HOST": h, "HOME": os.path.expanduser("~") }, shell=True)
          print("ollama", L_PROC.pid, f"{OLLAMA_DIR}/../log/{h}.log")
          for i in tqdm.tqdm(range(50), desc="⌛-buffor"): time.sleep(0.1)

        PY_PROC = subprocess.Popen(f'nohup python {__file__} > type.log 2>&1 &', shell=True)
        with open("type.host.json", "w") as f: f.write(json.dumps(OLLAMA_HOST))

        print(PY_PROC.pid)
      else: print("???")
      exit()
  with open("type.host.json", "r") as f:
    OLLAMA_HOST = json.loads(f.read())

log_file = open("type.log", "a", buffering=1)
sys.stdout = log_file
print(OLLAMA_HOST)

# Właściwy skrypt
# ===============
#
# Skrypt prowadzi krótki dialog z modelem LLM nt. zadanych referencji,
# które nie są jeszcze określone jako kody patentowe. Jego główne zadanie
# to wyszukiwanie cytowań biblograficznych w tekście i ew. próba
# wydobycia tytuły i autorów. Wyniki zapisywane są w plikach CSV.
from pandas import read_csv, DataFrame
from tqdm.asyncio import tqdm as progress
import json, asyncio, time
from ollama import AsyncClient as LLM

def qLLM(): # LLM z kolejki LLMów
  global OLLAMA_HOST
  OLLAMA_HOST = OLLAMA_HOST[1:] + [OLLAMA_HOST[0]]
  return LLM(host=f"http://{OLLAMA_HOST[0]}")

async def askdict(q:str, y0, D0:list=[], M=None):
    if M is None: M = qLLM()
    q += '\nExpected values are: ' + ', '.join(y0.keys()) + \
          f'\nFor example: {{ "value": "example-value" }}'
    D = [{ 'role': 'user', 'content': q }]
    r = (await M.chat(model='llama3.2', messages=D0 + D))['message']['content']
    D += [{ 'role': 'assistant', 'content': r, 'question': q }]
    try: y = y0[json.loads(r)["value"].lower()]
    except: y = None
    if y is None: return D
    for q, y0 in y.items():
      D += await ask(q, y0, D0 + D, M)
    return D

async def ask(q:str, y0, D0:list=[], M=None):
  if M is None: M = qLLM()
  if type(y0) is dict: return await askdict(q, y0, D0, M)
  if type(y0) is str:
    q += '\nExpected output type is as follows: ' + y0 + \
         ' or "" if there is not any, for example: ' + \
         '{ "value": "example" } or { "value": "" }'
  elif y0.__name__ == 'list':
    q += '\nExpected output type is a list ' + \
         ' or empty list [] if there is not any, for example: ' + \
         '{ "value": ["example-1", "example-2"] } or { "value": [] }'
  elif type(y0) is type:
    q += '\nExpected output type is: ' + y0.__name__ + \
         ' or "" if there is not any ' + \
         ' with key "value"; for example: { "value": "example" }'
  D = [{ 'role': 'user', 'content': q }]
  r = (await M.chat(model='llama3.2', messages=D0+D))['message']['content']
  D += [{ 'role': 'assistant', 'content': r, 'question': q }]
  return D

async def asktry(q:str, y0, D0:list=[]):
  try:
    Y = []
    for k in (await ask(q, y0, D0)):
      if k['role'] != 'assistant': continue
      try: Y.append({ 'question': k['question'], 'value': json.loads(k['content'])['value']})
      except: Y.append({ 'question': k['question'], 'value': '💣' + k['content']})
    return Y
  except: return []

e = read_csv('../linkage.code.ngram-quality.csv')
X0 = read_csv('../docs.csv').reset_index()\
    .rename(columns={'docs':'text', 'index':'docs'})[['docs', 'text']]\
    .query('~ docs.isin(@e.docs)')

s = "Your task is to answer questions about" + \
    "this particular citation reference. " + \
    "Provide your response in JSON format" + \
    "expected by user. Be specific and exact. " + \
    'Always return your response in key named "value"'

q0, y0 = "To which category, does the reference references?", {

  "patent": {
    "What country is it from? Refer to it by found short code.": str,
    "What number does the patent have?": str,
    "What authors are listed?": list,
    "What is the date of publication?": str,
  },

  "scientific": {
    "What title does it have?": str,
    "What authors are listed?": list,
    "What is the year of the publication?": str,
    "What is the month of the publication?": str,
    "What is the day of the publication?": str,
    "What journal is it from?": str,
  },

  "website": None,
  "book": None,
}

i0, N0 = 0, X0.shape[0]
t0 = time.time()

async def parallel(X, interactive=True):
  global i0, N0, t0
  U = asyncio.Semaphore(4*len(OLLAMA_HOST))
  async def f0(i, q, y0, d):
    async with U:
      y = await asktry(q, y0, d)
      for k in y: k['docs'] = i
      return y

  Q = []
  for i, t in X.itertuples(index=False):
    Q.append(f0(i,q0, y0, [{ 'role': 'system', 'content': s + '\nHere is the reference:\n' + t }]))

  Y = []
  if interactive:
    async for f in progress(asyncio.as_completed(Q), total=len(Q)):
      Y.append(await f)
  else:
    for f in asyncio.as_completed(Q):
      y = await f
      i0 += 1
      print(f"{i0}/{N0} {time.time()-t0:.2f} ETA {(N0-i0)*(time.time()-t0)/i0:.2f}")
      Y.append(y)

  return Y

async def jupyter():
  b = 256
  for b0 in range(0, len(X0), b):
    Y = await parallel(X0.iloc[b0:b0+b], interactive=True)
    V = DataFrame([y for Y0 in Y for y in Y0])\
      .pivot(index='docs', columns='question', values='value')
    V.to_csv(f'type.batches/{b0}.csv')

def terminal():
  b = 256
  for b0 in range(0, len(X0), b):
    Y = asyncio.run(parallel(X0.iloc[b0:b0+b], interactive=False))
    V = DataFrame([y for Y0 in Y for y in Y0])\
      .pivot(index='docs', columns='question', values='value')
    V.to_csv(f'type.batches/{b0}.csv')
    
if ipynb: jupyter()
else: terminal()