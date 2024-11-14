from pandas import read_csv, DataFrame, Series
from transformers import pipeline, AutoTokenizer as Tokenizer
from transformers import TFAutoModelForTokenClassification as Classy
import tensorflow as tf
for x in tf.config.list_physical_devices('GPU'): print(x)

import ray, psutil, time
from ray.experimental.tqdm_ray import tqdm as progress 

u = "Babelscape/wikineural-multilingual-ner"
M = Tokenizer.from_pretrained(u)
T = Classy.from_pretrained(u, from_pt=True)
NLP = pipeline("ner", model=T, tokenizer=M, device=0,
               grouped_entities=True, framework='tf')

X = read_csv('../docs.csv')
X = X.reset_index().rename(columns={'docs':'text', 'index':'docs'})
X = X[['docs', 'text']]

t0 = time.time()
L = 0.80#proclimit
n, N = X.shape[0], int(round(L*psutil.cpu_count(logical=True), 0))
N = min(n, N)
B = [X.iloc[i:i+n//N+1] for i in range(0, n, n//N+1)]

ray.init(num_cpus=N, ignore_reinit_error=True)
@ray.remote
def f(X, f0, k=None):
  if k is None: return [{ 'docs': x['docs'], **y }
    for x in progress(X.to_dict(orient="records"), total=X.shape[0])
    for y in f0(x['text'])]

  print(f"{k}: 0/{X.shape[0]}", flush=True)
  t0, Y = time.time(), []
  L = [X.shape[0]*i//10 for i in range(1, 11)]
  for i, x in enumerate(X.to_dict(orient="records")):
    if i in L:
       t = (time.time()-t0)
       avg = t/(i+1)
       ETA = (X.shape[0]-i)*avg
       print(f"{k}: {i}/{X.shape[0]}, {round(t,2)} it={round(avg,2)} {round(ETA, 2)}", flush=True)
    Y.extend([{ 'docs': x['docs'], **y } for y in f0(x['text'])])
  return Y

R = ray.put(NLP)
Y = ray.get([f.remote(b, R, i) for i, b in enumerate(B)])
Y = DataFrame([E for y in Y for E in y])
ray.shutdown()
print(time.time()-t0)

k = Y['entity_group'].unique()
H1 = Y['entity_group'].apply(lambda x: [x == k for k in k])
H1 = DataFrame(H1.tolist(), columns=[f"wikineural-{k}" for k in k])

Y = Y.drop(columns=["entity_group"])
Y = Y.rename({"word": "text", "score": "score-wikineural" }, axis=1)
Y = Y.join(H1)
Y.to_csv('wikin.csv', index=False)
print("wikin.csv")