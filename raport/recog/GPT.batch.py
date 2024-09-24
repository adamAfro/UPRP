class GPT:
  
  import openai as AI
  
  key = "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"  
  client = AI.OpenAI(api_key=key)
  
  with open("prompt.GPT.txt", "r", encoding="utf-8") as f:
    prompt = f.read()

  def lsat(d=1, h=(0, 0), M=9, y=2024):
    from datetime import datetime
    t = datetime(y, M, d, h[0], h[1], 0).timestamp()
    return GPT.ls(t)

  def ls(t=None):
    c, Y = GPT.AI.NOT_GIVEN, []
    while True:
      B = GPT.client.batches.list(after=c, limit=100).data
      if (B is None) or (len(B) == 0): break
      c = B[-1].id
      if t is not None:
        B = [b for b in B if b.created_at > t]
      Y.extend(B)
    return Y
  
  def read(x):
    P, pg = x['custom_id'].split(".")
    P = P[len("P"):]
    r = x['response']
    if r['status_code'] != 200:
      raise Exception(r['status_code'])
    return P, pg, r['body']['choices'][0]['message']['content']

  def fetchat(d=1, h=(0, 0), M=9, y=2024):
    return GPT.fetch(GPT.lsat(d, h, M, y))
  
  def fetch(l:list):
    import json
    X = [(x.metadata['batch'] if x.metadata is not None else None, 
          GPT.client.files.content(x.output_file_id))
         for x in l if x.status == "completed"]
    Q = [q for (q, _) in X]
    B = [b.text for (_, b) in X]
    Y = [[json.loads(r)
          for r in b.split('\n') if r.strip() != ""] for b in B]
    return list(zip(Q, Y))
  
  def txtbatch(Fs:list):
    import json
    B = [GPT.req(f) for f in Fs]
    return '\n'.join([json.dumps(b) for b in B])

  def req(f:str):
    P, pg = f.split("/")[-1].split(".")[0:2]
    with open(f, "rb") as f: I = f.read()
    R = dict(custom_id=f"P{P}.{pg}", method="POST", url="/v1/chat/completions")
    R['body'] = dict(model="gpt-4o-mini", messages=[GPT.imgmsg(I)], temperature=0.0)
    return R

  def imgmsg(I:bytes):
    import base64
    E = base64.b64encode(I).decode('utf-8')
    ME = dict(type="image_url",
              image_url=dict(url=f"data:image/jpeg;base64,{E}"))
    M = dict(role="user", content=[GPT.prompt, ME])  
    return M
  
  def ask(f:str):
    b = GPT.client.files.create(file=open(f, "rb"), purpose="batch")
    GPT.client.batches.create(input_file_id=b.id,
                              endpoint="/v1/chat/completions",
                              completion_window="24h",
                              metadata=dict(batch=f))

def ls():
  from os import listdir, path
  D = [path.join("../img", d) for d in listdir("../img")]
  D = [d for d in D if path.isdir(d)]
  Fs = [listdir(d) for d in D if path.isdir(d)]
  Fs = [path.join(D[i], Fs[i][j])
        for i in range(len(Fs))
        for j in range(len(Fs[i])) if Fs[i][j].endswith(".crop.jpg")]
  return Fs

def prep(Fs:list[str], s = 64):
  from os import makedirs as dir
  from tqdm import tqdm as progress
  dir("batches", exist_ok=True)
  for i0 in progress(range(0, len(Fs), s)):
    i1 = min(i0 + s, len(Fs))
    with open(f"batches/{i0}+.jsonl", "w") as f:
      f.write(GPT.txtbatch(Fs[i0:i1]))

def rprep(Fs:list[str], s = 128):
  from random import sample
  from os import makedirs as dir
  dir("batches", exist_ok=True)
  with open(f"batches/random{s}.jsonl", "w") as f:
    f.write(GPT.txtbatch(sample(Fs, s)))

def pull(X:list):
  from os import path, makedirs as dir
  for q, B in X:
    if q is None: q = "?"
    for b in B:
      P, pg, r = GPT.read(b)
      d = path.join("output-openai", q)
      dir(d, exist_ok=True)
      o = path.join(d, f"{P}.{pg}.txt")
      with open(o, "w", encoding="utf-8") as f: 
        f.write(r)

rprep(ls(), 128)#sample
GPT.ask("batches/random128.jsonl")#send24requests
GPT.lsat(24, (11, 40), M=9)#checkprogress
X = GPT.fetchat(24, (11, 40), M=9)#getresults
pull(X)#writeresults