class GPT:
  
  import openai as AI

  key = "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"  
  client = AI.OpenAI(api_key=key)
  setup = dict(model="gpt-4o-mini", temperature=0.0)

  pricing_in = 0.15/1_000_000#pertoken
  pricing_out = 0.6/1_000_000#pertoken
  with open("prompt.GPT.txt", "r", encoding="utf-8") as f:
    prompt = f.read()
  
  def __init__(self):
    self.respond = 0
    self.priced = 0.00

  def recog(self, img:bytes):
    import base64
    E = base64.b64encode(img).decode('utf-8')
    ME = dict(type="image_url",
              image_url=dict(url=f"data:image/jpeg;base64,{E}"))
    M = dict(role="user", content=[GPT.prompt, ME])
    R = GPT.client.chat.completions.create(**GPT.setup, messages=[M])
    
    self.respond += 1
    self.priced += GPT.pricing_in*R.usage.prompt_tokens
    self.priced += GPT.pricing_out*R.usage.completion_tokens
    
    return R.choices[0].message.content

from os import path, listdir, makedirs as dir
D = [path.join("../img", d) for d in listdir("../img")]
D = [d for d in D if path.isdir(d)]
Fs = [listdir(d) for d in D if path.isdir(d)]
Fs = [path.join(D[i], Fs[i][j])
      for i in range(len(Fs))
      for j in range(len(Fs[i])) if Fs[i][j].endswith(".crop.jpg")]

import json
try:
  with open("sample.GPT.100.json", "r", encoding="utf-8") as f:
    S = json.load(f)
except:
  import random 
  S = random.sample(Fs, 100)
  with open("sample.GPT.100.json", "w", encoding="utf-8") as f:
    json.dump(S, f)

def recog(X:list[str]):
  from tqdm import tqdm as progress
  G = GPT()
  P = progress(X)  
  for p in P:
    o = p.replace("../img", "output-openai")
    dir(path.dirname(o), exist_ok=True)
    o = o.replace(".crop.jpg", ".csv")
    with open(p, "rb") as f: img = f.read()
    R = G.recog(img)
    P.set_postfix({"$": G.priced/G.respond})
    if R.lower().startswith("```csv"): R = R[6:]
    if R.startswith("\n"): R = R[1:]
    if R.lower().endswith("```"): R = R[:-3]
    if R.endswith("\n"): R = R[:-1]
    with open(o, "w", encoding="utf-8") as f: f.write(R)

recog(S)