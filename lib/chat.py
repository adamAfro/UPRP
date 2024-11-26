from ollama import AsyncClient as LLM
import json, asyncio, subprocess, os, time
from .log import progress_async, log

class Dialog:

  def __init__(self, model,
               plan:dict[str, dict|str|type|None],
               messages=[]):

    self.plan = plan
    self.messages = messages
    self.model = model

  async def ask(self):

    q0 = next(iter(self.plan))
    H = self.plan[q0]
    await self._ask(q0, H)

    R = [(r['question'], r['content']) for r in self.messages
                                       if r['role'] == 'assistant']
    V = []
    for q, x in R:
      try: V.append({ "question": q, "value": json.loads(x)['value'], "valid": True })
      except: V.append({ "question": q, "value": x })

    return V

  async def _ask(self, question:str, plan:dict):

    "Zaczyna sesję pytań"

    M = self.model
    q = question
    H = plan

    if type(H) is dict: return await self._askdict(q, H)
    else: q += '\nExpected output is an empty list [] if not applicable' + \
               ' or a list of the following type: ' + H

    m0 = { 'role': 'user', 'content': q }
    self.messages.append(m0)

    r = await M.chat(model='llama3.2', messages=self.messages)
    m = { 'role': 'assistant', 'content': r['message']['content'], 'question': q }

    self.messages.append(m)

  async def _askdict(self, question:str, plan):

      "Kontynuuje sesję pytań dla słownika"

      M = self.model
      q = question
      H0 = plan

      q += '\nExpected values are: ' + ', '.join(H0.keys()) + \
            f'\nFor example: {{ "value": "example-value" }}'

      m0 = { 'role': 'user', 'content': q }
      self.messages.append(m0)

      r = (await M.chat(model='llama3.2', messages=self.messages))
      m = { 'role': 'assistant', 'content': r['message']['content'], 'question': q }
      self.messages.append(m)

      try:
        y = json.loads(r['message']['content'])["value"].lower()
        H = H0[y]
      except: return
      if H is None: return
      for q, H0 in H.items():
        await self._ask(q, H0)

class Crowd:

  def __init__(self, hosts:list):
    self.models = { h: LLM(host=h) for h in hosts }
    self.que = hosts

  async def briefing(self, ollamadir:str|None=None):
    for h, x in self.models.items():
      try:
        m0 = { 'role': 'user', 'content': 'How are you doing?' }
        await x.chat(model='llama3.2', messages=[m0])
        yield h, True
      except:
        yield h, False

  async def employ(self, host:str, ollamadir:str):

    h = host
    q = f"nohup {ollamadir}/bin/ollama serve > {ollamadir}/log/{h.split('/')[-1]}.log 2>&1 &"

    subprocess.Popen(q, shell=True, env={ "OLLAMA_HOST": h,
                                          "CUDA_VISIBLE_DEVICES": "0",
                                          "HOME": os.path.expanduser("~") })
    time.sleep(5)

  def delegate(self):
    self.que = self.que[1:] + [self.que[0]]
    return self.models[self.que[0]]

  async def parallel(self, idxgen):

    U = asyncio.Semaphore(4*len(self.models))
    async def f0(i, H, D):
      async with U:
        A = Dialog(self.delegate(), H, D)
        V = await A.ask()
        for v in V : v['index'] = i
      return V

    Q = [f0(i, H, D) for i, H, D in idxgen]
    P = progress_async(asyncio.as_completed(Q), desc="💬", total=len(Q))
    Y = [await f async for f in P]

    return Y