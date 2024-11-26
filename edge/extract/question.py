import sys, os, asyncio
from pandas import DataFrame, read_csv, concat

DIR = os.path.dirname(__file__)
ROOT = os.path.abspath(os.path.join(DIR, '..', '..'))
sys.path.append(ROOT) # dodanie lib
os.chdir(DIR) # zmiana katalogu dla procesów

from lib.log import log, notify
from lib.chat import Crowd

H = {

  "To which category, does the reference references?": {

    "patent": {
      "What number does the patent have?":
        "alphanumerical string, ex: AA00123456789",
      "What country is it from?": "string",
      "Which organisations can you spot?": "string",
      "What authors are listed in?": "string",
      "What is the date of publication?": "date",
      "Are there any cities mentioned? List them if so": "string",
    },

    "scientific": {
      "What title does it have?":
        "string with title of the publication",
      "Which organisations can you spot?": "string",
      "What authors are listed in?": "string",
      "What is the date of publication?": "date",
      "Are there any cities mentioned? List them if so": "string",
    },

    "website": None,
    "book": None,
  }
}

s0 = "Your task is to answer questions about" + \
     "this particular citation reference. " + \
     "Provide your response in JSON format" + \
     "expected by user. Be specific and exact. " + \
     'Always return your response in key named "value"' + \
     '\nHere is the reference:\n<here should be ref.>\n'
def s(x:str): return [{ 'role': 'system', 
                        'content': s0.replace("<here should be ref.>", x) }]

async def LLMapply():
  try:
    log("✨"); notify("✨")
    C = Crowd([f'http://localhost:{h}' for h in range(11434, 11434+12)])
    async for h, v in C.briefing():
      if not v: await C.employ(h, ollamadir=ROOT+'/ollama'); log("🪄", h)
      log("🦙", h)

    X = read_csv('../docs.csv').reset_index()\
      .rename(columns={'docs':'text', 'index':'docs'})[['docs', 'text']]
    log("📂")

    p, u = len(X)//100, [0,1,2,5,10,20,30,40,50,60,70,80,90]
    B = [range(p*u[i], p*u[i+1]) for i in range(len(u)-1)] + [range(p*u[-1], len(X))]
    Y0 = None
    for I in B:
      V = await C.parallel((i, H, s(x)) for i, x in X.iloc[I].itertuples(index=False))
      Y = DataFrame([y for Y in V for y in Y])\
         .pivot(index='index', columns='question', values='value')

      if Y0 is None: Y0 = Y
      else: Y0 = concat([Y0, Y])
      Y.to_csv('answers.csv', index=False)
      log("💾", 'answers.csv')

    notify("✅")

  except Exception as e: log("❌", e); notify("❌")

asyncio.run(LLMapply())