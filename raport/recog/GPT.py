import os; os.chdir("/home/adam/Projekty/UPRP/raport/recog")
key = "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"

import openai as AI, base64, tqdm

client = AI.OpenAI(api_key=key)
setup = dict(model="gpt-4o-mini", temperature=0.0)

class GPT:

  pricing_in = 0.15/1_000_000#pertoken
  pricing_out = 0.6/1_000_000#pertoken
  prm = (
    "Wyciągnij cały tekst i oznacz w nim tagi " +
    "zgodnie z podaną w specyfikacją: " +
    "...tekst<Odpowiedni tag>kod/tekst spełniający specyfikację</Odpowiedni tag>tekst...")
  prmmsg = dict(type="text", text=prm)
  with open("GPT.tags.json", "r", encoding="utf-8") as file:
    tags = file.read()
  tagmsg = dict(type="text", text=tags)
  
  def __init__(self, outdir:str=None):
    self.priced = 0.00
    self.outdir = outdir

  def recog(enc:str):
    
    imgtxt = f"data:image/jpeg;base64,{enc}"
    imgmsg = dict(type="image_url", image_url=dict(url=imgtxt))
    msg = dict(role="user", content=[GPT.prmmsg, GPT.tagmsg, imgmsg])
    return client.chat.completions.create(**setup, messages=[msg])

  def frecog(path:str):

    with open(path, "rb") as f: img = f.read()
    enc = base64.b64encode(img).decode('utf-8')
    return GPT.recog(enc)

  def dirrecog(self, path:str):
    
    progress = tqdm.tqdm(enumerate(os.listdir(path)))
    for (i, file) in progress:
      res = GPT.frecog(os.path.join(path, file))
      self.priced += GPT.pricing_in*res.usage.prompt_tokens
      self.priced += GPT.pricing_out*res.usage.completion_tokens
      mean = self.priced/(i+1)
      expected = mean * (len(os.listdir(path))-i-1)
      progress.set_postfix(priced=self.priced, mean=mean, expected=expected)
      output = res.choices[0].message.content
      if output.startswith("```xml"): output = output[6:]
      if output.startswith("```"): output = output[3:]
      if output.endswith("```"): output = output[:-3]
      output = "<P>" + output + "</P>"
      if self.outdir:
        with open(f"{self.outdir}/{file}.XML", "w") as f:
          f.write(output)

recog = GPT(outdir="output-GPT")
recog.dirrecog("../img/output")