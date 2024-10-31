month = dict( PL=[["styczeń", "stycznia", "stycz", "sty"],
                  ["luty", "lutego", "lut"],
                  ["marzec", "marca", "marz", "mar"],
                  ["kwiecień", "kwietnia", "kwiec", "kwi", "kw"],
                  ["maj", "maja"],
                  ["czerwiec", "czerwca", "czerw", "czer", "cze"],
                  ["lipiec", "lipca", "lip"],
                  ["sierpień", "sierpnia", "sierp", "sier", "sie"],
                  ["wrzesień", "września", "wrześ", "wrz"],
                  ["październik", "października", "paźdz", "paź"],
                  ["listopad", "listopada", "list", "lis"],
                  ["grudzień", "grudnia", "grud", "gru"]],
              EN=[["january", "jan"],
                  ["february", "feb"],
                  ["march", "mar"],
                  ["april", "apr"],
                  ["may"],
                  ["june", "jun"],
                  ["july", "jul"],
                  ["august", "aug"],
                  ["september", "sep"],
                  ["october", "oct"],
                  ["november", "nov"],
                  ["december", "dec"]] )

def patregex( code = r'\b[a-z]{2}\.?\s*[a-z]{0,2}\.?',
              num = r'nume?r?|nr\.?',
              pat = r'(?<![a-z])(pate?n?t?|p)[\W]{0,2}',
              digits = r'(\d{2,}\s*)+',
              pre = r'(?<!(?<=\d)r|\d|\s)\s*',
              suf = r'(?!\s*\d)(?!\s*doi)(?!\s*https\s*doi)' ):

  alpha = rf"({num}|{pat}){{1,2}}"
  alpha = rf"({alpha})?{code}({alpha})?"
  return rf"{pre}{alpha}\s*{digits}{suf}"

datenum = [ r"(?<!\d|\s)\s*(20[012]\d|19\d{2})\s*(?!\s\d)"
            r"\d{4}\s+\d{1,2}\s+\d{1,2}",
            r"\d{1,2}\s+\d{1,2}\s+\d{4}",
            r"\d{1,2}\s+\d{1,2}\s+\d{2}",
            r"\d{2}\s+\d{1,2}\s+\d{1,2}" ]

Q = dict(
          pgnum = r'\b((?<!(?<=p)l|\s)\s*p{1,2}|s(tr)?)\.?\s*\d{1,5}\s+\d{1,5}\b',

          etal = r'\b(et\s*al\.?|i\s*in{1,2}i?\.?)\b',

          code = r"\b[a-z]{1,3}\.?(\s[a-z]{1,2}\.?)?\s*((\d{3,}\s*\d{2,})|(\d{2,}\s*){3,})\s*(\d{2,}\s*)?",
          pub = r"pub\.?\s*(\d\s*)+",

          nrp = patregex(code=None),
          nrp56 = patregex(code=None, digits = r'\d?\d{2}\s*\d{3}'),
          EP56 = patregex(r'ep\.?\s*[^\Wp]?\s*\w?',
                          digits = r'\d?\d{2}\s*\d{3}'),

          PL56 = patregex(r'pl\.?\s*[^\Wp]?\s*\w?',
                          digits = r'\d?\d{2}\s*\d{3}'),

          patsu = patregex(suf = r'\s*[a-z]\d?(?!\w\.)'),
          PL56su = patregex(r'pl\.?\s*[^\Wp]?\s*\w?', 
                            digits = r'\d?\d{2}\s*\d{3}', 
                            suf = r'\s*[a-z]\d?(?!\w\.)'),

          Lmonth = rf"\d+\s*({'|'.join([ m for M in month['PL'] + month['EN'] for m in M ])})\s*\d*",
          Rmonth = rf"\s*\d*({'|'.join([ m for M in month['PL'] + month['EN'] for m in M ])})\s*\d+",
          fullmonth = rf"({'|'.join([ M[0] for M in month['PL'] + month['EN'] ] + [M[1] for M in month['PL']])})",
          datenum = rf"{'|'.join(datenum)}"
                                                            )
class Sentence:

  def split(x:str,  open = ['[', '(', '{'],
                    close = [']', ')', '}', ',', ';', ':'],
                    shift = ['"']):
    i, y, Y = 0, '', []
    for i in range(len(x)):
      if x[i] in open:
        if len(y) > 0: Y.append(y)
        y = x[i]
      elif x[i] in close:
        Y.append(y + x[i])
        y = ''
      elif x[i] in shift:
        if (len(y) > 0) and (y[0] != x[i]): 
          Y.append(y); y = x[i]
        else: Y.append(y + x[i]); y = ''
      else: y += x[i]
    if len(y) > 0: Y.append(y)
    return Y

  def nsub(x:str, nwords:int, by = r'([\W\s]+)'):
    import re

    X = [w for w in re.split(by, x) if len(w) > 0]
    n = nwords*2-1
    if len(X) <= n: return [(x, 0.0)]

    start, end = '', ''
    if re.fullmatch(by, X[ 0]): start, X = X[0], X[1:]
    if re.fullmatch(by, X[-1]): end, X = X[-1], X[:-1]

    N = [X[max(0, i-1):i+n] for i in range(0, len(X) - n + 1, 2)]
          # ^ greedy dla przerywaczy
    if len(N[-1]) < n-1: N = N[:-1]

    Y = [''.join(n) for n in N]
    Y[0] = start + Y[0]
    Y[-1] = Y[-1] + end

    return [(y, i/len(Y)) for i, y in enumerate(Y)]
  
  def extract(a:str, target:str, b=None):
    import re
    F = list(re.finditer(target, a))
    I = [i for m in F for i in m.span()]
    Q = [i for i in I if any((i == m.start()) for m in F)]
    if len(I) == 0: 
      if b is None: return [(a, False)]
      else: return [(a, b, False)]
    if I[0] != 0: I = [0] + I
    if I[-1]+1 != len(a): I = I + [len(a)]
    Y = [a[I[i]:I[i+1]] for i in range(len(I)-1)]
    Y = [(Y[i], I[i] in Q) for i in range(len(Y))]
    if b is not None:
      Y = [(a[I[i]:I[i+1]], b[I[i]:I[i+1]], Y[i][1]) for i in range(len(I)-1)]
    return Y