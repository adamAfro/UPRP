import re

class Month:
  class PL:
    m = [["styczeń", "stycznia", "stycz", "sty"],
        ["luty", "lutego", "lut"],
        ["marzec", "marca", "marz", "mar"],
        ["kwiecień", "kwietnia", "kwiec", "kwi", "kw"],
        ["maj", "maja"],
        ["czerwiec", "czerwca", "czerw"],
        ["lipiec", "lipca", "lip"],
        ["sierpień", "sierpnia", "sierp"],
        ["wrzesień", "września", "wrześ"],
        ["październik", "października", "paźdz"],
        ["listopad", "listopada", "list"],
        ["grudzień", "grud"]]
    q = [x for l in m for x in l]
    
  class EN:
    m = [["jan", "january"], 
        ["feb", "february"], 
        ["mar", "march"], 
        ["apr", "april"], 
        ["may"], 
        ["jun", "june"], 
        ["jul", "july"], 
        ["aug", "august"], 
        ["sep", "september"], 
        ["oct", "october"], 
        ["nov", "november"], 
        ["dec", "december"]]
    q = [x for l in m for x in l]
  
Q = dict( alnum = r'[\w\D]{1,8}\s*(\d{2,}\s*)+',
          digital = r'(?<!\D)(\d{2,}\D{0,3})+',
          digits5 = r'\b\d{5,}\b',
          digits5su = r'\b\d{5,}\s*\w\s*\d?\b',
          
          patent5 = r'[\w\D]{1,8}\s*(\d{5,}\s*)+',
          PL56 = r'((((patent|p)[\srplu\W]{1,8})|(numer|nr)\D{1,5})\s*\d?\d{2}\s*\d{3})',

          datestr = re.compile('|'.join(["(\d{2}|\d{4})\s+(" + "|".join(Month.PL.q) + ")\s+(\d{2}|\d{4})",
                                         "(\d{2}|\d{4})\s+(" + "|".join(Month.EN.q) + ")\s+(\d{2}|\d{4})"])),
          datenum = re.compile('|'.join(["\d{4}\s+\d{1,2}\s+\d{1,2}",
                                         "\d{1,2}\s+\d{1,2}\s+\d{4}",
                                         "\d{1,2}\s+\d{1,2}\s+\d{2}",
                                         "\d{2}\s+\d{1,2}\s+\d{1,2}"])) )

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

    X = [w for w in re.split(by, x) if len(w) > 0]
    n = nwords*2-1
    if len(X) <= n: return [x]

    start, end = '', ''
    if re.fullmatch(by, X[ 0]): start, X = X[0], X[1:]
    if re.fullmatch(by, X[-1]): end, X = X[-1], X[:-1]

    N = [X[max(0, i-1):i+n] for i in range(0, len(X) - n + 1, 2)]
          # ^ greedy dla przerywaczy
    if len(N[-1]) < n-1: N = N[:-1]

    Y = [''.join(n) for n in N]
    Y[0] = start + Y[0]
    Y[-1] = Y[-1] + end

    return Y
  
  def extract(a:str, target:str, b=None):
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