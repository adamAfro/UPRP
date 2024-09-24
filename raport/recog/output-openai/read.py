def CSV(f):
  try:
    import io
    from pandas import read_csv
    with open(f, mode='r', encoding='utf-8') as f: t = f.read()
    t = t[len("```csv\n"):-len("\n```")]
    s = io.StringIO(t)
    return read_csv(s, quotechar='"', skipinitialspace=True)
  except: return "CSV error"