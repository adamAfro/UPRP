import unittest, cProfile, pandas, re

class Placeholder(unittest.TestCase):

  def test_notimplemented(self):
    self.assertTrue(False)

class AssertionOnlyTestResult(unittest.TextTestResult):
  def addError(self, test, err):
    exc_type, exc_value, exc_traceback = err
    if exc_type is AssertionError:
      self.failures.append((test, self._exc_info_to_string(err, test)))
    else:
      try: raise err
      except: raise Exception(exc_type, exc_value).with_traceback(exc_traceback)

  def addSubTest(self, test, subtest, err):
    if err is not None:
      exc_type, exc_value, exc_traceback = err
      if exc_type is AssertionError:
        self.failures.append((subtest, self._exc_info_to_string(err, test)))
      else:
        try: raise err
        except: raise Exception(exc_type, exc_value).with_traceback(exc_traceback)

class AssertionOnlyTestRunner(unittest.TextTestRunner):
  def _makeResult(self):
    return AssertionOnlyTestResult(self.stream, self.descriptions, self.verbosity)

T = unittest.TestSuite()
T.addTest(Placeholder('test_notimplemented'))

with cProfile.Profile() as pr:
  AssertionOnlyTestRunner().run(T)

X = pandas.apiDataFrame(pr.getstats(),
  columns=['func', 'ncalls', 'ccalls', 'tottime', 'cumtime', 'callers'])

X['file'] = X['func'].apply(lambda x: re.search(r'file "([^"]+)"', str(x)))
X['file'] = X['file'].apply(lambda x: x.group(1) if x else None)

X['line'] = X['func'].apply(lambda x: re.search(r'line (\d+)', str(x)))
X['line'] = X['line'].apply(lambda x: x.group(1) if x else None)

X['code'] = X['func'].apply(lambda x: re.search(r'<code object \<?(\w+)\>?', str(x)))
X['code'] = X['code'].apply(lambda x: x.group(1) if x else None)

X['percall'] = X['tottime'] / X['ncalls']

K = ['percall', 'local', 'line', 'code', 'tottime']
X = X[K+ [k for k in X.columns if k not in K]]
X = X.sort_values('tottime', ascending=False)
X.head(24)