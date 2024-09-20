import os, pandas, feat, sklearn.preprocessing as prep, joblib, tensorflow.keras.models as models
os.chdir("/storage/home/ajakubczak/Dokumenty/UPRP/raport")

df = pandas.read_csv('labels/labeled.csv')
scl = joblib.load('scaler.joblib')
X = scl.transform(feat.feat(df))

pred = models.load_model('extractor.keras').predict(X)
df = df[pred > 0.5]