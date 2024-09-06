import Flux as Neural; using CUDA, BSON, ProgressMeter, Statistics, DataFrames
for pth in ["data.jl", "edges.jl"] include(pth) end
import .Data, .Edges



"Dokładność"
function acc(y, pred)::Float32
pred = round.(Int, pred)
return mean(pred .== y)
end



"Trenowanie sieci"
function train!(model, Mtr, ytr, Mvl, yvl; epochs = 100, opti = Neural.ADAM(0.001))
loss(x, y) = Neural.binarycrossentropy(model(x), y)
Dtr = Iterators.repeated((Mtr, ytr), epochs)
progress = Progress(epochs, 1, "🧠")
for epoch in 1:epochs
for (x, y) in Dtr Neural.train!(loss, Neural.params(model), [(x, y)], opti) end
test = loss(Mvl, yvl)
train = loss(Mtr, ytr)
accuracy = acc(yvl, model(Mvl))
next!(progress; showvalues = [
(:accuracy, accuracy),
(:test, test),
(:train, train),
(:overfit, test - train)])
end#for epoch
end#train



"""Funkcja szukająca dobranej grupy i oceniająca"""
function gcount(Gpred::Vector{Vector{Int}}, G::Vector{Vector{Int}})
stats = DataFrame(n=fill(Int(0), length(G)), max=map(length, G), groups=fill(Int(0), length(G)))
sort!(Gpred, by=length, rev=true)
sort!(G,     by=length, rev=true)
gmth, pmth = [], []
@showprogress 1 "💏" for (gid, g) in enumerate(G)
bestp, bestn = 0, 0
for (pid, p) in enumerate(Gpred)
  n = length(intersect(p, g))
  if n > bestn 
    bestp = pid
    bestn = n 
    end
  end#for
if bestn > 0
  push!(gmth, gid)
  push!(pmth, bestp)
  stats[gid, :n] = bestn
  stats[gid, :max] = length(g)
  for (ogid, other) in enumerate(G)
    stats[gid, :groups] += length(intersect(g, other)) > 0
    end#for
  end
end#for g

added = DataFrame(n=Int[], groups=Int[])
for pid in setdiff(1:length(Gpred), pmth)
  groups = 0
  for g in G
    groups += length(intersect(Gpred[pid], g)) > 0
    end#for g
  push!(added, (length(Gpred[pid]), groups))
  end#for p

omitted = DataFrame(gid=Int[], n=Int[], groups=Int[])
for gid in setdiff(1:length(G), gmth)
  groups = 0
  for p in Gpred
    groups += length(intersect(p, G[gid])) > 0
    end#for p
  push!(omitted, (gid, length(G[gid]), groups))
  end#for g
return stats, added, omitted
end#gstat



"Trenowanie sieci z uwzględnieniem jakości grupowania"
function train!(model, Mtr, ytr, Gtr, Etr, Mts, yts, Gts, Ets; epochs::Int, opti=Neural.ADAM(0.001))
loss(x, y) = Neural.binarycrossentropy(model(x), y)
Etr, Ets = copy(Etr), copy(Ets)
Dtr = Iterators.repeated((Mtr, ytr), epochs)
progress = Progress(epochs, 1, "🧠")
for epoch in 1:epochs
for (x, y) in Dtr Neural.train!(loss, Neural.params(model), [(x, y)], opti) end
test = loss(Mts, yts)
train = loss(Mtr, ytr)
predmx = model(Mts)
accuracy = acc(yts, predmx)
Ets.group = [collect(row) for row in eachrow(round.(Int, predmx))][1]
Gpred = Edges.group(Ets)
expected, added, omitted = gcount(Gpred, Gts)
next!(progress; showvalues = [
(:accuracy, accuracy),
(:test, test),
(:train, train),
(:overfit, test - train),
(:added, nrow(added)),
(:omitted, nrow(omitted)),
(:fullfillment, round(mean(expected.n ./ expected.max); digits=3))])
end#for epoch
end#train



tr, ts = Data.load(train=true)

Mtr, ytr = Matrix(tr[:, [Data.colnames[:pos]..., Data.colnames[:tbl]..., Data.colnames[:txt]...]]), tr.group .!= 1
Mtr = Mtr' |> Neural.gpu
ytr = ytr' |> Neural.gpu
Mts, yts = Matrix(ts[:, [Data.colnames[:pos]..., Data.colnames[:tbl]..., Data.colnames[:txt]...]]), ts.group .!= 1
Mts = Mts' |> Neural.gpu
yts = yts' |> Neural.gpu

fmodel = Neural.Chain( Neural.Dense(size(Mtr, 1), 64, Neural.relu),
                       Neural.Dense(64, 32, Neural.relu),
                       Neural.Dense(32, 1, Neural.sigmoid)) |> Neural.gpu

train!(fmodel, Mtr, ytr, Mts, yts; epochs = 100, opti = Neural.ADAM(0.001))



tr, ts = Data.load(train=true, grouped=true)
Mtr, Etr = Edges.mx(tr[:, [:unit, :group, Data.colnames[:pts]..., Data.colnames[:txt]...]])
Mtr = replace(Mtr, Inf32 => 2.0f0)
Mtr, ytr = Mtr' |> Neural.gpu, reshape(Etr[:,:group], 1, :) |> Neural.gpu
Gtr = Edges.group(Etr)

Mts, Ets = Edges.mx(ts[:, [:unit, :group, Data.colnames[:pts]..., Data.colnames[:txt]...]])
Mts = replace(Mts, Inf32 => 2.0f0)
Mts, yts = Mts' |> Neural.gpu, reshape(Ets[:,:group], 1, :) |> Neural.gpu
Gts = Edges.group(Ets)

gmodel = Neural.Chain( Neural.Dense(size(Mtr, 1), 64, Neural.relu),
                      Neural.Dense(64, 32, Neural.relu),
                      Neural.Dense(32, 1, Neural.sigmoid)) |> Neural.gpu

train!(gmodel, Mtr, ytr, Mts, yts; epochs=90)
train!(gmodel, Mtr, ytr, Gtr, Etr, Mts, yts, Gts, Ets; epochs=10)



@BSON.save "fmodel.bson" fmodel
@BSON.save "gmodel.bson" gmodel