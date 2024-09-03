using CSV, DataFrames, ProgressMeter, LinearAlgebra, Statistics, Random
for pth in ["../paths.jl", "edges.jl", "textual.jl"] include(pth) end
import .Edges, .Paths, .Textual
import Flux as Neural; using CUDA, BSON



df = vcat([CSV.read(p, DataFrame; types=Dict(
"file" => String, 
"text" => String, 
"page" => Int,
"x0" => Int, 
"x1" => Int, 
"x2" => Int, 
"x3" => Int,
"y0" => Int, 
"y1" => Int, 
"y2" => Int, 
"y3" => Int,
"px0" => Float32, 
"px1" => Float32, 
"px2" => Float32, 
"px3" => Float32, 
"py0" => Float32, 
"py1" => Float32, 
"py2" => Float32, 
"py3" => Float32, 
"group" => Int
)) for p in Paths.Recog.list(labeled=true)]...)

df.id = 1:nrow(df)
df.unit = zeros(Int, nrow(df))
umap = Dict{Tuple{String, Int}, Int}()
for row in eachrow(df)
       unit = (row.file, row.page)
  umap[unit] = get(umap, unit, length(umap) + 1)
  row[:unit] = umap[unit]
end#progress

txtfeats = [:codes, :years, :sentences, :length, :width, :dockeyword]
sented = Textual.sententify.( Textual.wordchain.(df.text) )
df.codes .= [count(Textual.codealike, S) for S in sented]
df.years .= [count(Textual.yearalike, S) for S in sented]
df.sentences = length.(sented) .- df.codes .- df.years
df.length = length.(df.text)
df.width = df.px2 .- df.px0
df.dockeyword .= [maximum(s -> Textual.dockeyword(s, 0.1), S) for S in sented]

tblfeats = [:updoc, :dwdoc]
df.updoc = zeros(Float32, nrow(df))
df.dwdoc = zeros(Float32, nrow(df))
for unit in groupby(df, :unit)
docs = filter(row -> row.dockeyword > 0, unit)
if isempty(docs) continue end
for quad in eachrow(unit)
if quad.id in docs.id continue end
updiff = filter(x -> x > 0, quad.py0 .- docs.py3)
if !isempty(updiff) quad.updoc = minimum(updiff) end
dwdiff = filter(x -> x > 0, docs.py0 .- quad.py3)
if !isempty(dwdiff) quad.dwdoc = minimum(dwdiff) end
end
end

posfeats = [:px0, :px1, :px2, :px3, :py0, :py1, :py2, :py3]

split = round(Int, 0.8 * (unique(df[:, :unit]) |> length))
df.test = zeros(Bool, nrow(df))
for (i, unit) in enumerate(groupby(df, :unit))
unit[!, :test] .= i > split
end

tr = filter(row -> !row.test, df)
ts = filter(row -> row.test, df)



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



Mtr, ytr = Matrix(tr[:, [posfeats..., tblfeats..., txtfeats...]]), tr.group .!= 1
Mtr = Mtr' |> Neural.gpu
ytr = ytr' |> Neural.gpu
Mts, yts = Matrix(ts[:, [posfeats..., tblfeats..., txtfeats...]]), ts.group .!= 1
Mts = Mts' |> Neural.gpu
yts = yts' |> Neural.gpu

fmodel = Neural.Chain( Neural.Dense(size(Mtr, 1), 64, Neural.relu),
                       Neural.Dense(64, 32, Neural.relu),
                       Neural.Dense(32, 1, Neural.sigmoid)) |> Neural.gpu

train!(fmodel, Mtr, ytr, Mts, yts; epochs = 100, opti = Neural.ADAM(0.001))



Mtr, Etr = Edges.mx(tr[:, [:unit, :group, posfeats..., txtfeats...]])
Mtr = replace(Mtr, Inf32 => 2.0f0)
Mtr, ytr = Mtr' |> Neural.gpu, reshape(Etr[:,:group], 1, :) |> Neural.gpu
Gtr = Edges.group(Etr)

Mts, Ets = Edges.mx(ts[:, [:unit, :group, posfeats..., txtfeats...]])
Mts = replace(Mts, Inf32 => 2.0f0)
Mts, yts = Mts' |> Neural.gpu, reshape(Ets[:,:group], 1, :) |> Neural.gpu
Gts = Edges.group(Ets)

gmodel = Neural.Chain( Neural.Dense(size(Mtr, 1), 64, Neural.relu),
                      Neural.Dense(64, 32, Neural.relu),
                      Neural.Dense(32, 1, Neural.sigmoid)) |> Neural.gpu

train!(gmodel, Mtr, ytr, Gtr, Etr, Mts, yts, Gts, Ets; epochs=40)



@BSON.save "fmodel.bson" fmodel
@BSON.save "gmodel.bson" gmodel