using CSV, DataFrames, ProgressMeter, LinearAlgebra, Statistics, Random
for pth in ["../paths.jl", "spatial.jl", "textual.jl"] include(pth) end
import .Spatial, .Paths, .Textual
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



"Zwraca czworokąty z ramki"
function quadvec(unit::Union{DataFrame, SubDataFrame})::Vector{Spatial.Quadrangle}
return[((row.px0, row.py0), 
        (row.px1, row.py1), 
        (row.px2, row.py2), 
        (row.px3, row.py3)) for row in eachrow(unit)]
        end



"Zwraca środki czworokątów z ramki"
function midvec(unit::Union{DataFrame, SubDataFrame})::Vector{Spatial.Point}
return[( (row.px0 + row.px1 + row.px2 + row.px3) / 4,
         (row.py0 + row.py1 + row.py2 + row.py3) / 4 ) for row in eachrow(unit)]
         end



"Tworzy siatkę odległości i dodaje informacje do ramki df"
function distnet!(df::DataFrame, id=1)::DataFrame
edges = DataFrame(
from=Int[], 
to=Int[], 
group=Bool[], 
offset=Float32[], 
shift=Float32[])
@showprogress "📐" for unit in groupby(df, :unit)
unit.id = range(id, length=nrow(unit))
quads = quadvec(unit)
middles = midvec(unit)
unit.x = [m[1] for m in middles]
unit.y = [m[2] for m in middles]
adjacency = Spatial.mxpassage(quads, middles)
passages = findall(adjacency |> triu)
offsets = Spatial.mxoffset([Q[1][1] for Q in quads], [Q[3][1] for Q in quads], passages)
shifts = Spatial.mxshift(middles, passages)
for idx in findall(isfinite, offsets)
  i, j = Tuple(idx)
  of, sf = offsets[i, j], shifts[i, j]
  if of == 0 && sf == 0 continue end 
  push!(edges, (unit[i, :id], unit[j, :id], 
                unit[i, :group] == unit[j, :group], of, sf))
                end
id += nrow(unit)
end#progress
return edges
end



"Kierunki, w kolejności hierarchicznej"
dirs = [:top, :lft, :btm, :rgt]



"Oblicza stratę dla kierunku - do wyboru który box na który kier."
function dirloss(dir::Symbol, offset::Float32, shift::Float32)::Float32
@assert dir in dirs
if dir == :top return shift > 0 ? Inf32 : abs(offset) end
if dir == :btm return shift < 0 ? Inf32 : abs(offset) end
if dir == :lft return shift > 0 ? Inf32 : abs(shift) end
if dir == :rgt return shift < 0 ? Inf32 : abs(shift) end
end



"Dodaje zerowe/∞ kolumny cech dla każdego kierunku"
function dircols!(df::DataFrame)::Nothing
for dir in ["top", "lft", "btm", "rgt"]
for var in ["x", "y"]
  df[!, Symbol("$(dir)$(var)")] = fill(Inf32, nrow(df))
  end
for var in ["width", "length", "codes", "years", "sentences"]
  df[!, Symbol("$(dir)$(var)")] = zeros(Float32, nrow(df))
  end
end
end#fcolumns



"Ustala wartość cech dla każdego kierunku"
function dircols!(df::DataFrame, edges::DataFrame)::Nothing
dircols!(df)
@showprogress "📏" for row in eachrow(df)
links = edges[edges.from .== row.id, :]
if isempty(links) continue end
lossmx = Array{Float32}(undef, nrow(links), length(dirs))
for (i, edge) in enumerate(eachrow(links)) 
  for (j, dir) in enumerate(dirs)
    lossmx[i, j] = dirloss(dir, edge.offset, edge.shift)
  end
end
dirred = Dict{Symbol, Union{Missing, Int}}(s => missing for s in dirs)
for dir in dirs
  val, id = findmin(lossmx)
  if val == Inf32 continue end
  i, j = Tuple(CartesianIndices(lossmx)[id])
  dirred[dir] = links[i, :to]
  lossmx[i, :] .= Inf32
  lossmx[:, j] .= Inf32
end
for dir in dirs
  if dirred[dir] === missing continue end
  for var in ["x", "y", "width", "length", "codes", "years", "sentences"]
    df[row.id, Symbol("$(dir)$(var)")] = df[dirred[dir], Symbol(var)]
  end
end
end#progress
end



"Dodaje zerowe/∞ kolumny cech dla każdego kierunku"
function edgedircols!(edges::DataFrame)::Nothing
for ltype in ["from", "to"]
for dir in ["top", "lft", "btm", "rgt"]
for var in ["x", "y"]
  edges[!, Symbol("$(ltype)$(dir)$(var)")] = fill(Inf32, nrow(edges))
  end
for var in ["width", "length", "codes", "years", "sentences"]
  edges[!, Symbol("$(ltype)$(dir)$(var)")] = fill(Inf32, nrow(edges))
  end
end
end
end



"Ustala wartość cech dla każdego kierunku"
function edgedircols!(edges::DataFrame, df::DataFrame)::Nothing
edgedircols!(edges)
@showprogress "🔀" for row in eachrow(edges)
from, to = df[row.from, :], df[row.to, :]
for dir in dirs
  for var in ["x", "y", "width", "length", "codes", "years", "sentences"]
    row[Symbol("from$(dir)$(var)")] = from[Symbol("$(dir)$(var)")]
    row[Symbol("to$(dir)$(var)")] = to[Symbol("$(dir)$(var)")]
  end
end
end#progress
end



"Tworzy krawędzie i macierz do sieci"
function edgemx(df::DataFrame)::Tuple{Matrix{Float32}, DataFrame}
df = copy(df)
edges = distnet!(df)
df.id = 1:nrow(df)
dircols!(df, edges)
edgedircols!(edges, df)
M = Matrix{Float32}(select(edges, Not([:from, :to])))
return M, edges
end



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



"Tworzy wektor grup - każda to (pod)wektor z wartościami id (jako `Int`), pomija puste grupy"
function edgegroup(from::Vector{Int}, to::Vector{Int})::Vector{Vector{Int}}
@assert length(from) == length(to)
groups, ptrs = Vector{Set{Int}}(), Dict{Int, Int}()
for i in 1:length(from)
if haskey(ptrs, from[i]) && haskey(ptrs, to[i])
  keep, move = ptrs[from[i]], ptrs[to[i]]
  if keep == move continue end
  for id in groups[move]
    push!(groups[keep], id)
    ptrs[id] = keep
  end
  empty!(groups[move])
elseif haskey(ptrs, from[i])
  keep = ptrs[from[i]]
  push!(groups[keep], to[i])
  ptrs[to[i]] = keep
elseif haskey(ptrs, to[i])
  keep = ptrs[to[i]]
  push!(groups[keep], from[i])
  ptrs[from[i]] = keep
else
  push!(groups, Set([from[i], to[i]]))
  ptrs[from[i]] = length(groups)
  ptrs[to[i]] = length(groups)
end
end
groups = [sort(Vector([x for x in G])) for G in groups if !isempty(G)]
return sort(groups, by=g -> g[1])
end

function edgegroup(edges::DataFrame)::Vector{Vector{Int}}
E = filter(row -> row.group == true, edges)
return edgegroup(E.from, E.to)
end



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
Gpred = edgegroup(Ets)
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



Mtr, Etr = edgemx(tr[:, [:unit, :group, posfeats..., txtfeats...]])
Mtr = replace(Mtr, Inf32 => 2.0f0)
Mtr, ytr = Mtr' |> Neural.gpu, reshape(Etr[:,:group], 1, :) |> Neural.gpu
Gtr = edgegroup(Etr)

Mts, Ets = edgemx(ts[:, [:unit, :group, posfeats..., txtfeats...]])
Mts = replace(Mts, Inf32 => 2.0f0)
Mts, yts = Mts' |> Neural.gpu, reshape(Ets[:,:group], 1, :) |> Neural.gpu
Gts = edgegroup(Ets)

gmodel = Neural.Chain( Neural.Dense(size(Mtr, 1), 64, Neural.relu),
                      Neural.Dense(64, 32, Neural.relu),
                      Neural.Dense(32, 1, Neural.sigmoid)) |> Neural.gpu

train!(gmodel, Mtr, ytr, Gtr, Etr, Mts, yts, Gts, Ets; epochs = 100)



@BSON.save "fmodel.bson" fmodel
@BSON.save "gmodel.bson" gmodel