module Edges
using DataFrames, ProgressMeter, LinearAlgebra
for pth in ["../paths.jl", "spatial.jl", "textual.jl"] include(pth) end
import .Spatial



"Tworzy siatkę odległości i dodaje informacje do ramki df"
function net!(df::DataFrame, id=1)::DataFrame
edges = DataFrame(
from=Int[], 
to=Int[], 
group=Bool[], 
offset=Float32[], 
shift=Float32[])
@showprogress "📐" for unit in groupby(df, :unit)
unit.id = range(id, length=nrow(unit))
quads = quadvec(unit)
# ponowne zliczanie środków czworokątów jest szybsze niż użycie
# gotowych środków z funkcją collect(zip(...))
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
                hasproperty(unit, :group) && 
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
function mx(df::DataFrame)::Tuple{Matrix{Float32}, DataFrame}
df = copy(df)
edges = net!(df)
df.id = 1:nrow(df)
dircols!(df, edges)
edgedircols!(edges, df)
exclude = [:from, :to]
if hasproperty(df, :group) push!(exclude, :group) end
M = Matrix{Float32}(select(edges, Not(exclude)))
return M, edges
end



"Tworzy wektor grup - każda to (pod)wektor z wartościami id (jako `Int`), pomija puste grupy"
function group(from::Vector{Int}, to::Vector{Int})::Vector{Vector{Int}}
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

function group(edges::DataFrame)::Vector{Vector{Int}}
E = filter(row -> row.group == true, edges)
return group(E.from, E.to)
end



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



end