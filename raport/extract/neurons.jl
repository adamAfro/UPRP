using CSV, DataFrames, ProgressMeter, LinearAlgebra, Statistics
for pth in ["paths.jl", "spatial.jl"] include(pth) end
import .Spatial, .Paths
#
#
#
#
#
#
#
#
#
#
#
# Wczytywanie danych
df = vcat([CSV.read(path, DataFrame; types=Dict("file" => String,
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
                                                "type" => String, 
                                                "group" => Int    )) for path in Paths.Recog.list(labeled=true)]...)
                                                filter!(row -> row.type != "text", df)
df.unit = zeros(Int, nrow(df))
umap = Dict{Tuple{String, Int}, Int}()
@showprogress "📃" for row in eachrow(df)
       unit = (row.file, row.page)
  umap[unit] = get(umap, unit, length(umap) + 1)
  row[:unit] = umap[unit]
end#progress
#
#
#
#
#
#
#
#
#
#
# Przygotowanie grafu
function quadvec(unit::Union{DataFrame, SubDataFrame})::Vector{Spatial.Quadrangle}
return[((row.px0, row.py0), 
        (row.px1, row.py1), 
        (row.px2, row.py2), 
        (row.px3, row.py3)) for row in eachrow(unit)]
        end

function midvec(unit::Union{DataFrame, SubDataFrame})::Vector{Spatial.Point}
return[( (row.px0 + row.px1 + row.px2 + row.px3) / 4,
         (row.py0 + row.py1 + row.py2 + row.py3) / 4 ) for row in eachrow(unit)]
         end

edges = DataFrame(from=Int[], to=Int[], 
                  group=Bool[], offset=Float32[], shift=Float32[])

W = zeros(Float32, nrow(df), nrow(df))

id = 1
@showprogress "📐" for unit in groupby(df, :unit)
unit.id = range(id, length=nrow(unit))
links = Matrix( zeros(Bool, nrow(unit), nrow(unit)) )
for (iA, A) in enumerate(eachrow(unit)) 
for (iB, B) in enumerate(eachrow(unit)) 
if iA == iB || A.group != B.group continue end
links[iA, iB] = links[iB, iA] = 1
end end
quads = quadvec(unit)
middles = midvec(unit)
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
linked = findall(links .&& adjacency)
cartesian = CartesianIndex.(linked) .+ CartesianIndex(id-1, id-1)
W[cartesian] .= 1 # ./ sqrt.(offsets[linked] .^ 2 .+ shifts[linked] .^ 2)
id += nrow(unit)
unit.x = [x for (x,y) in middles]
unit.y = [y for (x,y) in middles]
end#progress

df.width = df.px0 .- df.px2
df.length = length.(df.text)
