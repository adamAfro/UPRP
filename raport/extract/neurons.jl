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
# Budowa sieci
# ref: https://github.com/CarloLucibello/GraphNeuralNetworks.jl/blob/master/examples/link_prediction_pubmed.jl
import Flux as Neural; using GraphNeuralNetworks; using CUDA

device = Neural.cpu # TODO fix4gpu

struct DotPredictor end
function (::DotPredictor)(g, x)
z = apply_edges((xi, xj, e) -> sum(xi .* xj, dims = 1), g, xi = x, xj = x)
# z = apply_edges(xi_dot_xj, g, xi=x, xj=x) # Same with built-in method
return vec(z)
end

G = GNNGraph(W; ndata=(;features=hcat(df.x, df.y,
                                      df.width, 
                                      df.length)')) |> device
η = 0.001
nvar = size(G.ndata.features, 1)

function loss(model, pos, neg=nothing; measure=false)
h = model(G.ndata.features)
if neg === nothing neg = negative_sample(pos) end
pscore, nscore = pred(pos, h), pred(neg, h)
scores = [pscore; nscore]
labels = [fill!(similar(pscore), 1); fill!(similar(nscore), 0)]
logit = Neural.logitbinarycrossentropy(scores, labels)
if measure
  acc = 0.5 * mean(pscore .>= 0) + 0.5 * mean(nscore .< 0)
  return logit, acc
else return logit end
end

train, test = rand_edge_split(G, 0.9)
testneg = negative_sample(G, num_neg_edges=test.num_edges)
chain = GNNChain( GCNConv(nvar => 128, Neural.relu),
                  GCNConv(128 => 128, Neural.relu),
                  GCNConv(128 => 128, Neural.relu),
                  Neural.Dense(128, 128),
                  Neural.Dense(128, 1) )
model = WithGraph(chain, train) |> device
opti = Neural.setup(Neural.Adam(η), model)
pred = DotPredictor()

losses = reports = DataFrame(epoch = Int[],
                             train = Float64[],
                             test = Float64[],
                             acc = Float64[])
repeats = 100
progress = Progress(repeats, 1, "🧠")
for epoch in 1:repeats
grads = Neural.gradient(model -> loss(model, train), model)
Neural.update!(opti, model, grads[1])
push!(losses, (epoch,
              loss(model, train, measure=true)[1],
              loss(model, test, testneg, measure=true)...))
gain = missing
if nrow(losses) > 1 
  gain = losses[end - 1, :test] - losses[end, :test]
  end
next!(progress; showvalues = [(:accuracy,  losses.acc[end]),
                              (:test, losses.test[end]),
                              (:train, losses.train[end]),
                              (:overfit, losses.test[end] 
                                        -losses.train[end]),
                              (:gain, gain)])
                              end