using CSV, DataFrames, ProgressMeter, Random, Statistics
using Base.Filesystem: joinpath

for p in ["paths.jl", "feats.jl", "../labels/meta.jl"] 
  include("../../$(p)") 
  end; import .Paths, .Features, .DatasetMeta


    """Wczytuje dane: z plików CSV łączy w jedną, szeroką ramkę"""
  function dataload(NN::Int; trsplit::Rational=4//5)
df = vcat([CSV.read(x, DataFrame; types=DatasetMeta.type) 
           for x in Paths.Recog.list(labeled=true)]...)
filter!(row -> row.type != "text", df)
FE = Features.extract(df)
FE[!, :id] = 1:nrow(FE)
gFE = groupby(FE, :unit)
gFE = gFE[shuffle(1:size(gFE, 1))]

split = round(Int, trsplit*length(gFE))
idtr = 1; for unit in gFE
  unit[!, :id] = idtr:idtr+nrow(unit)-1
  idtr += nrow(unit)
  end#idtr
idvl = 1; for unit in gFE[split+1:end]
  unit[!, :id] = idvl:idvl+nrow(unit)-1
  idvl += nrow(unit)
  end#idvl


Uwides = Vector{DataFrame}()
Ulabs = Vector{Matrix}()
Uids = Vector{Matrix}()
@showprogress 1 "🪗 " for unit in gFE
  wide, labs, ids = Features.widen(unit, NN)
  push!(Uwides, wide) 
  push!(Ulabs, labs)
  push!(Uids, ids)
  end

Wtr = vcat(Uwides[1:split]...)
ytr = vcat(Ulabs[1:split]...)
Itr = vcat(Uids[1:split]...)
Gtr = Features.group(Wtr, [:unit])

Wvl = vcat(Uwides[split+1:end]...)
yvl = vcat(Ulabs[split+1:end]...)
Ivl = vcat(Uids[split+1:end]...)
Gvl = Features.group(Wvl, [:unit])

Mtr = Matrix{Float32}(select(Wtr, Not([:id, :group, :unit])))
Mvl = Matrix{Float32}(select(Wvl, Not([:id, :group, :unit])))
for col in 1:size(Mtr, 2)
  if all(v -> v == 0 || v == 1, Mtr[:, col]) m, s = 0, 1
  else m, s = mean(Mtr[:, col]), std(Mtr[:, col]) end
  if s == 0 s = 1 end
  Mtr[:, col] = (Mtr[:, col] .- m) ./ s
  Mvl[:, col] = (Mvl[:, col] .- m) ./ s
  end#for col

return Mtr, ytr, Itr, Gtr, Mvl, yvl, Ivl, Gvl
end#dataload



print("🔥 ... ")
using CUDA
  using Flux: DataLoader, gpu, cpu
  println("✅")

NN = 24#neighbours
Mtr, ytr, Itr, Gtr, Mvl, yvl, Ivl, Gvl = dataload(NN)
  Mtr = transpose(Mtr) |> gpu
  ytr = transpose(ytr) |> gpu
  Mvl = transpose(Mvl) |> gpu
  yvl = transpose(yvl) |> gpu
nvars, nsamp = size(Mtr)
Dtr = DataLoader((Mtr, ytr), batchsize=1000, shuffle=true) 
Dvl = DataLoader((Mvl, yvl), batchsize=1000)





  using Flux: Chain, Dense, Dropout, relu, sigmoid
model = Chain(Dense(nvars, 1024, relu),
              Dense(1024, NN, sigmoid)) |> gpu





  function confussion(M, y, tres = 0.5)
pred = model(M) |> cpu
labs = y |> cpu
mxnrow, mxncol = size(labs)
C = DataFrame(d=Int[], obs=Int[], value=Symbol[])
for c in 1:mxncol for r in 1:mxnrow
  predtrue = pred[r, c] > tres
  labtrue = labs[r, c] > 0.5
  if predtrue && labtrue
    push!(C, (r, c, :TP))
  elseif predtrue == true
    push!(C, (r, c, :FP))
  elseif labtrue == true
    push!(C, (r, c, :FN))
  else
    push!(C, (r, c, :TN))
    end#if
  end#row
  end#col
return C
end


using Flux: train!, params, ADAM, binarycrossentropy
using Plots
layout = @layout [ l; ptr pvl ]

  losses = []
    loss(x, y) = binarycrossentropy(model(x), y) 
  epochs, opt = 1000, ADAM(0.0005)
@showprogress "🤖 " for epoch in 1:epochs
  for (x, y) in Dtr
    train!(loss, params(model), [(x, y)], opt)
    end#end for i
  push!(losses, (loss(Mtr, ytr), loss(Mvl, yvl)))

  lossplt = plot(ylim=(0, Inf))
  plot!(lossplt, [loss[1] for loss in losses], label="tr")
  plot!(lossplt, [loss[2] for loss in losses], label="vl")
  annotate!(lossplt, (0.5, 0.5), text("""
  tr = $(round(losses[end][1],digits=3))\n
  vl = $(round(losses[end][2],digits=3))\n
  diff = $(round(losses[end][2] - losses[end][1],digits=3))
  """, :center, 12, "black"))
  Ctr, Cvl = confussion(Mtr, ytr), confussion(Mvl, yvl)

  Ctrplt = pie([count(row -> row.value == :TP, eachrow(Ctr))
                count(row -> row.value == :FP, eachrow(Ctr))
                count(row -> row.value == :FN, eachrow(Ctr))
                count(row -> row.value == :TN, eachrow(Ctr))],
     color=[:green, :red, :yellow, :black], title="tr",
     legend=false)
  Cvlplt = pie([count(row -> row.value == :TP, eachrow(Cvl)),
                count(row -> row.value == :FP, eachrow(Cvl)),
                count(row -> row.value == :FN, eachrow(Cvl)),
                count(row -> row.value == :TN, eachrow(Cvl))],
     color=[:green, :red, :yellow, :black], title="vl",
     legend=false)
  
  display(plot(lossplt, 
               Ctrplt, Cvlplt, 
               layout=layout, 
               size=(800, 600)))
end#for epoch
print("📉 ", losses[end])




    """Dosyć kosztowna funkcja szukająca dobranej grupy i oceniająca"""
  function gstat(Gpred::Vector{Vector{Int}}, G::Vector{Vector{Int}})
sort!(Gpred, by=length, rev=true)
sort!(G,     by=length, rev=true)
stats = DataFrame(n=fill(Int(0), length(G)),
                  max=map(length, G),
                  groups=fill(Int(0), length(G)))

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



pred = Matrix(map(x -> x > 0.5 ? true : false, model(Mtr) |> cpu)')
Gpred = Features.widegroup(Itr, pred)
E, over, under = gstat(Gpred, Gtr)