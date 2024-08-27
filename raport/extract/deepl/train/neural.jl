print("🔥 ... ")
using Flux: Chain, Dense, Dropout, relu, sigmoid
using Flux: DataLoader, gpu, cpu
using Flux: train!, params, ADAM, binarycrossentropy
using CUDA, Plots, CSV, DataFrames, ProgressMeter, Random, Statistics
using Base.Filesystem: joinpath
for p in ["paths.jl", "feats.jl", "score.jl", "../labels/meta.jl"]
  include("../../$(p)") 
  end; import .Paths, .Features, .DatasetMeta, .Score
println("✅")







  function architect(input::Int, output::Int, ndense::Vector{Int}; dropout=0)
model = Chain(Dense(input, ndense[1], relu), Dropout(dropout))
for (prv, h) in enumerate(ndense[2:end])
  model = Chain(model, Dense(ndense[prv], h, relu))
  if dropout > 0 model = Chain(model, Dropout(dropout)) end
  end
return Chain(model, Dense(ndense[end], output, sigmoid))
end







  function train(model, Mtr, ytr, Mvl, yvl; epochs = 100, opt = ADAM(0.0005), layout = @layout [ l; ptr pvl ])
losses = []

    loss(x, y) = binarycrossentropy(model(x), y) 
  @showprogress "🤖 " for epoch in 1:epochs
for (x, y) in Dtr
  train!(loss, params(model), [(x, y)], opt)
  end#end for i
push!(losses, (loss(Mtr, ytr), loss(Mvl, yvl)))
diff = losses[end][2] - losses[end][1]
lossplt = plot(ylim=(0, Inf))
plot!(lossplt, [loss[1] for loss in losses], label="tr")
plot!(lossplt, [loss[2] for loss in losses], label="vl")
annotate!(lossplt, (0.5, 0.5), text("""
tr = $(round(losses[end][1],digits=3))\n
vl = $(round(losses[end][2],digits=3))\n
(tr-vl)/tr = $(round(diff/losses[end][1],digits=3))
""", :center, 12, "black"))

Ctr = Score.mxconfussion(model(Mtr) |> cpu, ytr |> cpu) 
Cvl = Score.mxconfussion(model(Mvl) |> cpu, yvl |> cpu)
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
if diff/losses[end][1] > 0.20 break end
end#for epoch
return losses[end]
  end#train






df = vcat([CSV.read(x, DataFrame; types=DatasetMeta.type) 
          for x in Paths.Recog.list(labeled=true)]...)


NNeigh = 24
Mtr, ytr, Itr, Gtr, Mvl, yvl, Ivl, Gvl = Features.widenmx(df, NNeigh; aug=
  Features.Augment(2, 3, 1, 0, false,false,true,false,false))

  Mtr = transpose(Mtr) |> gpu
  ytr = transpose(ytr) |> gpu
  Mvl = transpose(Mvl) |> gpu
  yvl = transpose(yvl) |> gpu

nvars, nsamp = size(Mtr)
Dtr = DataLoader((Mtr, ytr), batchsize=2000, shuffle=true) 
Dvl = DataLoader((Mvl, yvl))

model = architect(nvars, NNeigh, [100, 1000]) |> gpu
train(model, Mtr, ytr, Mvl, yvl; opt=ADAM(0.001))

pred = Matrix(map(x -> x > 0.5 ? true : false, model(Mtr) |> cpu)')
Gpred = Features.widegroup(Itr, pred)
E, over, under = Score.mxcount(Gpred, Gtr)