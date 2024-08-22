using CSV, DataFrames, ProgressMeter, Random, Statistics
using Base.Filesystem: joinpath

for p in ["paths.jl", "feats.jl", "../labels/meta.jl"] 
  include("../../$(p)") 
  end; import .Paths, .Features, .DatasetMeta


    """Wczytuje dane: z plików CSV łączy w jedną, szeroką ramkę"""
  function dataload(NN::Int; trsplit::Rational=4//5)
df = vcat([CSV.read(x, DataFrame; types=DatasetMeta.type) 
  for x in Paths.Recog.list(labeled=true)]...)
fdf = Features.extract(df)

pg1 = minimum(fdf.page)
gfdf = groupby(fdf, :unit)
gfdf = gfdf[shuffle(1:size(gfdf, 1))]
unitwides = Vector{DataFrame}()
unitlabs = Vector{Matrix}()

@showprogress 1 "🪗 " for (i, unit) in enumerate(gfdf)
if nrow(unit) <= NN
  if i == 1 || unit[1, :page] == pg1 
    continue 
    end 
  continue
  end
wide, labs = Features.widen(unit, NN)
push!(unitwides, wide); push!(unitlabs, labs)
end; wide, labs = vcat(unitwides...), vcat(unitlabs...)
select!(wide, Not(:unit))

idtr = 1:round(Int, trsplit*nrow(wide))
idvl = round(Int, trsplit*nrow(wide))+1:nrow(wide)

Mtr = Matrix{Float32}(wide[idtr, :])
Mvl = Matrix{Float32}(wide[idvl, :])
for col in 1:size(Mtr, 2)
  if all(v -> v == 0 || v == 1, Mtr[:, col]) m, s = 0, 1
  else m, s = mean(Mtr[:, col]), std(Mtr[:, col]) end
  if s == 0 s = 1 end
  Mtr[:, col] = (Mtr[:, col] .- m) ./ s
  Mvl[:, col] = (Mvl[:, col] .- m) ./ s
  end#for col

return Mtr, labs[idtr, :], Mvl, labs[idvl, :]
end#dataload



print("🔥 ... ")
using CUDA
  using Flux: DataLoader, gpu, cpu
  println("✅")

NN = 16#neighbours
Mtr, ytr, Mvl, yvl = dataload(NN)
  Mtr = transpose(Mtr) |> gpu
  ytr = transpose(ytr) |> gpu
  Mvl = transpose(Mvl) |> gpu
  yvl = transpose(yvl) |> gpu
nvars, nsamp = size(Mtr)
Dtr = DataLoader((Mtr, ytr), batchsize=10000, shuffle=true) 
Dvl = DataLoader((Mvl, yvl), batchsize=10000)





  using Flux: Chain, Dense, relu, sigmoid
model = Chain(Dense(nvars, 512, relu),
              Dense(512, 256, relu),
              Dense(256, 128, relu),
              Dense(128, 64, relu),
              Dense(64, 32, relu),
              Dense(32, NN, sigmoid)) |> gpu




  function confussion(M, y)
tres = 0.5
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


using Flux: train!, params, ADAM, logitcrossentropy
using Plots
layout = @layout [ l; ptr pvl ]

    loss(x, y) = logitcrossentropy(model(x), y) 
  epochs, opt, losses = 100, ADAM(0.001), []
@showprogress "🤖 " for epoch in 1:epochs
  for (x, y) in Dtr
    train!(loss, params(model), [(x, y)], opt)
    end#end for i
  push!(losses, (loss(Mtr, ytr), loss(Mvl, yvl)))

  lossplt = plot(ylim=(0, Inf))
  plot!(lossplt, [loss[1] for loss in losses], label="tr")
  plot!(lossplt, [loss[2] for loss in losses], label="vl")
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
