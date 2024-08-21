using CSV, DataFrames, ProgressMeter, Random, Statistics
using Base.Filesystem: joinpath

for p in ["paths.jl", "feats.jl", "../labels/meta.jl"] 
  include("../../$(p)") 
  end; import .Paths, .Features, .DatasetMeta


    """Wczytuje dane: z plików CSV łączy w jedną, szeroką ramkę"""
  function dataload(NN::Int)
df = vcat([CSV.read(x, DataFrame; types=DatasetMeta.type) 
  for x in Paths.Recog.list(labeled=true)]...)
fdf = Features.extract(df)

pg1 = minimum(fdf.page)
gfdf = groupby(fdf, :unit)
unitwides, unitlabs = Vector{DataFrame}(), Vector{Matrix}()
@showprogress 1 "🪗 " for (i, unit) in enumerate(gfdf)
if nrow(unit) <= NN
  if i == 1 || unit[1, :page] == pg1 
    continue 
    end 
  continue
  end
wide, labs = Features.widen(unit, NN)
push!(unitwides, wide)
push!(unitlabs, labs)
end#for

wide = vcat(unitwides...) #mapowanie pkt. na współrzędne
for name in ["btmlft", "btmrgt", "toplft", "toprgt"]
  wide[!, "x$(name)"] = [pt[1] for pt in wide[!, "p$(name)"]]
  wide[!, "y$(name)"] = [pt[2] for pt in wide[!, "p$(name)"]]
  end; select!(wide, Not(:pbtmlft, :pbtmrgt, :ptoplft, :ptoprgt))

mx = Matrix{Float32}(select(wide, Not(:unit)))
return mx, vcat(unitlabs...)
end



print("🔥 ... ")
using CUDA
  using Flux: DataLoader, gpu, cpu
  println("✅")

NN = 16#neighbours
trmx, trlabs = dataload(NN)
trmx = transpose(trmx) |> gpu
trlabs = transpose(trlabs) |> gpu
nvars, nsamp = size(trmx)
loadtr = DataLoader((trmx, trlabs), 
                     batchsize=10000, shuffle=true) 





  using Flux: Chain, Dense, relu, sigmoid
model = Chain(Dense(nvars, 512, relu),
              Dense(512, 256, relu),
              Dense(256, 128, relu),
              Dense(128, 64, relu),
              Dense(64, 32, relu),
              Dense(32, NN, sigmoid)) |> gpu




using Flux: train!, params, ADAM, logitcrossentropy

        loss(x, y) = logitcrossentropy(model(x), y) 
      epochs, opt, losses = 100, ADAM(0.001), []
    @showprogress "🤖 " for epoch in 1:epochs
  for (x, y) in loadtr
train!(loss, params(model), [(x, y)], opt)
end#end for i
push!(losses, loss(trmx, trlabs))
end#for epoch
print("📉 ", losses[end])





using Plots
clrmap = Dict(:TP => :green, 
              :FP => :red, 
              :FN => :yellow, 
              :TN => :black)

tres = 0.5
pred = model(trmx) |> cpu
labs = trlabs |> cpu
mxnrow, mxncol = size(labs)
acc = DataFrame(d=Int[], obs=Int[], value=Symbol[])
for c in 1:mxncol for r in 1:mxnrow
  predtrue = pred[r, c] > tres
  labtrue = labs[r, c] > 0.5
  if predtrue && labtrue
    push!(acc, (r, c, :TP))
  elseif predtrue == true
    push!(acc, (r, c, :FP))
  elseif labtrue == true
    push!(acc, (r, c, :FN))
  else
    push!(acc, (r, c, :TN))
    end#if
  end#row
  end#col

pie([count(row -> row.value == :TP, eachrow(acc)),
     count(row -> row.value == :FP, eachrow(acc)),
     count(row -> row.value == :FN, eachrow(acc)),
     count(row -> row.value == :TN, eachrow(acc))],
     color=[:green, :red, :yellow, :black])

accgr = groupby(acc, [:d, :value])
counts = combine(accgr, nrow)
sort!(counts, [:d])
clr = [clrmap[val] for val in counts.value]
bar(string.(counts.d), 
    counts.nrow, 
    group=counts.value, color=clr)