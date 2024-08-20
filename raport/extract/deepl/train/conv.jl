using Flux, JLD2, Random, CUDA, ProgressMeter, Plots
using Flux: 
Chain, Conv, ConvTranspose, MaxPool, 
  relu, softmax, params, DataLoader, crossentropy
using Base.Filesystem: joinpath

collect(devices())
# device!(1)
println("CUDA")
CUse = CUDA.has_cuda()
if CUse CUDA.memory_status()
else println("(x_X)") end

  function conv(input::Int, output::Int)
return Chain(Conv((16, 16), input => output, pad=1), relu,
              Conv((16, 16), output => output, pad=1), relu)
              end
  function upconv(input::Int, output::Int)
return Chain(ConvTranspose((2, 2), input => output, stride=2), relu)
end

  function architecture(inchan::Int, outchan::Int) return Chain(
conv(inchan, 64), MaxPool((2, 2)),
conv(64, 128), MaxPool((2, 2)),
conv(128, 256), MaxPool((2, 2)),
conv(256, 512), MaxPool((2, 2)),
conv(512, 1024),
upconv(1024, 512), conv(512, 512),
upconv(512, 256), conv(256, 256),
upconv(256, 128), conv(128, 128),
upconv(128, 64), conv(64, 64),
Conv((1, 1), 64 => outchan), softmax
) end

function split(X, Y, val_split=0.2)
  n = size(X, 4)
  idx = shuffle(1:n)
  n_val = round(Int, val_split * n)
  val_idx = idx[1:n_val]
  train_idx = idx[n_val+1:end]
  return X[:,:,:,train_idx], Y[:,:,:,train_idx], X[:,:,:,val_idx], Y[:,:,:,val_idx]
  end

#Model
inchan, outchan = 3, 24
model = architecture(inchan, outchan)
#Data
X = jldopen(joinpath(@__DIR__, "grid.X.jld2"), "r") do file read(file, "X") end
Y = jldopen(joinpath(@__DIR__, "grid.Y.jld2"), "r") do file read(file, "Y") end
X_train, Y_train, X_val, Y_val = split(X, Y)
batch_size = 16
train_loader = DataLoader((X_train, Y_train), batchsize=batch_size, shuffle=true)
val_loader = DataLoader((X_val, Y_val), batchsize=batch_size)
#Training
opt = ADAM(0.0005)
epochs = 300
loss(x, y) = crossentropy(model(x), y)
vhis, trhis = [], []
if CUse
  model = gpu(model)
  loss = gpu(loss)
  end
for epoch in 1:epochs
  trloss = 0.0
  @showprogress desc=string(epoch) for (x, y) in train_loader
    if CUse x, y = gpu(x), gpu(y) end
    Flux.train!(loss, params(model), [(x, y)], opt)
    trloss += loss(x, y)
    if CUse
      CUDA.unsafe_free!(x)
      CUDA.unsafe_free!(y)
      end
    end
  trloss /= length(train_loader)
  print(" ", trloss)
  push!(trhis, trloss)

  vloss = 0.0
  for (x, y) in val_loader
    if CUse x, y = gpu(x), gpu(y) end
    vloss += loss(x, y)
    if CUse
      CUDA.unsafe_free!(x)
      CUDA.unsafe_free!(y)
      end
    end
  vloss /= length(val_loader)
  print(" ", vloss)
  push!(vhis, vloss)

  if epoch > 11
    before = vhis[1:end-10]
    last = vhis[end-9:end]
    if minimum(last) >= minimum(before) break end
    end

  println()
end

plot(1:length(trhis), trhis, label="Train Loss", xlabel="Epoch", ylabel="Loss", title="Training and Validation Loss")
plot!(1:length(vhis), vhis, label="Validation Loss")
