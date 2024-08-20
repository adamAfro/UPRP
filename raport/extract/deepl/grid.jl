module Grid
export docnum, catnum, clmnum, txtnum, draw
using DataFrames, Plots

    "typ etykiety tekstu - referencja dokumentu"      const docnum = 1
    "typ etykiety tekstu - kategoria dokument"        const catnum = 2
    "typ etykiety tekstu - zastrzeżenia do dokumentu" const clmnum = 3
    "typ etykiety tekstu - brak etykiety"             const txtnum = 0
    """Koduje typ tekstu do liczby"""
  function encode(type::String)
    if type == "docs"     return docnum
elseif type == "category" return catnum
elseif type == "claims"   return clmnum
else                      return txtnum end
end
    """zamienia ramkę na macierz 3D, gdzie głębokość określa typ etykiety"""
  function render(df::Union{DataFrame, SubDataFrame}, resolution::Int; 
    from::Vector{Symbol}, colsuniqs::Vector{Vector{Int64}})
xmin = minimum([minimum(df.x0), minimum(df.x1), minimum(df.x2), minimum(df.x3)])
xmax = maximum([maximum(df.x0), maximum(df.x1), maximum(df.x2), maximum(df.x3)])
xrange = xmax - xmin; if xrange == 0 xrange = 1 end
ymin = minimum([minimum(df.y0), minimum(df.y1), minimum(df.y2), minimum(df.y3)])
ymax = maximum([maximum(df.y0), maximum(df.y1), maximum(df.y2), maximum(df.y3)])
yrange = ymax - ymin; if yrange == 0 yrange = 1 end
grids::Vector{Array{Int, 3}} = []
for uniqs in colsuniqs
  push!(grids, zeros(Int, resolution, resolution, length(uniqs))) 
  end
for row in eachrow(df)
  x0, x1, x2, x3 = row.x0, row.x1, row.x2, row.x3
  y0, y1, y2, y3 = row.y0, row.y1, row.y2, row.y3
  x0, x1, x2, x3 = Int.(ceil.(([x0, x1, x2, x3] .-xmin .+1) ./(xrange +1) .*resolution))
  y0, y1, y2, y3 = Int.(ceil.(([y0, y1, y2, y3] .-ymin .+1) ./(yrange +1) .*resolution))
  for (i, col) in enumerate(from)
    value = getproperty(row, col)
    channel = findfirst(colsuniqs[i] .== value)
    grids[i][x0:x2, y0:y2, channel] .= 1
    grids[i][x0:x1, y0:y1, channel] .= 0
    grids[i][x1:x2, y1:y2, channel] .= 0
    grids[i][x2:x3, y2:y3, channel] .= 0
    grids[i][x3:x0, y3:y0, channel] .= 0
    end
  end
return grids
end
    """Rysuje macierz 3D"""
  function show(rendered::Array{Int, 3})
maps = []
for i in 1:size(rendered, 3)
  H = heatmap(rendered[:, :, i], c=:grays, legend=false)
  push!(maps, H)
  end
return plot(maps..., layout=(1, size(rendered, 3)))
end
  end#module