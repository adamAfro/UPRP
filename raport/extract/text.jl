module Text
  export wordchain, sententify
  




  """Zwraca listę słów z tekstu oraz znaków które je oddzielają"""
function wordchain(string::String)
chained = split(string, r"((?=[^a-zA-Z0-9\p{L}])|(?<=[^a-zA-Z0-9\p{L}]))")
chained = filter(x -> !isempty(x), chained)
return chained
end




  """Łączy słowa złożone z liter w sentencje"""
function sententify(strings::Union{Vector{String}, Vector{SubString{String}}})
sentified::Vector{String} = [strings[1]]
lttrprv = all(isletter, strings[1])
prvsent = lttrprv
for string in strings[2:end]
  if all(isletter, collect(string))
    lttrprv = true
    if prvsent
      sentified[end] = sentified[end] * " " * string
    else
      push!(sentified, string)
      prvsent = true
      end
  elseif lttrprv && (all(isspace, collect(string)) || string == ".")
    sentified[end] = sentified[end] * string
  elseif all(x -> isletter(x) || isdigit(x), collect(string)) 
    push!(sentified, string) 
    prvsent = false
  else prvsent = false end
end#for
return sentified
  end#function
end#TextFeatures