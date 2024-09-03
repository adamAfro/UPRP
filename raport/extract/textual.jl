module Textual
using StringDistances

function levenshtein(str1::String, str2::String)
return evaluate(Levenshtein(), str1, str2)
end



"""Zwraca listę słów z tekstu oraz znaków które je oddzielają"""
function wordchain(string::String)::Vector{String}
chained = split(string, r"((?=[^a-zA-Z0-9\p{L}])|(?<=[^a-zA-Z0-9\p{L}]))")
chained = filter(x -> !isempty(x), chained)
return chained
end



"""Łączy słowa złożone z liter w sentencje"""
function sententify(strings::Union{Vector{String}, Vector{SubString{String}}})::Vector{String}
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



function yearalike(string::String)::Bool
return (startswith(string, "20") || 
        startswith(string, "19")) && length(string) == 4
        end



function codealike(string::String)::Bool
return count(isdigit, string) > 4
end



"""Czy w tekście znajduje się słowo _dokument_:\n
1. słowo dokument (l. poj.)
2. słowo dokumenty (l. mn.)
3. dokument/y poprzedzające dalszy tekst\n
`tol` - tolerancja dla odległości między słowami"""
function dockeyword(string::String, tol::Float64)::Int
words = split(string, " ")
n = length(words)
for (i, word) in enumerate(words)
word = lowercase(String(word))
dist = levenshtein("dokument", word)
if dist >= length(word)*tol continue end
if i < n return 3 end
if levenshtein("dokumenty", word) < dist ||
   levenshtein("dokumentów", word) < dist
return 2 else return 1 end
end
return 0
end



end#TextFeatures