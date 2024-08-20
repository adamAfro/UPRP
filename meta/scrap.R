# set up the scene -----

setwd("/home/adam/Projekty/inżynierka🎓/uprp/p")

library(tidyverse)
library(httr)
library(jsonlite)
library(XML)
library(xml2)
library(curl)

# get one-time token for API with access token
token <- content(
  httr::POST(
    "https://api.uprp.gov.pl/cmuia/api/external/auth/token", 
    body = list(
      access_token = "API-be36a67954a339674ac2da9f4c29519bf931dd1ff7926e11e1ca01ed14931524"
    ), 
    encode = "form"
  )
)

# get data from API ----
## get patents (Patenty) records via API "P" ----
## prepare patent numbers

numbers_p <- map(
  .progress = T,
  .x = 0:445000,
  .f = \(p) paste0(
    "https://api.uprp.gov.pl/ewyszukiwarka/api/external/pwp/",
    "P.",
    paste0(
      rep(0, 6 - nchar(p)),
      collapse = ""
    ),
    p, 
    collapse = ""
  )
)
## add exceptions

numbers_p <- append(
  numbers_p,
  paste0(
    "https://api.uprp.gov.pl/ewyszukiwarka/api/external/pwp/",
    c(
      'P.045924-A',
      'P.046210-A',
      'P.047104-A',
      'P.101936-A',
      'P.103221-A',
      'P.104705-A',
      'P.104912-A',
      'P.105050-A',
      'P.106219-A',
      'P.106232-A',
      'P.106640-A',
      'P.109561-A',
      'P.110036-A',
      'P.110488-A',
      'P.111089-A',
      'P.111742-A',
      'P.112252-A',
      'P.112288-A',
      'P.114374-A',
      'P.114379-A',
      'P.114820-A',
      'P.116211-A',
      'P.118995-A',
      'P.120320-A',
      'P.121631-A',
      'P.140470-A',
      'P.141556-A',
      'P.174824-A',
      'P.96307-A'
    )
  )
)

## get records from API

for (p in 1:length(numbers_p)) {
  print(p)
  # get response from API
  tryCatch(
    {
      record <- GET(
        numbers_p[[p]], 
        add_headers(
          Authorization = paste(
            "Bearer",
            token
          ) 
        )
      ) |> 
      # convert response to xml document
        read_xml()
      # write XML document 
      write_xml(
        record,
        file = paste0(
          str_extract(
            numbers_p[[p]],
            "P\\..+$"
          ),
          ".xml"
        )
      )
    },
    error = function(e) {
      record <- as_xml_document(list(root = list()))
      write_xml(
        record,
        file = paste0(
          str_extract(
            numbers_p[[p]],
            "P\\..+$"
          ),
          "_empty.xml"
        )
      )
    }
  )
}
