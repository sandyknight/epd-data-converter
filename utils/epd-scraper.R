library(rvest)
library(curl)

url <-
  "https://opendata.nhsbsa.net/dataset/english-prescribing-data-epd"


page <- read_html(url)

urls <- page %>%
  html_nodes("a") %>%
  html_attr("href") %>%
  { .[grepl("^https://", .)] }



urls <- unique(grep("zip", urls, value =  TRUE))


vapply(as.character(seq(from = 1, to = 88, by = 1)),
       function(x) ifelse(nchar(x) < 2, paste0("0", x), x),
       FUN.VALUE = "character",
       USE.NAMES =  FALSE)

nms <-
file.path("data",
          paste0(
          "epd-data-",
          vapply(as.character(seq(from = 1,
                                  to = 88,
                                  by = 1)),
                 function(x) ifelse(nchar(x) < 2,
                                    paste0("0", x),
                                    x),
                 FUN.VALUE = "character",
                 USE.NAMES =  FALSE), ".zip"))


for (i in seq_along(urls)) {
  curl::curl_download(urls[i], destfile = nms[i])
}


## curl_download(urls[1], destfile =  "data/epd-data-01.zip")
## curl_download(urls[2], destfile =  "data/epd-data-02.zip", quiet = FALSE)
