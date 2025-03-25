## EPD Converter 

The [English prescribing dataset](https://www.nhsbsa.nhs.uk/prescription-data/prescribing-data/english-prescribing-data-epd) (EPD) contains detailed information on prescriptions issued in England.

I had a query I thought was too specific for the (very good) work on [openprescribing.net](openprescribing.net) and I found the actual EPD API too tricky to get very far with.

The raw data is on the website to download as CSV or ZIP files. There's a seperate dataset for every month of available data.

I downloaded all 88 available zip files with `curl` package in R. They were about 500MB each, so about 40GB in total. 

When I extracted a couple with 7zip it turned out they were *really* compressed; the inflated files were about 6.5GB each so if I extracted them all it'd be over 500GB.

That would consume all the storage on my laptop very quickly so I decided to convert them to `.parquet` files, which would be about 260MB each.

That's still 23GB of data, which probably won't load into the RAM on my hardware, but `arrow` allows you to query a `.parquet` file using `dplyr` verbs without loading it into the RAM. 

I suspect the fastest way to do this would have been to use `system2()` in R to unzip a file, `data.table::fread()` to load it, and `arrow::write_parquet()` to write it. Looping that and deleting the `.zip` and `.csv` files at the end of each iteration would have allowed the conversion without filling too much storage at once. [*Edit: I have since tested this and it actually seems to take about twice as long (about 80 seconds per file), although I didn't give `fread()` a list of the column data types*] 

But, since I'm trying to learn a language other than R, and for some reason I can't get into Python, I wrote this script in Rust. It took me a few weeks in total, and I did take some help from LLMs in the end. It worked though, and converted all 88 zipped `.csv` files into `.parquet` in about an hour on a 4 core CPU from 2017. 

And it turns out you don't have to join all the `.parquet` files together into one big file in order to query it, see the Arrow tutorial on working with multi-file datasets: https://arrow.apache.org/docs/r/articles/dataset.html

I had a go at this and it seems to work very well, example below. I am still stuck with 23GB of prescriptions data on my laptop though.

```{r}
library(arrow, warn.conflicts = FALSE)
library(dplyr, warn.conflicts = FALSE)

ds <-
  arrow::open_dataset("data")


sum_na_rm <- function(x) { # {arrow} does not allow for anonymous functions
  sum(x, na.rm = TRUE)
}

df <-
ds |>
  filter(BNF_CHEMICAL_SUBSTANCE %in% c("0410030C0",     # Methadone
                                       "0410030B0",     # Subutex
                                       "0410030A0")) |> # Buprenorphine
  select(YEAR_MONTH,
         REGIONAL_OFFICE_NAME ,
         BNF_CHEMICAL_SUBSTANCE,
         BNF_DESCRIPTION,
         TOTAL_QUANTITY,
         ACTUAL_COST) |>
  mutate(TOTAL_QUANTITY = as.numeric(TOTAL_QUANTITY),
         ACTUAL_COST = as.numeric(ACTUAL_COST)) |>
  group_by(YEAR_MONTH, BNF_DESCRIPTION) |>
  summarise(across(c(TOTAL_QUANTITY,
                     ACTUAL_COST),
                   sum_na_rm)) |>
  collect()

```

## Usage

If for some reason you'd like to try to use this you'll need to install rust. I recommend following the method in [The Rust Programming Language](https://doc.rust-lang.org/book/ch01-01-installation.htmlhttps://doc.rust-lang.org/book/ch01-01-installation.html) book.

On Linux you can just run:

```{sh}

$ curl --proto '=https' --tlsv1.2 https://sh.rustup.rs -sSf | sh

```

```
```
