I had a query I thought was too specific for the (very good) work on openprescribing.net and I found the actual EPD API too tricky to get very far with.

So I scraped all of the EPD monthly datasets I could with the `curl` package in R. 

That was 88 .zip files (so about 7 years worth), each about 500MB

When I extracted a couple with 7zip it turned out they were *really* compressed; the inflated files were about 6.5GB each.

That would consume all the storage on my laptop very quickly so I decided to convert them to `.parquet` files, which would be about 260MB each.

That's still 23GB of data, which probably won't load into the RAM on my hardware, but `arrow` allows you to query a `.parquet` file using `dplyr` verbs without loading it into the RAM. 

I suspect the fastest way to do this would have been to use `system2()` in R to unzip a file, `data.table::fread()` to load it, and `arrow::write_parquet()` to write it. Looping that and deleting the `.zip` and `.csv` files at the end of each iteration would have allowed the conversion without filling too much storage at once. 

But, since I'm trying to learn a language other than R, and for some reason I can't get into Python, I wrote this script in Rust. It took me a few weeks in total, and I did take some help from LLMs in the end. It worked though, and converted all 88 zipped `.csv` files into `.parquet` in about an hour on a 4 core CPU from 2017. 
