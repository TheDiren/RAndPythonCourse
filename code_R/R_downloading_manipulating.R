# How to download data from PANGAEA and manipulate it
#
# This will guide you how to search and retrieve diverse earth- and environmental 
# data and its metadata from the PANGAEA data repository using R. 
# It uses the PangaeaR package for data download.
#
# are shown to facilitate the metadata search. 
# Check out https://wiki.pangaea.de/wiki/PANGAEA_search for further details.

# Import libraries --------------------------------------------------------

# install.packages('pangaear')
# install.packages('dplyr')
library(pangaear) # https://github.com/ropensci/pangaear
library(dplyr)    # https://dplyr.tidyverse.org/
library(tidyr)    # https://dplyr.tidyverse.org/


# Query for data in PANGAEA -----------------------------------------------

# AIM: How to search for datasets of a particular topic such as a species, 
# location, project or author? 
#
# Notes:
# - Search term is enclosed with single quotes ' 
# - If the search term includes a blank, use additional double quotes " inside the single quotes
#   Example: 'sea ice' vs. '"sea ice"'
#   Example: 'parameter:Temperature, water method:CTD/Rosette' 
#            vs. 'parameter:"Temperature, water" method:CTD/Rosette'
#
# Query options:
# - limit = max number of datasets (default = 10, max = 500)
# - offset = fetch more than 500 results (pagination)
# - size_measure = "data.points" vs. "datasets"
# - score = match quality
#
# About pangaear:
# - R package by rOpenSci
# - Data retrieval interface for PANGAEA
# - GitHub: https://github.com/ropensci/pangaear
# - CRAN: https://CRAN.R-project.org/package=pangaear
# - Enables automated workflows, reproducibility, and bulk downloads


# 1. Basic data queries ------------------------------------------------------

# Query PANGAEA with 1 keyword
query <- pg_search('Geochemistry')
query

# Query PANGAEA with combinations of keywords
query <- pg_search('Geochemistry "sediment core"')

# Optional query terms
query <- pg_search('Geochemistry AND (Spitzbergen OR Svalbard)')

# Uncertain spelling
query <- pg_search('Pal?nologic')

# Specific Author
query <- pg_search('citation:author:Boetius')

# Within geographical coordinates (bounding box)
query <- pg_search(query = 'Geochemistry "sediment core"', count = 500, bbox=c(-60, 50, -10, 70))

# 2. Query PANGAEA without result limitations (pagination) -------------------

# Function to retrieve ALL results using pagination
pg_search_all <- function(keyword, count = 500) {
  all_results <- data.frame()
  offset <- 0
  
  repeat {
    results <- pg_search(keyword, count = count, offset = offset)
    all_results <- bind_rows(all_results, results)
    if (nrow(results) < count) break
    offset <- offset + count
  }
  return(all_results)
}

# query <- pg_search_all('Geochemistry "sediment core"')
query <- pg_search_all('project:label:PAGES_C-PEAT')

# Download datasets -------------------------------------------------------

# 1. Download single dataset -------------------------------------------------
# Hoppe, C et al. (2024): Chlorophyll a concentrations from leads, melt ponds and under ice sampling during the MOSAiC expedition (PS122) in the Central Arctic Ocean 2019-2020 [dataset]. PANGAEA, https://doi.org/10.1594/PANGAEA.972802

dset <- pg_data("10.1594/PANGAEA.972802")
df <- dset[[1]][["data"]]

# Save dataset
getwd()
dir.create(path=paste0(getwd(),"/PANGAEA_data"))

folderpath <- (paste0(getwd(),"/PANGAEA_data/"))
write.table(df, paste0(folderpath, "dataset_", gsub("10.1594/PANGAEA.", "", dset[[1]][["doi"]] ), ".txt"), 
            sep = "\t", quote = FALSE, row.names = FALSE, na = "")
# print(paste0("Dataset ",dset[[1]][["doi"]], " has been saved."))

# 2. Download multiple datasets -------------------------------------------------------

PAGES_Sweden <- pg_search("project:label:PAGES_C-PEAT", count = 500, bbox=c(17.7, 67.7, 21, 69))
PAGES_geochem <- filter(PAGES_Sweden, grepl("Geochemistry", citation))
PAGES_geochem <- arrange(PAGES_geochem, citation)

geochem_data <- data.frame()
for (i in 1:nrow(PAGES_geochem)) {
  geochem <- pg_data(PAGES_geochem$doi[i])
  event <- names(geochem[[1]][["metadata"]][["events"]][1])
  latitude <- geochem[[1]][["metadata"]][["events"]][["LATITUDE"]]
  longitude <- geochem[[1]][["metadata"]][["events"]][["LONGITUDE"]]
  geochem <- geochem[[1]]$data
  geochem$DOI <- PAGES_geochem$doi[i]
  geochem$event <- event
  geochem$latitude <- latitude
  geochem$longitude <- longitude
  geochem_data <- bind_rows(geochem_data, geochem)
}

# Alternative with pause on HTTP 429 errors
geochem_data <- data.frame()
for (i in 1:nrow(PAGES_geochem)) {
  success <- FALSE
  while (!success) {
    tryCatch({
      geochem <- pg_data(PAGES_geochem$doi[i])
      event <- names(geochem[[1]][["metadata"]][["events"]][1])
      latitude <- geochem[[1]][["metadata"]][["events"]][["LATITUDE"]]
      longitude <- geochem[[1]][["metadata"]][["events"]][["LONGITUDE"]]
      geochem <- geochem[[1]]$data
      geochem$DOI <- PAGES_geochem$doi[i]
      geochem$event <- event
      geochem$latitude <- latitude
      geochem$longitude <- longitude
      geochem_data <- bind_rows(geochem_data, geochem)
      success <- TRUE
    }, error = function(e) {
      if (grepl("HTTP 429", e$message)) {
        print("HTTP 429 error encountered. Waiting for 30 seconds.")
        Sys.sleep(30)
      } else {
        stop(e)
      }
    })
  }
}

# Column?  unique events?
names(geochem_data)
length(unique(geochem_data$event))

# Rearrange and merge OM [%] and LOI [%]
geochem_data_unsorted <- geochem_data
geochem_data <- geochem_data_unsorted[,c(11:14, 1:2, 15, 3, 4, 16, 5, 6, 17, 7:9)]
geochem_data <- unite(data = geochem_data, col = "OM [%]", `OM [%]`, `LOI [%]`, na.rm = TRUE, sep = ",")

# Save final data frame
write.table(geochem_data, paste0(folderpath, "CPEAT_Geochem_Sweden.txt"), sep = "\t", 
            quote = FALSE, row.names = FALSE, na = "")
# print("Dataset has been saved.")


# Download dataset including binary files (e.g. images) -------------------
# Katlein, C et al. (2020): Extracted frames from main ROV camera videos during MOSAiC Leg 2 [dataset]. PANGAEA, https://doi.org/10.1594/PANGAEA.919398

table <- pg_data(doi="10.1594/PANGAEA.919398")
table <- table[[1]]$data
prefix <- "https://download.pangaea.de/dataset/919398/files/"
folderpath <- "/PANGAEA_data"

download.file(paste0(prefix, table$IMAGE[1]), destfile = paste0(folderpath, table$'IMAGE'[1]))

# Download one file
download.file("https://download.pangaea.de/dataset/919398/files/vlcsnap-2020-01-26-12h57m03s914.png", 
              "vlcsnap-2020-01-26-12h57m03s914.png")

# Example conditional download (disabled):
# for (i in (1:nrow(table))){
#   if (grepl("fauna", table$'Content'[i]) == TRUE ) {
#     download.file(paste0(prefix, table$IMAGE[i]), destfile = table$'IMAGE'[i])
#     print(paste0("File ", table$'IMAGE'[i], " has been saved."))
#   }
# }
