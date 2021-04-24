######### CODE OVERVIEW

#Coder: Raffaele Saposhnik, raffaele.saposhnik@yale.edu
#Date: April - August 2021
#Objective: Clean and prepare Weedmaps data scrape to generate disctinct datasets for
#           future econometric and statistical analyses

######### User Input (PUT DATE HERE)

scrape_date <- "12_01_2019"

# Install Packges and fetch library -----------------------------------------------------
# install.packages("tigris")
# install.packages('devtools')
# install.packages('zipcodeR')
# install.packages("tidyverse")

library("tigris")
library('devtools')
library('zipcodeR')
library("tidyverse")
library("glue")

#################### Load in XLSX of Dispensaries
dispensaries_sf <- read_csv(glue("/Users/rsaposhnik/Documents/AIC/data/scraped_data/{scrape_date}/dispensary_services.csv")) %>%
  mutate(id_for_merge = glue("storefront_{ID}") )

dispensaries_delivery <- read_csv(glue("/Users/rsaposhnik/Documents/AIC/data/scraped_data/{scrape_date}/delivery_services.csv")) %>%
  mutate(id_for_merge = glue("delivery_{ID}") )
                                                          
all_dispensary_info <- rbind(dispensaries_sf, dispensaries_delivery)

rm(dispensaries_sf,
   dispensaries_delivery)
