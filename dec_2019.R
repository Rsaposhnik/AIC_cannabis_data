######### CODE OVERVIEW

#Coder: Raffaele Saposhnik, raffaele.saposhnik@yale.edu
#Date: April - August 2021
#Objective: Clean and prepare Weedmaps data scrape to generate disctinct datasets for
#           future econometric and statistical analyses

######### User Input 

#(INPUT Scrape Date HERE)
scrape_date <- "12_01_2019"

#(INPUT path of data scrape here)
data_path <- "/Users/rsaposhnik/Documents/AIC/data"

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
library("readxl")
library(ggplot2)

# A1: LOAD IN DISPENSARY (MERCHANT) DETAILS FOR DELIV AND STOREFRONT -----------------------------------------------------

dispensaries_sf <- read_csv(glue("{data_path}/scraped_data/{scrape_date}/dispensary_services.csv")) %>%
  mutate(id_for_merge = glue("storefront_{ID}") )

dispensaries_delivery <- read_csv(glue("{data_path}/scraped_data/{scrape_date}/delivery_services.csv")) %>%
  mutate(id_for_merge = glue("delivery_{ID}") )
                                                          
all_dispensary_info <- rbind(dispensaries_sf, dispensaries_delivery)

rm(dispensaries_sf,
   dispensaries_delivery)


# A2: ADD IN COUNTY-MAPPING TO MERCHANT DETAILS ACROSS USA -----------------------------------------------------

########################### A2.i - Map all US ZIP codes to county subdivisions (subs)

#Download all state names 
all_states <- state.abb

#Start with first Alphabetical state by creating dataframe(Alabama)
df_county_subs <- county_subdivisions(state = "AL",
                                      cb = TRUE,
                                      class = "sf")

#Loop through remaining 49 and add to df, making a full dataset of all counties and their geospatial boundaries
remaining_states <- all_states[2:50]

for (looped_state in remaining_states) {
  loop_county_sub <- county_subdivisions(state = looped_state,
                                         cb = TRUE,
                                         class = "sf")
  df_county_subs <- rbind(df_county_subs,
                          loop_county_sub)
}

rm(loop_county_sub)

#create ID_variable to merge county_names (from lookup) onto their state_county identifier
df_county_subs <- df_county_subs %>%
  mutate(county_FIPS = parse_number(glue("{STATEFP}{COUNTYFP}"))) %>%
  select(county_FIPS, 
         geometry)

#Read in County FIP to County Crosswalk (downloaded from https://www.nrcs.usda.gov/wps/portal/nrcs/detail/national/home/?cid=nrcs143_013697)
county_crosswalk <- read_xlsx(glue("{data_path}/lookups/county_fips_lookup.xlsx"), skip = 1)

#
df_county_subs <- df_county_subs %>% 
  left_join(.,
            county_crosswalk, 
            by = "county_FIPS")

rm(county_crosswalk)

########################### A2.ii - Determine Geospatial point of each dispensary in dataset using Lat/Lon

#Narrow down dispensary dataset to only geospatial details and variables needed to merge back on
disp_lat_lon <- all_dispensary_info %>%
  select(ID, id_for_merge, Slug, Longitude, Latitude )

#Convert Lat/Lon to geometry 
library(sf)
disp_lat_lon <- st_as_sf(disp_lat_lon, coords = c("Longitude", "Latitude"), 
                         crs = 4326, agr = "constant")

disp_lat_lon <- st_transform(disp_lat_lon, 4269) 

# Using our greater county-geometry map from earlier, we can now pinpoint (by joining) where each
# dispensary falls into a specific county
library(sf)
mapped_dispensaries <- st_join(disp_lat_lon, df_county_subs, join = st_within) %>%
  select(id_for_merge, county_name)

rm(disp_lat_lon)
#Remove geometry
mapped_dispensaries <- st_set_geometry(mapped_dispensaries, NULL)

########################### A2.iii - merge county names back onto the master "all dispensary" df using our merge ID
all_dispensary_info <- all_dispensary_info %>%
  left_join(.,
            mapped_dispensaries,
            "id_for_merge")

rm(mapped_dispensaries)





# A3: STANDARDIZE STATE NAME ACROSS DATASET  -----------------------------------------------------










