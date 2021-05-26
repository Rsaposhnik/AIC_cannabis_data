######### CODE OVERVIEW

#Coder: Raffaele Saposhnik, raffaele.saposhnik@yale.edu
#Date: April - August 2021
#Objective: Clean and prepare Weedmaps data scrape to generate disctinct datasets for
#           future econometric and statistical analyses

######### User Input 

#(INPUT Scrape Dates HERE)
scrape_dates <- c("12_01_2019")

#(INPUT path of data scrapes here)
data_path <- "/Users/rsaposhnik/Documents/AIC/data"

# Install Packges and fetch library -----------------------------------------------------

# install.packages("tigris")
# install.packages('devtools')
# install.packages('zipcodeR')
# install.packages("tidyverse")
# install.packages("writexl")
# install.packages("lubridate")

library("tigris")
library('devtools')
library('zipcodeR')
library("tidyverse")
library("glue")
library("readxl")
library(ggplot2)
library("writexl")
library(lubridate)


#Begin For-Loop to create datasets for all scrape dates 
for (scrape_date in scrape_dates) {
  
### SECTION A - Merchant Information (Storefront Dispensaries and Delivery Dispensaries)
# A1: LOAD IN DISPENSARY (MERCHANT) DETAILS FOR DELIV AND STOREFRONT -----------------------------------------------------

dispensaries_sf <- read_csv(glue("{data_path}/scraped_data/{scrape_date}/dispensary_services.csv")) %>%
  mutate(id_for_merge = glue("storefront_{ID}") )

dispensaries_delivery <- read_csv(glue("{data_path}/scraped_data/{scrape_date}/delivery_services.csv")) %>%
  mutate(id_for_merge = glue("delivery_{ID}") )
                                                          
all_dispensary_info <- rbind(dispensaries_sf, dispensaries_delivery)

rm(dispensaries_sf,
   dispensaries_delivery)


# A2: STANDARDIZE AND ADD COUNTY-MAPPING TO MERCHANT DETAILS ACROSS USA (USING LAT/LON)-----------------------------------------------------

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
rm(df_county_subs)
#Remove geometry
mapped_dispensaries <- st_set_geometry(mapped_dispensaries, NULL)

#Remove duplicates Merge IDs (multiple of the same dispensary in the same county)
mapped_dispensaries <- mapped_dispensaries %>%
  mutate(dupe = duplicated(id_for_merge)) %>%
  filter(dupe == FALSE) %>%
  select(-dupe)

########################### A2.iii - merge county names back onto the master "all dispensary" df using our merge ID
all_dispensary_info <- all_dispensary_info %>%
  left_join(.,
            mapped_dispensaries,
            "id_for_merge")

rm(mapped_dispensaries)


# A3: STANDARDIZE STATE NAME ACROSS DATASET (USING SAME LAT/LON FROM DISPENSARIES), Remove non-US Entries -----------------------------------------------------

########### A3.i Read in Geospatial map of states across USA, and select state Abbrev and geometry

states_geom_df <- states(cb = FALSE, resolution = "500k", year = 2019) %>%
  select(STUSPS, geometry)

########### A3.ii #Narrow down dispensary dataset only to geosaptial details and then join on state boundaries

disp_lat_lon <- all_dispensary_info %>%
  select(ID, id_for_merge, Slug, Longitude, Latitude )

#Convert Lat/Lon to geometry 
library(sf)
disp_lat_lon <- st_as_sf(disp_lat_lon, coords = c("Longitude", "Latitude"), 
                         crs = 4326, agr = "constant")

disp_lat_lon <- st_transform(disp_lat_lon, 4269) 

# Using our greater state-geometry map from earlier, we can now pinpoint (by joining) where each
# dispensary falls into a specific state
library(sf)
mapped_dispensaries <- st_join(disp_lat_lon, states_geom_df, join = st_within) %>%
  select(id_for_merge, STUSPS) %>%
  rename("corrected_state" = "STUSPS")

rm(disp_lat_lon)
rm(states_geom_df)
#Remove geometry
mapped_dispensaries <- st_set_geometry(mapped_dispensaries, NULL)

#Remove duplicates Merge IDs (multiple of the same dispensary in the same state)
mapped_dispensaries <- mapped_dispensaries %>%
  mutate(dupe = duplicated(id_for_merge)) %>%
  filter(dupe == FALSE) %>%
  select(-dupe)

########################### A3.iii - merge corrected state names back onto the master "all dispensary" df using our merge ID
all_dispensary_info <- all_dispensary_info %>%
  left_join(.,
            mapped_dispensaries,
            "id_for_merge")

rm(mapped_dispensaries)

#Reorganize Data and filter our non-US entries
all_dispensary_info <- all_dispensary_info %>% 
  select(State,
         corrected_state,
         county_name,
         City, 
         `Zip Code`,
         everything()) %>%
  filter(!is.na(corrected_state))

# A4: RENAME RELVEANT VARIABLES,-----------------------------------------------------

########## Rename Variables
all_dispensary_info <- all_dispensary_info %>%
  rename("phone_number" = "Phone Number",
         "slug" = "Slug",
         "city" = "City",
         "lic_type_1" = "License Type 1",
         "lic_num_1" = "License Number 1",
         "lic_type_2" = "License Type 2",
         "lic_num_2" = "License Number 2",
         "lic_type_3" = "License Type 3",
         "lic_num_3" = "License Number 3",
         "lic_type_4" = "License Type 4",
         "lic_num_4" = "License Number 4") %>%
  mutate(id_for_merge = as.character(id_for_merge))


# A5: REFINE LICENSING DETALIS, ADD SCRARPE DATE TO VARIABLE----------------------------------------------------

############ Make blank cells equal to "- for License Numbers
all_dispensary_info <- all_dispensary_info %>%
  mutate(lic_num_1 = ifelse(is.na(lic_num_1),"-", lic_num_1),
         lic_type_1 = ifelse(lic_num_1 == "-", "-", lic_type_1),
         lic_num_2 = ifelse(is.na(lic_num_2),"-", lic_num_2),
         lic_type_2 = ifelse(lic_num_2 == "-", "-", lic_type_2),
         lic_num_3 = ifelse(is.na(lic_num_3),"-", lic_num_3),
         lic_type_3 = ifelse(lic_num_3 == "-", "-", lic_type_3),
         lic_num_4 = ifelse(is.na(lic_num_4),"-", lic_num_4),
         lic_type_4 = ifelse(lic_num_4 == "-", "-", lic_type_4)
  )

############ Add License Type Variable Checks [ (A) Is Lic valid? ]  (B) What is the Lic type?
all_dispensary_info <- all_dispensary_info %>%
  mutate(lic_category_1 = case_when(str_detect(lic_num_1, regex("\\bLIC\\b", ignore_case = TRUE)) ~ "LIC",
                                    str_detect(lic_num_1, regex("\\bTEMP\\b", ignore_case = TRUE)) ~"TEMP",
                                    lic_num_1 == "-" ~ "-",
                                    TRUE ~ "Other License"
  ),
  lic_category_2 = case_when(str_detect(lic_num_2, regex("\\bLIC\\b", ignore_case = TRUE)) ~ "LIC",
                             str_detect(lic_num_2, regex("\\bTEMP\\b", ignore_case = TRUE)) ~"TEMP",
                             lic_num_2 == "-" ~ "-",
                             TRUE ~ "Other License"
  ),
  lic_category_3 = case_when(str_detect(lic_num_3, regex("\\bLIC\\b", ignore_case = TRUE)) ~ "LIC",
                             str_detect(lic_num_3, regex("\\bTEMP\\b", ignore_case = TRUE)) ~"TEMP",
                             lic_num_3 == "-" ~ "-",
                             TRUE ~ "Other License"
  ),
  lic_category_4 = case_when(str_detect(lic_num_4, regex("\\bLIC\\b", ignore_case = TRUE)) ~ "LIC",
                             str_detect(lic_num_4, regex("\\bTEMP\\b", ignore_case = TRUE)) ~"TEMP",
                             lic_num_4 == "-" ~ "-",
                             TRUE ~ "Other License"
  )
  )


### Add Scrape Date to Variable
all_dispensary_info <- all_dispensary_info %>%
  mutate(date_of_scrape = mdy(scrape_date), 
         yr = year(date_of_scrape),
         mnth = month(date_of_scrape))
  
# A6: REMOVE DUPES, EXPORT DATASET; RE-ARRANGE DATA FOR JOIN ONTO PRICING DATASET ----------------------------------------------------

# REMOVE DUPES 
all_dispensary_info <- all_dispensary_info %>%
  mutate(dupe = duplicated(id_for_merge)) %>%
  filter(dupe == FALSE)

###########Export Merchant Dataset
write_xlsx(all_dispensary_info,glue("{data_path}/datasets/cleaned_merchant_data/{scrape_date}.xlsx"))

############ Re-arrange data for merge onto pricing dataset

df_dispensaries <- all_dispensary_info %>%
  select("id_for_merge",
         "corrected_state",
         "county_name",
         "city",
         "slug",
         "phone_number",
         "lic_type_1",
         "lic_num_1",
         "lic_category_1",
         "lic_type_2",
         "lic_num_2",
         "lic_category_2",
         "lic_type_3",
         "lic_num_3",
         "lic_category_3",
         "lic_type_4",
         "lic_num_4")


#############------------
### SECTION B - Cannabis Price Information (Storefront and Delivery) 

### B1: LOAD IN DISPENSARY (PRICE/ITEM) DETAILS FOR DELIV AND STOREFRONT -----------------------------------------------------

items_sf <- read_csv(glue("{data_path}/scraped_data/{scrape_date}/dispensary_items.csv")) %>%
  rename("retail_id" = "Retail ID") %>%
  mutate(id_for_merge = glue("storefront_{retail_id}") )

items_delivery <- read_csv(glue("{data_path}/scraped_data/{scrape_date}/delivery_items.csv")) %>%
  rename("retail_id" = "Retail ID") %>%
  mutate(id_for_merge = glue("storefront_{retail_id}") )

all_item_info <- rbind(items_sf, items_delivery)

rm(items_sf,
   items_delivery)


### B2: JOIN ON USA MERCHANT DATA ONTO PRICING DATA  -----------------------------------------------------

df_retail_items <- all_item_info %>%
  left_join(.,
            df_dispensaries, 
            by= "id_for_merge")

rm(all_item_info)

### B3: DROP IRRELEVANT VARIABLES, REORGANIZE DATASET  -----------------------------------------------------

df_retail_items <- df_retail_items %>%
  mutate(scrape_date) %>%
  select("scrape_date",
         "id_for_merge",
         "corrected_state",
         "county_name",
         "city",
         "slug",
         "phone_number",
         "Delivery",
         "License",
         "Rating",
         "Name",
         "Category Name",
         "Price",
         "Grams",
         "Ounces",
         "Grams Per Eighth",
         "lic_type_1",
         "lic_num_1",
         "lic_category_1",
         "lic_type_2",
         "lic_num_2",
         "lic_category_2",
         "lic_type_3",
         "lic_num_3",
         "lic_category_3",
         "lic_type_4",
         "lic_num_4")

df_retail_items <- df_retail_items %>%
  rename("delivery" = "Delivery",
         "license" = "License",
         "rating" = "Rating",
         "strain_name" = "Name",
         "category_name" = "Category Name",
         "price" = "Price",
         "grams" = "Grams",
         "ounces" = "Ounces",
         "grams_per_eigth" = "Grams Per Eighth")

### B4: Adjust Package Sizes -----------------------------------------------------

############## No quantity available in observations (package size information)
df_retail_items <- df_retail_items %>%
  mutate(no_qty_info_flag = ifelse(is.na(grams) & is.na(ounces),
                                   TRUE,
                                   FALSE))


############## Correct The "Grams Per Eighth" measurement for strains across
############## each retailer (e.g. is "eighth" of OG Kush from AA Dispensary
############## 3.5 grams? 4 grams? 5? based on some special deal????)

#Determine unique grams-per-eighth at a retailer-product level
grams_per_eigth_prod_level_all <- df_retail_items %>%
  filter(category_name %in% c("Indica", "Hybrid", "Sativa" ),
         !is.na(grams_per_eigth)) %>%
  group_by(slug, city, strain_name, category_name, grams_per_eigth) %>%
  summarize() %>%
  ungroup() %>%
  mutate(id_for_dupe = glue("{slug}_{city}_{strain_name}_{category_name}"), 
         dupe = duplicated(id_for_dupe))

#Remove duplicate products
grams_per_eigth_prod_level_ready_for_merge <- grams_per_eigth_prod_level_all %>%
  filter(dupe == FALSE) %>%
  select(-id_for_dupe,-dupe) %>%
  rename("corrected_grams_per_eigth" = "grams_per_eigth")

#Join back correct grams-per-eights, and correct those without a merge (those with NA we correct to 3.5g per eighth)
df_retail_items <- df_retail_items %>%
  left_join(., grams_per_eigth_prod_level_ready_for_merge, by = c( "slug", "city", "strain_name", "category_name" )) %>%
  mutate(grams_per_eigth = ifelse(is.na(grams_per_eigth) & (category_name %in% c("Indica", "Hybrid", "Sativa" )) ,
                                  corrected_grams_per_eigth,
                                  grams_per_eigth
  )) %>%
  select(-corrected_grams_per_eigth)


############## Correct Ounces Variables and Grams Variable

df_retail_items <- df_retail_items %>%
  mutate(ounces = case_when( ounces == "1/8" ~ 0.125 ,
                             ounces == "1/4" ~ 0.25 ,
                             ounces == "1/2" ~ 0.5 ,
                             ounces == "1" ~ 1 ,
                             FALSE ~ NaN),
         grams = case_when( grams == "1" ~ 1.0 ,
                            grams == "2" ~ 2.0 ,
                            grams == "1/2" ~ 0.5 ,
                            FALSE ~ NaN)
  )

############## Unify Gram Variable

# For those with n/a in the "grams" field, but a numerical value in "ounces" field, we
# convert ounces to grams. For all other observations, accept grams as true.

df_retail_items <- df_retail_items %>%
  mutate(fixed_grams = case_when( is.na(grams) & !is.na(ounces) ~ 28.35 * ounces,
                                  !is.na(grams) & is.na(ounces) ~ grams,
                                  is.na(grams) & ounces == 0.125 & grams_per_eigth != 3.5 ~ grams_per_eigth,
                                  FALSE ~ NaN
  ))




}
