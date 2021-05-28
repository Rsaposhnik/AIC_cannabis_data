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

rm(grams_per_eigth_prod_level_all)
#Join back correct grams-per-eights, and correct those without a merge (those with NA we correct to 3.5g per eighth)
df_retail_items <- df_retail_items %>%
  left_join(., grams_per_eigth_prod_level_ready_for_merge, by = c( "slug", "city", "strain_name", "category_name" )) %>%
  mutate(grams_per_eigth = ifelse(is.na(grams_per_eigth) & (category_name %in% c("Indica", "Hybrid", "Sativa" )) ,
                                  corrected_grams_per_eigth,
                                  grams_per_eigth
  )) %>%
  select(-corrected_grams_per_eigth)

rm(grams_per_eigth_prod_level_ready_for_merge)

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





### B5: Add Flags to DataSet in order to (1) clean noisy data; and (2) refine categorizations of flower and concentrates  -----------------------------------------------------

############## Create Flags for Irrelevant Keywords & Symbols

# Create Flags based off keywords and symbols that seem to be linked towards
# noisy data (first time specials, promotions, free products, etc.)

irrelevant_key_words <- c("follow", "call", "text", "sample", "menu", "refer", "delivery", "upgrade", "review")
irrelevant_key_words <- glue_collapse(irrelevant_key_words, sep = "\\b|\\b")

df_retail_items <- df_retail_items %>%
  mutate(exclamation_flag = str_detect(strain_name, regex("\\!", ignore_case = TRUE)),
         at_sign_flag = str_detect(strain_name, regex("\\@", ignore_case = TRUE)),
         asterix_flag = str_detect(strain_name, regex("\\*", ignore_case = TRUE)),
         keyword_flag = str_detect(strain_name, regex(irrelevant_key_words, ignore_case = TRUE)),
         all_flag = ifelse(exclamation_flag == TRUE | at_sign_flag == TRUE | asterix_flag == TRUE | keyword_flag == TRUE,
                           TRUE,
                           FALSE))


############## Create Flags for Better Categorization of Flower and Oils

cartridges <- c("cart", "carts", "cartridge", "cartridges","dosist", "dosist™")
cartridges <- glue_collapse(cartridges, sep = "\\b|\\b")

concentrates <- c("concentrate", "concentrated", "concentrates", "resin", "kief", "keef", "crumble", "wax", "dab", "dabs")
concentrates <- glue_collapse(concentrates, sep = "\\b|\\b")

pen_pod <- c("pen", "pod", "pods", "vape" , "tank")
pen_pod <- glue_collapse(pen_pod, sep = "\\b|\\b")

oil <- c("oil", "oil", "E-Liquid", "syringe", "refill", "prefilled", "plug exotics", "plug dna", "stiizy", "jetty", "disposable")
oil <- glue_collapse(oil, sep = "\\b|\\b")

shatter_moonrock <- c("shatter", "moon rock", "moon-rock", "moon rocks", "moon-rocks", "moonrocks", "moonrock")
shatter_moonrock <- glue_collapse(shatter_moonrock, sep = "\\b|\\b")

tincture <- c("tincture", "tinctures")
tincture <- glue_collapse(tincture, sep = "\\b|\\b")

df_retail_items <- df_retail_items %>%
  mutate(cart_flag = str_detect(strain_name, regex(cartridges, ignore_case = TRUE)),
         concentrate_flag = str_detect(strain_name, regex(concentrates, ignore_case = TRUE)),
         pen_pod_flag = str_detect(strain_name, regex(pen_pod, ignore_case = TRUE)),
         shatter_moonrock_flag = str_detect(strain_name, regex(shatter_moonrock, ignore_case = TRUE)),
         tincture_flag = str_detect(strain_name, regex(tincture, ignore_case = TRUE)),
         liquid_oil_flag = str_detect(strain_name, regex(oil, ignore_case = TRUE))
  )

# Re-code cartridges, concentrates, and pen/pods to concentrates that may have been miscategorized as flower
# Re-code tinctures as tinctures (not concentrates)

df_retail_items <- df_retail_items %>%
  mutate(category_name =
           ifelse(category_name %in% c("Hybrid", "Sativa", "Indica"),
                  "Flower",
                  category_name),
         category_name =
           ifelse(concentrate_flag == TRUE | shatter_moonrock_flag == TRUE,
                  "Concentrate",
                  category_name),
         category_name =
           ifelse((cart_flag == TRUE | pen_pod_flag == TRUE | liquid_oil_flag == TRUE) & (fixed_grams >= 0.5 & fixed_grams <= 2) ,
                  "oil",
                  category_name),
         category_name = 
           ifelse(tincture_flag == TRUE,
                  "Tincture",
                  category_name)
  )


######## Add flags for undesirable flower products

shake_trim_stem <- c("shake", "trim", "stem")
shake_trim_stem <- glue_collapse(shake_trim_stem, sep = "\\b|\\b")

pre_roll <- c("Pre-Roll", "Preroll", "Pre Roll", "Prerolled")
pre_roll <- glue_collapse(pre_roll, sep = "\\b|\\b")

df_retail_items <- df_retail_items %>%
  mutate(shake_trim_stem_flag = str_detect(strain_name, regex(shake_trim_stem, ignore_case = TRUE)),
         preroll_flag = str_detect(strain_name, regex(pre_roll, ignore_case = TRUE))
  )



### B6: Refine "Delivery" and "Phone Number" variable, Create Price Per Gram Variable  -----------------------------------------------------

#Change Delivery Variable from Boolean to Descriptive
df_retail_items <- df_retail_items %>%
  mutate(delivery = ifelse(delivery == 1, "delivery", "storefront"))

#Clean Phone number Variable
df_retail_items <- df_retail_items %>%
  mutate(cleaned_phone_number = gsub(" ", "", (gsub("[^A-Za-z0-9 ]", "", phone_number))))

############## Create Price per Gram (PPG) Variable 
df_retail_items <- df_retail_items %>%
  mutate(ppg = price/fixed_grams
  )

### B7: Add more flags to clean data using the PPG variable  -----------------------------------------------------

############# To lessen data noise and exclude observations with faulty scrape data, 
############# observations should be removed if they have a with PPG over $135 and under $0.35

df_retail_items <- df_retail_items %>%
  mutate(upper_lower_bound_flag = ifelse(ppg <= 135 & ppg >= 0.35,
                                         FALSE,
                                         TRUE)
  )


############# To lessen data noise and exclude observations with faulty scrape data, 
############# flower observations should be removed with a price greater than or equal to $888.80

df_retail_items <- df_retail_items %>%
  mutate(flower_error_flag_upper_bound = ifelse((price >= 888.80 & category_name == "Flower"),
                                                TRUE,
                                                FALSE)
  )

############# To lessen data noise and exclude observations with faulty scrape data, 
############# flower observations should be removed with grams < 1 (aka 0.5g). These are most commonly oils, or even sample sizes.

df_retail_items <- df_retail_items %>%
  mutate(fixed_grams_0.5_flower_flag = ifelse((fixed_grams == 0.5 & category_name == "Flower"),
                                              TRUE,
                                              FALSE)
  )

############# To lessen data noise and exclude observations with faulty scrape data, 
############# price ceilings and floors have been set for flower and oil (agreed upon by Robin G. and Raffaele S.)

df_retail_items <- df_retail_items %>%
  mutate(flower_bound_flag  = ifelse(((ppg <= 100 & ppg > 1 )& category_name == "Flower"),
                                     TRUE,
                                     FALSE),
         oil_bound_flag  = ifelse(((ppg <= 300 & ppg > 3 )& category_name == "oil"),
                                  TRUE,
                                  FALSE)
  )
















### B8: Add flags to signify retailer type (e.g. delivery, storefront, etc.)  -----------------------------------------------------


############## Create Adult/Medical SF/NonSF Dummy Variables
df_retail_items <- df_retail_items %>%
  mutate(flag_adult_retail = ifelse(delivery == "storefront" & license == "recreational",
                                    TRUE,
                                    FALSE),
         flag_med_retail = ifelse(delivery == "storefront" & license == "medical",
                                  TRUE,
                                  FALSE),
         flag_hybrid_retail = ifelse(delivery == "storefront" & license == "hybrid",
                                     TRUE,
                                     FALSE),
         flag_adult_non_sf = ifelse(delivery == "delivery" & license == "recreational",
                                    TRUE,
                                    FALSE),
         flag_medical_non_sf = ifelse(delivery == "delivery" & license == "medical",
                                      TRUE,
                                      FALSE),
         flag_hybrid_non_sf = ifelse(delivery == "delivery" & license == "hybrid",
                                     TRUE,
                                     FALSE),
         flag_any_medical = ifelse((flag_med_retail == TRUE |
                                      flag_medical_non_sf == TRUE ),
                                   TRUE,
                                   FALSE),
         flag_any_delivery = ifelse((flag_adult_non_sf == TRUE |
                                       flag_medical_non_sf == TRUE |
                                       flag_hybrid_non_sf == TRUE),
                                    TRUE,
                                    FALSE),
         flag_microbiz = ifelse(no_lic_match_flag == TRUE, FALSE,
                                ifelse(
                                  (lic_type_1 == "Microbusiness" |  
                                     lic_type_2 == "Microbusiness" |
                                     lic_type_3 == "Microbusiness" |
                                     lic_type_4 == "Microbusiness"  ),
                                  TRUE,
                                  FALSE)),
         flag_other_biz = ifelse(no_lic_match_flag == TRUE, FALSE,
                                 ifelse(
                                   (lic_type_1 %in% c("Distributor", "Medical Cultivation", "Adult-Use Cultivation", "Adult-Use Mfg.", "Event" ) |
                                      lic_type_2 %in% c("Distributor", "Medical Cultivation", "Adult-Use Cultivation", "Adult-Use Mfg.", "Event" ) |
                                      lic_type_3 %in% c("Distributor", "Medical Cultivation", "Adult-Use Cultivation", "Adult-Use Mfg.", "Event" ) |
                                      lic_type_4 %in% c("Distributor", "Medical Cultivation", "Adult-Use Cultivation", "Adult-Use Mfg.", "Event" )  ),
                                   TRUE,
                                   FALSE))
  )

# 
# ############ Add License Type Flags and create flags on these new license confirmations
# 
# All_df <- All_df %>%
#   mutate(flag_lic_type_LIC = ifelse(no_lic_match_flag == TRUE, FALSE,
#                                     ifelse((lic_category_1 == "LIC" |
#                                               lic_category_2 == "LIC" |
#                                               lic_category_3 == "LIC" |
#                                               lic_category_4 == "LIC"  ),
#                                            TRUE,
#                                            FALSE)),
#          flag_lic_type_TEMP = ifelse(no_lic_match_flag == TRUE, FALSE,
#                                      ifelse((lic_category_1 == "TEMP" |
#                                                lic_category_2 == "TEMP" |
#                                                lic_category_3 == "TEMP" |
#                                                lic_category_4 == "TEMP"  ),
#                                             TRUE,
#                                             FALSE)),
#          LIC_or_TEMP = ifelse(flag_lic_type_LIC == TRUE | flag_lic_type_TEMP == TRUE, 
#                               TRUE, 
#                               FALSE),
#          no_license = ifelse(flag_lic_type_LIC == TRUE | flag_lic_type_TEMP == TRUE, 
#                              FALSE, 
#                              TRUE)
#   )
# 
# 
# 







}
