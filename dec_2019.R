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
# install.packages("haven")

library("tigris")
library('devtools')
library('zipcodeR')
library("tidyverse")
library("glue")
library("readxl")
library(ggplot2)
library("writexl")
library(lubridate)
library(haven)

# STANDARDIZE AND ADD COUNTY-MAPPING TO MERCHANT DETAILS ACROSS USA (USING LAT/LON)-----------------------------------------------------

########################### A2.i - Map all US ZIP codes to county subdivisions (subs)

#Download all state names 
all_states <- state.abb

#Start with first Alphabetical state by creating dataframe(Alabama)
df_county_subs <- county_subdivisions(state = "AL",
                                      cb = TRUE,
                                      class = "sf")


######### Loop through remaining 49 and add to df, making a full dataset of all counties and their geospatial boundaries
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



# [BEGIN DATA BUILD] Start For-Loop to create datasets for all scrape dates --------------------------------------

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



# A2: See Lines of Code before Loop Begins for County-Mapping

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
         everything()) 

# A4: RENAME RELVEANT VARIABLES,-----------------------------------------------------

########## Rename Variables
all_dispensary_info <- all_dispensary_info %>%
  rename("reviews_count" = "Reviews Count",
         "phone_number" = "Phone Number",
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
         "reviews_count",
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
         "lic_num_4",
         "lic_category_4")


### SECTION B - Load in Retail Item information (i.e. Prices) and Join on Merchant Info ------------
### SECTION B - Cannabis Price Information (Storefront and Delivery) 

### B1: LOAD IN DISPENSARY (PRICE/ITEM) DETAILS FOR DELIV AND STOREFRONT -----------------------------------------------------

items_sf <- read_csv(glue("{data_path}/scraped_data/{scrape_date}/dispensary_items.csv")) %>%
  rename("retail_id" = "Retail ID") %>%
  mutate(id_for_merge = glue("storefront_{retail_id}") )

items_delivery <- read_csv(glue("{data_path}/scraped_data/{scrape_date}/delivery_items.csv")) %>%
  rename("retail_id" = "Retail ID") %>%
  mutate(id_for_merge = glue("delivery_{retail_id}") )

all_item_info <- rbind(items_sf, items_delivery) %>%
  mutate(id_for_merge = as.character(id_for_merge))

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
         "reviews_count",
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
         "lic_num_4",
         "lic_category_4")

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


############## Create Flags for Better Categorization of flower and Oils

cartridges <- c("cart", "carts", "cartridge", "cartridges","dosist", "dosist™")
cartridges <- glue_collapse(cartridges, sep = "\\b|\\b")

concentrates <- c("concentrate", "concentrated", "concentrates", "resin", "kief", "keef", "crumble", "wax", "dab", "dabs")
concentrates <- glue_collapse(concentrates, sep = "\\b|\\b")

dosist <- c("dosist", "dosist™")
dosist <- glue_collapse(dosist, sep = "\\b|\\b")

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
         dosist_flag = str_detect(strain_name, regex(dosist, ignore_case = TRUE)),
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
                  "flower",
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

### B6: Text Search "strain_name" to manually search for "dosist" (specifc product) grams and correct  -----------------------------------------------------

df_retail_items <- df_retail_items %>%
  mutate(dosist_dose_amount = ifelse(dosist_flag == TRUE, parse_number(strain_name), NaN ),
         fixed_grams = case_when(dosist_flag == TRUE & (dosist_dose_amount == 50 | is.na(dosist_dose_amount)) ~ 0.1125,
                                 dosist_flag == TRUE & dosist_dose_amount == 200 ~ 0.45,
                                 (dosist_flag == FALSE | is.na(dosist_flag)) ~ fixed_grams,
                                 FALSE ~ NaN)) 

### B7: Refine "Delivery" and "Phone Number" variable, Create Price Per Gram Variable  -----------------------------------------------------

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

### B8: Add more flags to clean data using the PPG variable  -----------------------------------------------------

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
  mutate(flower_error_flag_upper_bound = ifelse((price >= 888.80 & category_name == "flower"),
                                                TRUE,
                                                FALSE)
  )

############# To lessen data noise and exclude observations with faulty scrape data, 
############# flower observations should be removed with grams < 1 (aka 0.5g). These are most commonly oils, or even sample sizes.

df_retail_items <- df_retail_items %>%
  mutate(fixed_grams_0.5_flower_flag = ifelse((fixed_grams == 0.5 & category_name == "flower"),
                                              TRUE,
                                              FALSE)
  )

############# To lessen data noise and exclude observations with faulty scrape data, 
############# price ceilings and floors have been set for flower and oil (agreed upon by Robin G. and Raffaele S.)

df_retail_items <- df_retail_items %>%
  mutate(flower_bound_flag  = ifelse(((ppg <= 100 & ppg > 1 )& category_name == "flower"),
                                     TRUE,
                                     FALSE),
         oil_bound_flag  = ifelse(((ppg <= 300 & ppg > 3 )& category_name == "oil"),
                                  TRUE,
                                  FALSE)
  )




### B9: Add flags to signify retailer type (e.g. delivery, storefront, etc.)  -----------------------------------------------------


############ Create flag for entries without Licenses (No item-ID match from all dispensary license dataset)
df_retail_items <- df_retail_items %>%
mutate(no_lic_match_flag = ifelse(is.na(lic_type_1),
                                  TRUE,
                                  FALSE))
       
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
  

############ Add License Type Flags and create flags on these new license confirmations
df_retail_items <- df_retail_items %>%
  mutate(flag_lic_type_OTHER = ifelse(no_lic_match_flag == TRUE,
                                      FALSE,
                                      ifelse((lic_category_1 == "Other License") & 
                                               (lic_type_1 %in% c("Adult-Use Cultivation",
                                                                  "Adult-Use Nonstorefront",
                                                                  "Adult-Use Retail",
                                                                  "Distributor",
                                                                  "Medical Nonstorefront",
                                                                  "Medical Retail",
                                                                  "Microbusiness")),
                                             TRUE,
                                             FALSE)),
         flag_lic_type_TEMP = ifelse(no_lic_match_flag == TRUE, FALSE,
                                     ifelse((lic_category_1 == "TEMP" |
                                               lic_category_2 == "TEMP" |
                                               lic_category_3 == "TEMP" |
                                               lic_category_4 == "TEMP"  ),
                                            TRUE,
                                            FALSE)),
         flag_lic_type_LIC = ifelse(no_lic_match_flag == TRUE, FALSE,
                                    ifelse((lic_category_1 == "LIC" |
                                              lic_category_2 == "LIC" |
                                              lic_category_3 == "LIC" |
                                              lic_category_4 == "LIC"  ),
                                           TRUE,
                                           FALSE)),
         any_license = ifelse(flag_lic_type_LIC == TRUE |
                              flag_lic_type_TEMP == TRUE | 
                              flag_lic_type_OTHER == TRUE,
                              TRUE,
                              FALSE)
  )

### B10: Add flag to "duplicate" observations from merchants with more than one listing (typically delivery outfitters)  -----------------------------------------------------

# This is based on identical phone numbers, strain name, quantity,and flower/oil category, 
df_retail_items <- df_retail_items %>%
  mutate(id_with_phone_num_match = glue("{phone_number}_{strain_name}_{fixed_grams}_{category_name}"),
         dupe_by_phone_num = duplicated(id_with_phone_num_match)) 

### B11: Create Tidy Variables for Table  -----------------------------------------------------

df_retail_items <- df_retail_items %>%
  mutate(med_or_adult_use = ifelse(flag_any_medical == TRUE, "medical", "recreational"),
         deliv_or_storefront = ifelse(flag_any_delivery == TRUE, "delivery", "storefront"),
         license_status = ifelse(any_license == TRUE, "licensed", "unlicensed"),
         license_type = case_when(flag_lic_type_LIC == TRUE & flag_lic_type_TEMP == TRUE ~ "LIC",
                                  flag_lic_type_LIC == TRUE & flag_lic_type_TEMP == FALSE ~ "LIC",
                                  flag_lic_type_LIC == FALSE & flag_lic_type_TEMP == TRUE ~ "TEMP",
                                  flag_lic_type_OTHER == TRUE & flag_lic_type_LIC == FALSE & flag_lic_type_TEMP == FALSE ~ "Other License",
                                  flag_lic_type_OTHER == FALSE  & flag_lic_type_LIC == FALSE & flag_lic_type_TEMP == FALSE ~ "No License",
                                  TRUE ~ NA_character_)
  )

df_retail_items <- df_retail_items %>%
  select(scrape_date,
         id_for_merge,
         corrected_state,
         county_name,
         city,
         slug,
         phone_number,
         cleaned_phone_number,
         deliv_or_storefront,
         delivery,
         license_type,
         license_status,
         med_or_adult_use,
         license,
         rating,
         reviews_count,
         strain_name,
         category_name,
         price,
         grams,
         ounces,
         grams_per_eigth,
         fixed_grams,
         ppg,
         no_qty_info_flag,
         dosist_dose_amount,
         everything())

### B10[FILTER]----------------------------------------------------

df_retail_items <- df_retail_items %>%
  filter(#Remove non US Entries
         !is.na(corrected_state),
         #Remove observations without quantity information (e.g. grams, ounces)
         (no_qty_info_flag == FALSE | (!is.na(fixed_grams))),
         #Remove observations without odd symbols any indications that they may be noisy data
         all_flag == FALSE,
         #Remove observations that have been scraped as "oil" or "flower" but are so low/highly priced that its likely a listing eror
         (flower_bound_flag == TRUE | oil_bound_flag == TRUE),
         #Remove flower observations that are a 1/2 gram (only a sample, not an actual sold quantity)
         fixed_grams_0.5_flower_flag == FALSE,
         #Remove any merchants without any reviews (these are noisy listings)
         (!(reviews_count == 0 | is.na(reviews_count))),
         #Narrow down dataset only to oil and flower entries
         category_name %in% c("flower", "oil"),
         #Remove shake/trim/stem observations
         shake_trim_stem_flag == FALSE
         )

### B11 - Remove flags used for filtering and others----------------------------------------------------

df_retail_items <- df_retail_items %>%
  select(-exclamation_flag,
         -at_sign_flag,
         -asterix_flag,
         -keyword_flag,
         -all_flag,
         -cart_flag,
         -concentrate_flag,
         -pen_pod_flag,
         -shatter_moonrock_flag,
         -tincture_flag,
         -liquid_oil_flag,
         -shake_trim_stem_flag,
         -preroll_flag,
         -upper_lower_bound_flag,
         -flower_error_flag_upper_bound,
         -fixed_grams_0.5_flower_flag,
         -flower_bound_flag,
         -oil_bound_flag,
         # -flag_adult_retail,
         # -flag_med_retail,
         # -flag_hybrid_non_sf,
         # -flag_hybrid_retail,
         # -flag_adult_non_sf,
         # -flag_adult_retail,
         # -flag_medical_non_sf,
         # -flag_med_retail,
         -flag_microbiz,
         -flag_other_biz,
         -id_with_phone_num_match
         )
  

### B12[EXPORT]: Export Final Scrape Date Dataset Build of all Deliv/Storefront Prices  -----------------------------------------------------
write_csv(df_retail_items, glue("{data_path}/datasets/cleaned_price_data/{scrape_date}.csv"))

#remove all irrelevant datasets 
rm(all_dispensary_info,
   df_county_subs,
   df_dispensaries)


### SECTION C - Remove duplicates and create new dataset without dupes ------------
deduped_df_retail_items <- df_retail_items %>%
  filter(dupe_by_phone_num == FALSE)

## SECTION D - Generate Summary Statistics for Export (Across all merchants) ------------

########## Create For-Loop to run for both datasets (with dupes, without dupes), and for multiple products
df_list <- list(df_retail_items,
                deduped_df_retail_items)

all_categories <- c("flower",
                    "oil")

#Loop through retail dataset and de-duped retail dataset
for (retail_dataset in df_list) {
####NOTE TO RAFF <- YOU HAVNET INTEGRATED THE DEDUPE INTO THE LOOP UYET (6/16, 8AM

  #Loop through both flower and oil
  for (category in all_categories) {
      
        # Create variable for name of dataset
        dupe_type <- ifelse(nrow(retail_dataset) == nrow(df_retail_items),
                            "wdupe",
                            "deduped")
    
        ### QUANTITY LEVEL STATS (SEPARATED BY FIXED GRAMS)
        #Lic vs. Unlic
        A_qty_lvl_ss <- retail_dataset %>%
        filter(category_name == category) %>%
        group_by(corrected_state, scrape_date, category_name, fixed_grams, license_status) %>%
        summarize(observations = n(),
                 min_price = min(ppg),
                 mean_price = mean(ppg),
                 median_price = median(ppg),
                 max_price = max(ppg),
                 std_dev = sd(ppg)
        )

        write_csv(A_qty_lvl_ss,
                  glue("{data_path}/datasets/summary_stats/{dupe_type}/A_qty_lvl_{category}_{scrape_date}.csv"))
        
        # Deliv vs. Storefront
        B_qty_lvl_ss <- retail_dataset %>%
        filter(category_name == category) %>%
        group_by(corrected_state, scrape_date, category_name, fixed_grams, deliv_or_storefront) %>%
        summarize(observations = n(),
                min_price = min(ppg),
                mean_price = mean(ppg),
                median_price = median(ppg),
                max_price = max(ppg),
                std_dev = sd(ppg)
        )
        
        write_csv(B_qty_lvl_ss,
                  glue("{data_path}/datasets/summary_stats/{dupe_type}/B_qty_lvl_{category}_{scrape_date}.csv"))
        
        
        # Deliv vs. Storefront AND Lic/Unlic
        C_qty_lvl_ss <- retail_dataset %>%
        filter(category_name == category) %>%
        group_by(corrected_state, scrape_date, category_name, fixed_grams, license_status, deliv_or_storefront) %>%
        summarize(observations = n(),
                min_price = min(ppg),
                mean_price = mean(ppg),
                median_price = median(ppg),
                max_price = max(ppg),
                std_dev = sd(ppg)
        )
       
         write_csv(C_qty_lvl_ss,
                  glue("{data_path}/datasets/summary_stats/{dupe_type}/C_qty_lvl_{category}_{scrape_date}.csv"))
        
        
        #### AGGREGATE LEVEL STATS (PPG ACROSS ALL PACKAGE SIZES OF FIXED GRAMS)
        # Lic vs. Unlic
        A_AGG_ss <- retail_dataset %>%
        filter(category_name == category) %>%
        group_by(corrected_state, scrape_date, category_name, license_status) %>%
        summarize(observations = n(),
                min_price = min(ppg),
                mean_price = mean(ppg),
                median_price = median(ppg),
                max_price = max(ppg),
                std_dev = sd(ppg)
        )
        
        write_csv(A_AGG_ss,
                  glue("{data_path}/datasets/summary_stats/{dupe_type}/A_AGG_{category}_{scrape_date}.csv"))
      
        
        # Deliv vs. Storefront
        B_AGG_ss <- retail_dataset %>%
        filter(category_name == category) %>%
        group_by(corrected_state, scrape_date, category_name, deliv_or_storefront) %>%
        summarize(observations = n(),
                min_price = min(ppg),
                mean_price = mean(ppg),
                median_price = median(ppg),
                max_price = max(ppg),
                std_dev = sd(ppg)
        )
       
        write_csv(B_AGG_ss,
                  glue("{data_path}/datasets/summary_stats/{dupe_type}/B_AGG_{category}_{scrape_date}.csv"))
        
        
        # Deliv vs. Storefront AND Lic/Unlic
        C_AGG_ss <- retail_dataset %>%
        filter(category_name == category) %>%
        group_by(corrected_state, scrape_date, category_name, license_status, deliv_or_storefront) %>%
        summarize(observations = n(),
                min_price = min(ppg),
                mean_price = mean(ppg),
                median_price = median(ppg),
                max_price = max(ppg),
                std_dev = sd(ppg)
        )
        write_csv(C_AGG_ss,
                  glue("{data_path}/datasets/summary_stats/{dupe_type}/C_AGG_{category}_{scrape_date}.csv"))
        

        #### MERCHANT LEVEL STATS (PPG ACROSS ALL PACKAGE SIZES OF FIXED GRAMS)
        
        #Merchant Level Prices, QUANTITY Level 
        D_mlp_qty_lvl_ss <- retail_dataset %>%
          filter(category_name == category) %>%
          group_by(corrected_state, scrape_date, slug, phone_number, category_name, fixed_grams, license_status, deliv_or_storefront) %>%
          summarize(observations = n(),
                    min_price = min(ppg),
                    mean_price = mean(ppg),
                    median_price = median(ppg),
                    max_price = max(ppg),
                    std_dev = sd(ppg))
        
        write_csv(D_mlp_qty_lvl_ss,
                  glue("{data_path}/datasets/summary_stats/{dupe_type}/D_mlp_qty_lvl_{category}_{scrape_date}.csv"))
        
        #Merchant Level Prices, AGGREGATE Level 
        D_mlp_AGG_ss <- retail_dataset %>%
          filter(category_name == category) %>%
          group_by(corrected_state, scrape_date, slug, phone_number, category_name, license_status, deliv_or_storefront) %>%
          summarize(observations = n(),
                    min_price = min(ppg),
                    mean_price = mean(ppg),
                    median_price = median(ppg),
                    max_price = max(ppg),
                    std_dev = sd(ppg))
        write_csv(D_mlp_AGG_ss,
                  glue("{data_path}/datasets/summary_stats/{dupe_type}/D_mlp_AGG_{category}_{scrape_date}.csv"))
        
        #Summary of the merchant-level summaries (qty level)
        E_sum_of_mchts_qty_lvl_ss <- D_mlp_qty_lvl_ss %>%
          group_by(corrected_state, scrape_date, category_name, fixed_grams) %>%
          summarize(observations = n(),
                    min_mean = mean(min_price),
                    min_median = median(min_price),
                    mean_mean = mean(mean_price),
                    mean_median = median(mean_price),
                    max_mean = mean(max_price),
                    max_median = median(max_price)
          )
        write_csv(E_sum_of_mchts_qty_lvl_ss,
                  glue("{data_path}/datasets/summary_stats/{dupe_type}/E_sum_of_mchts_qty_lvl_{category}_{scrape_date}.csv"))
        
        #Summary of the merchant-level summaries (Aggregate level)
        E_sum_of_mchts_AGG_ss <- D_mlp_AGG_ss %>%
          group_by(corrected_state, scrape_date, category_name) %>%
          summarize(observations = n(),
                    min_mean = mean(min_price),
                    min_median = median(min_price),
                    mean_mean = mean(mean_price),
                    mean_median = median(mean_price),
                    max_mean = mean(max_price),
                    max_median = median(max_price)
          )
        write_csv(E_sum_of_mchts_AGG_ss,
                  glue("{data_path}/datasets/summary_stats/{dupe_type}/E_sum_of_mchts_AGG_{category}_{scrape_date}.csv"))
        
        
       }

    }

rm(df_list)
rm()
}
