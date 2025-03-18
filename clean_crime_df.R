# clean crime case df

#### notes ####
# 

#### environment setup ####
# set working directory
setwd("/Users/reinakishida/Dropbox/Judicial_Decisions_and_Gender_Norms/analysis")

# load packages
pacman::p_load(data.table, tidyverse, lubridate)

#### inspect date columns ####
# visualize
year_list <- c(2010:2018)
date_vars <- c("date_of_filing", "date_of_decision", "date_first_list", 
               "date_last_list", "date_next_list")

for (i in year_list) {
  crime_i <- fread(paste0("data/dev/crime_merged/crime_", i, "_merged.csv"))
  
  for (j in date_vars) {
    crime_i %>% 
      ggplot(aes_string(x = j)) +
      geom_histogram() +
      labs(title = paste0("Histogram of ", j, " in ", i),
           x = j,
           y = "Frequency") +
      theme_minimal()
    
    ggsave(paste0("fig/inspect_date/crime_", i, "_", j, "_hist.png"), bg = "white")
  }
  
  crime_i <- NULL
}

#### clean crime case df ####

for (i in year_list) {
  crime_i <- fread(paste0("data/dev/crime_merged/crime_", i, "_merged.csv"))
  
  # Create dummies for dates falling outside 2010-2018
  for (j in date_vars) {
    crime_i <- crime_i %>% 
      mutate(
        !!sym(j) := as.IDate(!!sym(j)),
        !!sym(paste0(j, "_outside_dummy")) := if_else(!!sym(j) < as.IDate("2010-01-01") | !!sym(j) >= as.IDate("2019-01-01"), 1, 0)
      ) 
      # filter(!!sym(j) < as.IDate("2025-01-01") & !!sym(j) >= as.IDate("1900-01-01"))  # What is the adequate range of dates????
  }
  
  crime_i <- crime_i %>% 
    mutate(
      female_adv_def_dummy = if_else(female_adv_def == 1, 1, if_else(female_adv_def == 0, 0, NA)),
      female_adv_pet_dummy = if_else(female_adv_pet == 1, 1, if_else(female_adv_pet == 0, 0, NA)),
      female_judge_dummy = if_else(female_judge == "1 female", 1, if_else(female_judge == "0 nonfemale", 0, NA)),
    ) %>% 
    select(-c(female_defendant, female_petitioner, female_adv_def, female_adv_pet, female_judge)) 
  
  crime_i <- crime_i %>%
    select(ddl_case_id:court_no, judge_position, ends_with("dummy"), type_name:disp_name, 
           starts_with("date"), act:number_sections_ipc, ddl_filing_judge_id, ddl_judge_id,
           ends_with("date"), state_name, district_name, court_name, 
           type_name_s, purpose_name_s, disp_name_s) %>% 
    # Extract year, month, and day for each date variable
    mutate(across(where(~ inherits(., "IDate")),  
                  list(year = ~ year(.),month = ~ month(.), day = ~ day(.)), 
                  .names = "{.col}_{.fn}"),
           across(ends_with("date"), ~ as.IDate(.))) %>%
    mutate(
      tenure_length = as.numeric(end_date - start_date), # Create judge tenure length variable
      across(where(is.character), ~ as.factor(.)), # Convert character to factor
      across(ends_with("dummy"), ~ as.factor(.)), # Convert dummy variables to factor
      across(c(state_code, dist_code, court_no, type_name, purpose_name, disp_name, act, section), 
             ~ as.factor(.)), # Convert integer (code) to factor
      tenure_length_negative = as.factor(ifelse(tenure_length < 0, 1, 0)) # Create dummy for negative tenure length  
      ) %>% 
    mutate(across(where(is.factor), ~ fct_na_value_to_level(., "Missing"))) %>%   # Replace NA with "Missing"
    select(-c(all_of(date_vars), start_date, end_date))   # Remove original date variables
  
  # save
  write_csv(crime_i, paste0("data/dev/crime_merged/crime_", i, "_merged_clean.csv"))
  rm(crime_i)
}


#### notes: further cleaning ####

for (i in year_list) {
  crime_i <- fread(paste0("data/dev/crime_merged/crime_", i, "_merged_clean.csv")) %>% 
    select(ddl_case_id:court_no, judge_position, ends_with("dummy"), type_name:disp_name, 
           starts_with("date"), act:number_sections_ipc, ddl_filing_judge_id, ddl_judge_id,
           ends_with("date"), state_name, district_name, court_name, 
           type_name_s, purpose_name_s, disp_name_s) %>% 
    # Extract year, month, and day for each date variable
    mutate(across(where(~ inherits(., "IDate")),  
                  list(year = ~ year(.),month = ~ month(.), day = ~ day(.)), 
                  .names = "{.col}_{.fn}"),
           across(ends_with("date"), ~ as.IDate(.))) %>%
    mutate(
      tenure_length = as.numeric(end_date - start_date), # Create judge tenure length variable
      across(where(is.character), ~ as.factor(.)), # Convert character to factor
      across(ends_with("dummy"), ~ as.factor(.)), # Convert dummy variables to factor
      across(c(state_code, dist_code, court_no, type_name, purpose_name, disp_name, act, section), 
             ~ as.factor(.)) # Convert integer (code) to factor
    ) %>% 
    mutate(across(where(is.factor), ~ fct_na_value_to_level(., "Missing"))) %>%   # Replace NA with "Missing"
    select(-c(all_of(date_vars), start_date, end_date)) %>%  # Remove original date variables
    mutate(tenure_length_negative = ifelse(tenure_length < 0, 1, 0))  # Create dummy for negative tenure length
  
  # save
  write_csv(crime_i, paste0("data/dev/crime_merged/crime_", i, "_merged_clean.csv"))
  crime_i <- NULL
}


