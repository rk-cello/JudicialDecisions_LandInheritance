# construct case df for years 2010-2014

#### notes ####
# female_defendant variable is character--> should be converted into binary variable

#### environment setup ####
# set working directory
# e.g. setwd("~/Dropbox/Judicial Decisions and Gender Norms/analysis")

# load packages
pacman::p_load(data.table, tidyverse)

#### acts_sections ####
acts_sections <- fread("/Users/reinakishida/Desktop/Judges/csv/acts_sections.csv")

#### construct df ####
year_list <- c(2010:2014)

for (year in year_list) {
  
  cases_year <- fread(paste0("/Users/reinakishida/Desktop/Judges/csv_edit/cases/cases_", year, ".csv"))
  
  df_year <- cases_year %>% 
    left_join(acts_sections, by = "ddl_case_id", suffix = c("", "_merge")) # keeps only cases of that year
  
  df_year <- df_year %>% 
    # filter(criminal == 1) %>% # keeps only criminal cases
    left_join(judge_case_merge_key, by = "ddl_case_id", suffix = c("", "_merge")) %>%
    rename(ddl_judge_id = ddl_decision_judge_id) %>%
    left_join(judges_clean, by = "ddl_judge_id", suffix = c("", "_merge")) %>%
    left_join(act_key, by = "act", suffix = c("", "_key_merge")) %>%
    left_join(section_key, by = "section", suffix = c("", "_key_merge")) %>%
    left_join(purpose_name_key, by = c("purpose_name", "year"), suffix = c("", "_key_merge")) %>%
    left_join(type_name_key, by = c("type_name", "year"), suffix = c("", "_key_merge")) %>%
    left_join(disp_name_key, by = c("disp_name", "year"), suffix = c("", "_key_merge")) %>%
    left_join(cases_state_key, by = c("state_code", "year"), suffix = c("", "_key_merge")) %>%
    left_join(cases_district_key, by = c("state_code", "dist_code", "year"), suffix = c("", "_key_merge")) %>%
    left_join(cases_court_key, by = c("state_code", "dist_code", "court_no", "year"), suffix = c("", "_key_merge"))
  
  # save
  write.table(df_year, paste0("/Users/reinakishida/Desktop/Judges/output/cases_", year, "_merged.csv"), 
              sep = ",", row.names = FALSE, col.names = TRUE, quote = TRUE)
  
  # save the initial inspection results to a text file
  capture.output(str(df_year), file = paste0("/Users/reinakishida/Desktop/Judges/output/txt/inspection_cases_", year, ".txt"))
  capture.output(summary(df_year), file = paste0("/Users/reinakishida/Desktop/Judges/output/txt/summary_cases_", year, ".txt"))  
  
  cases_year <- NULL
  df_year <- NULL
  
}




