# check if ddl_judge_id is selective

#### notes ####
# a lot of missing data on judges assigned to each case (both decision and filing)

#### environment setup ####
# set working directory
setwd("/Users/reinakishida/Dropbox/Judicial_Decisions_and_Gender_Norms/analysis")

# load packages
pacman::p_load(data.table, tidyverse)

#### load data ####
judge_case_merge_key <- fread("/Users/reinakishida/Dropbox/Judicial_Decisions_and_Gender_Norms/analysis/data/raw/keys/judge_case_merge_key.csv")
judges_clean <- fread("/Users/reinakishida/Dropbox/Judicial_Decisions_and_Gender_Norms/analysis/data/raw/judges_clean.csv")


#### check unique ddl_judge_id ####
judge_key_unique <- judge_case_merge_key %>% 
  select(ddl_decision_judge_id) %>% 
  distinct() %>% 
  nrow()
# 39764L

judge_clean_unique <- judges_clean %>% 
  select(ddl_judge_id) %>% 
  distinct() %>% 
  nrow()
# 98478L


#### check judge_id = NA ####
# ddl_decision_judge_id=NA 
judge_key_na <- judge_case_merge_key %>% 
  filter(is.na(ddl_decision_judge_id)) 
# 610,441 obs

# check ddl_filing_judge_id=NA
judge_key_filing_na <- judge_case_merge_key %>% 
  filter(is.na(ddl_filing_judge_id)) 
# 1,964,969 obs

# check ddl_judge_id=NA
judge_clean_na <- judges_clean %>% 
  filter(is.na(ddl_judge_id))
# 0 obs


#### Crime cases: construct NA dummy ####



