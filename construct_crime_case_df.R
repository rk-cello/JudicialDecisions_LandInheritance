# construct criminal case df for years 2010-2018

#### notes ####
# female_defendant variable is character--> should be converted into binary variable

#### environment setup ####
# set working directory
setwd("/Users/reinakishida/Dropbox/Judicial_Decisions_and_Gender_Norms/analysis")

# load packages
pacman::p_load(data.table, tidyverse)

#### acts_sections: limit to criminal cases ####
acts_sections_crime <- fread("/data/raw/acts_sections.csv") %>% 
  filter(criminal == 1)

# output
write.table(acts_sections_crime, "/data/dev/acts_sections_crime.csv", 
            sep = ",", row.names = FALSE, col.names = TRUE, quote = TRUE)

#### load other data ####
judge_case_merge_key <- fread("/data/raw/keys/judge_case_merge_key.csv")
judges_clean <- fread("/data/raw/judges_clean.csv")
act_key <- fread("/data/raw/keys/act_key.csv")
cases_court_key <- fread("/data/raw/keys/cases_court_key.csv")
cases_district_key <- fread("/data/raw/keys/cases_district_key.csv")
cases_state_key <- fread("/data/raw/keys/cases_state_key.csv")
disp_name_key <- fread("/data/raw/keys/disp_name_key.csv")
purpose_name_key <- fread("/data/raw/keys/purpose_name_key.csv")
section_key <- fread("/data/raw/keys/section_key.csv")
type_name_key <- fread("/data/raw/keys/type_name_key.csv")

#### 2010 ####
cases_2010 <- fread("/data/raw/cases/cases_2010.csv")

# convert female_defendant, female_petitioner variables into binary
cases_2010 <- cases_2010 %>% 
  mutate(
    female_def_dummy = if_else(female_defendant == "1 female", 1, if_else(female_defendant == "0 male", 0, NA)),
    female_pet_dummy = if_else(female_petitioner == "1 female", 1, if_else(female_petitioner == "0 male", 0, NA))
  )

# check if all ddl_case_id for year 2010 end with 2010 --> No
check_ddl_case_id <- cases_2010 %>% 
  filter(!str_detect(ddl_case_id, "2010$")) %>% 
  select(ddl_case_id)

# save
write.table(cases_2010, "/data/dev/cases/cases_2010.csv", 
            sep = ",", row.names = FALSE, col.names = TRUE, quote = TRUE)

# merge data
df_2010 <- cases_2010 %>% 
  left_join(acts_sections_crime, by = "ddl_case_id", suffix = c("", "_merge")) # keeps only year 2010 cases

df_2010 <- df_2010 %>% 
  filter(criminal == 1) %>% # keeps only criminal cases
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
write.table(df_2010, "/data/dev/crime_merged/crime_2010_merged.csv", 
            sep = ",", row.names = FALSE, col.names = TRUE, quote = TRUE)

# inspect structure and summary of dataset
str(df_2010)
summary(df_2010)

# save the initial inspection results to a text file
capture.output(str(df_2010), file = paste0("/txt/cases/inspection_crime_", "2010", ".txt"))
capture.output(summary(df_2010), file = paste0("/txt/cases/summary_crime_", "2010", ".txt"))  

cases_2010 <- NULL
df_2010 <- NULL

#### 2011 ####
cases_2011 <- fread("/data/raw/cases/cases_2011.csv")

# convert female_defendant, female_petitioner variables into binary
cases_2011 <- cases_2011 %>% 
  mutate(
    female_def_dummy = if_else(female_defendant == "1 female", 1, if_else(female_defendant == "0 male", 0, NA)),
    female_pet_dummy = if_else(female_petitioner == "1 female", 1, if_else(female_petitioner == "0 male", 0, NA))
  )

# save
write.table(cases_2011, "/data/dev/cases/cases_2011.csv", 
            sep = ",", row.names = FALSE, col.names = TRUE, quote = TRUE)

# merge data
df_2011 <- cases_2011 %>% 
  left_join(acts_sections_crime, by = "ddl_case_id", suffix = c("", "_merge")) # keeps only year 2011 cases

df_2011 <- df_2011 %>% 
  filter(criminal == 1) %>% # keeps only criminal cases
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
write.table(df_2011, "/data/dev/crime_merged/crime_2011_merged.csv", 
            sep = ",", row.names = FALSE, col.names = TRUE, quote = TRUE)

# inspect structure and summary of dataset
str(df_2011)
summary(df_2011)

# save the initial inspection results to a text file
capture.output(str(df_2011), file = paste0("/txt/cases/inspection_crime_", "2011", ".txt"))
capture.output(summary(df_2011), file = paste0("/txt/cases/summary_crime_", "2011", ".txt"))  

cases_2011 <- NULL
df_2011 <- NULL

#### 2012 ####
cases_2012 <- fread("/data/raw/cases/cases_2012.csv")

# convert female_defendant, female_petitioner variables into binary
cases_2012 <- cases_2012 %>% 
  mutate(
    female_def_dummy = if_else(female_defendant == "1 female", 1, if_else(female_defendant == "0 male", 0, NA)),
    female_pet_dummy = if_else(female_petitioner == "1 female", 1, if_else(female_petitioner == "0 male", 0, NA))
  )

# save
write.table(cases_2012, "/data/dev/cases/cases_2012.csv", 
            sep = ",", row.names = FALSE, col.names = TRUE, quote = TRUE)

# merge data
df_2012 <- cases_2012 %>% 
  left_join(acts_sections_crime, by = "ddl_case_id", suffix = c("", "_merge")) # keeps only year 2012 cases

df_2012 <- df_2012 %>% 
  filter(criminal == 1) %>% # keeps only criminal cases
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
write.table(df_2012, "/data/dev/crime_merged/crime_2012_merged.csv", 
            sep = ",", row.names = FALSE, col.names = TRUE, quote = TRUE)

# inspect structure and summary of dataset
str(df_2012)
summary(df_2012)

# save the initial inspection results to a text file
capture.output(str(df_2012), file = paste0("/txt/cases/inspection_crime_", "2012", ".txt"))
capture.output(summary(df_2012), file = paste0("/txt/cases/summary_crime_", "2012", ".txt"))  

cases_2012 <- NULL
df_2012 <- NULL

#### 2013 ####
cases_2013 <- fread("/data/raw/cases/cases_2013.csv")

# convert female_defendant, female_petitioner variables into binary
cases_2013 <- cases_2013 %>% 
  mutate(
    female_def_dummy = if_else(female_defendant == "1 female", 1, if_else(female_defendant == "0 male", 0, NA)),
    female_pet_dummy = if_else(female_petitioner == "1 female", 1, if_else(female_petitioner == "0 male", 0, NA))
  )

# save
write.table(cases_2013, "/data/dev/cases/cases_2013.csv", 
            sep = ",", row.names = FALSE, col.names = TRUE, quote = TRUE)

# merge data
df_2013 <- cases_2013 %>% 
  left_join(acts_sections_crime, by = "ddl_case_id", suffix = c("", "_merge")) # keeps only year 2013 cases

df_2013 <- df_2013 %>% 
  filter(criminal == 1) %>% # keeps only criminal cases
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
write.table(df_2013, "/data/dev/crime_merged/crime_2013_merged.csv", 
            sep = ",", row.names = FALSE, col.names = TRUE, quote = TRUE)

# inspect structure and summary of dataset
str(df_2013)
summary(df_2013)

# save the initial inspection results to a text file
capture.output(str(df_2013), file = paste0("/txt/cases/inspection_crime_", "2013", ".txt"))
capture.output(summary(df_2013), file = paste0("/txt/cases/summary_crime_", "2013", ".txt"))  

cases_2013 <- NULL
df_2013 <- NULL

#### 2014 ####
cases_2014 <- fread("/data/raw/cases/cases_2014.csv")

# convert female_defendant, female_petitioner variables into binary
cases_2014 <- cases_2014 %>% 
  mutate(
    female_def_dummy = if_else(female_defendant == "1 female", 1, if_else(female_defendant == "0 male", 0, NA)),
    female_pet_dummy = if_else(female_petitioner == "1 female", 1, if_else(female_petitioner == "0 male", 0, NA))
  )

# save
write.table(cases_2014, "/data/dev/cases/cases_2014.csv", 
            sep = ",", row.names = FALSE, col.names = TRUE, quote = TRUE)

# merge data
df_2014 <- cases_2014 %>% 
  left_join(acts_sections_crime, by = "ddl_case_id", suffix = c("", "_merge")) # keeps only year 2014 cases

df_2014 <- df_2014 %>% 
  filter(criminal == 1) %>% # keeps only criminal cases
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
write.table(df_2014, "/data/dev/crime_merged/crime_2014_merged.csv", 
            sep = ",", row.names = FALSE, col.names = TRUE, quote = TRUE)

# inspect structure and summary of dataset
str(df_2014)
summary(df_2014)

# save the initial inspection results to a text file
capture.output(str(df_2014), file = paste0("/txt/cases/inspection_crime_", "2014", ".txt"))
capture.output(summary(df_2014), file = paste0("/txt/cases/summary_crime_", "2014", ".txt"))  

cases_2014 <- NULL
df_2014 <- NULL

#### 2015 ####
cases_2015 <- fread("/data/raw/cases/cases_2015.csv")

# convert female_defendant, female_petitioner variables into binary
cases_2015 <- cases_2015 %>% 
  mutate(
    female_def_dummy = if_else(female_defendant == "1 female", 1, if_else(female_defendant == "0 male", 0, NA)),
    female_pet_dummy = if_else(female_petitioner == "1 female", 1, if_else(female_petitioner == "0 male", 0, NA))
  )

# save
write.table(cases_2015, "/data/dev/cases/cases_2015.csv", 
            sep = ",", row.names = FALSE, col.names = TRUE, quote = TRUE)

# merge data
df_2015 <- cases_2015 %>% 
  left_join(acts_sections_crime, by = "ddl_case_id", suffix = c("", "_merge")) # keeps only year 2015 cases

df_2015 <- df_2015 %>% 
  filter(criminal == 1) %>% # keeps only criminal cases
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
write.table(df_2015, "/data/dev/crime_merged/crime_2015_merged.csv", 
            sep = ",", row.names = FALSE, col.names = TRUE, quote = TRUE)

# inspect structure and summary of dataset
str(df_2015)
summary(df_2015)

# save the initial inspection results to a text file
capture.output(str(df_2015), file = paste0("/txt/cases/inspection_crime_", "2015", ".txt"))
capture.output(summary(df_2015), file = paste0("/txt/cases/summary_crime_", "2015", ".txt"))  

cases_2015 <- NULL
df_2015 <- NULL

#### 2016 ####
cases_2016 <- fread("/data/raw/cases/cases_2016.csv")

# convert female_defendant, female_petitioner variables into binary
cases_2016 <- cases_2016 %>% 
  mutate(
    female_def_dummy = if_else(female_defendant == "1 female", 1, if_else(female_defendant == "0 male", 0, NA)),
    female_pet_dummy = if_else(female_petitioner == "1 female", 1, if_else(female_petitioner == "0 male", 0, NA))
  )

# save
write.table(cases_2016, "/data/dev/cases/cases_2016.csv", 
            sep = ",", row.names = FALSE, col.names = TRUE, quote = TRUE)

# merge data
df_2016 <- cases_2016 %>% 
  left_join(acts_sections_crime, by = "ddl_case_id", suffix = c("", "_merge")) # keeps only year 2016 cases

df_2016 <- df_2016 %>% 
  filter(criminal == 1) %>% # keeps only criminal cases
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
write.table(df_2016, "/data/dev/crime_merged/crime_2016_merged.csv", 
            sep = ",", row.names = FALSE, col.names = TRUE, quote = TRUE)

# inspect structure and summary of dataset
str(df_2016)
summary(df_2016)

# save the initial inspection results to a text file
capture.output(str(df_2016), file = paste0("/txt/cases/inspection_crime_", "2016", ".txt"))
capture.output(summary(df_2016), file = paste0("/txt/cases/summary_crime_", "2016", ".txt"))  

cases_2016 <- NULL
df_2016 <- NULL

#### 2017 ####
cases_2017 <- fread("/data/raw/cases/cases_2017.csv")

# convert female_defendant, female_petitioner variables into binary
cases_2017 <- cases_2017 %>% 
  mutate(
    female_def_dummy = if_else(female_defendant == "1 female", 1, if_else(female_defendant == "0 male", 0, NA)),
    female_pet_dummy = if_else(female_petitioner == "1 female", 1, if_else(female_petitioner == "0 male", 0, NA))
  )

# save
write.table(cases_2017, "/data/dev/cases/cases_2017.csv", 
            sep = ",", row.names = FALSE, col.names = TRUE, quote = TRUE)

# merge data
df_2017 <- cases_2017 %>% 
  left_join(acts_sections_crime, by = "ddl_case_id", suffix = c("", "_merge")) # keeps only year 2017 cases

df_2017 <- df_2017 %>% 
  filter(criminal == 1) %>% # keeps only criminal cases
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
write.table(df_2017, "/data/dev/crime_merged/crime_2017_merged.csv", 
            sep = ",", row.names = FALSE, col.names = TRUE, quote = TRUE)

# inspect structure and summary of dataset
str(df_2017)
summary(df_2017)

# save the initial inspection results to a text file
capture.output(str(df_2017), file = paste0("/txt/cases/inspection_crime_", "2017", ".txt"))
capture.output(summary(df_2017), file = paste0("/txt/cases/summary_crime_", "2017", ".txt"))  

cases_2017 <- NULL
df_2017 <- NULL

#### 2018 ####
cases_2018 <- fread("/data/raw/cases/cases_2018.csv")

# convert female_defendant, female_petitioner variables into binary
cases_2018 <- cases_2018 %>% 
  mutate(
    female_def_dummy = if_else(female_defendant == "1 female", 1, if_else(female_defendant == "0 male", 0, NA)),
    female_pet_dummy = if_else(female_petitioner == "1 female", 1, if_else(female_petitioner == "0 male", 0, NA))
  )

# save
write.table(cases_2018, "/data/dev/cases/cases_2018.csv", 
            sep = ",", row.names = FALSE, col.names = TRUE, quote = TRUE)

# merge data
df_2018 <- cases_2018 %>% 
  left_join(acts_sections_crime, by = "ddl_case_id", suffix = c("", "_merge")) # keeps only year 2018 cases

df_2018 <- df_2018 %>% 
  filter(criminal == 1) %>% # keeps only criminal cases
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
write.table(df_2018, "/data/dev/crime_merged/crime_2018_merged.csv", 
            sep = ",", row.names = FALSE, col.names = TRUE, quote = TRUE)

# inspect structure and summary of dataset
str(df_2018)
summary(df_2018)

# save the initial inspection results to a text file
capture.output(str(df_2018), file = paste0("/txt/cases/inspection_crime_", "2018", ".txt"))
capture.output(summary(df_2018), file = paste0("/txt/cases/summary_crime_", "2018", ".txt"))  

cases_2018 <- NULL
df_2018 <- NULL
