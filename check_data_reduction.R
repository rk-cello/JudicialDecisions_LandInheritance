# check reduction of data after merging

#### notes ####
# 

#### environment setup ####
# set working directory
setwd("/Users/reinakishida/Dropbox/Judicial_Decisions_and_Gender_Norms/analysis")

# load packages
pacman::p_load(data.table, tidyverse)

#### summary of row no., criminal case no. and share ####
year_list <- c(2010:2018)

# cases
row_num_all <- NULL

for (i in year_list) {
  cases_i <- fread(paste0("data/raw/cases/cases_", i, ".csv"))
  
  # create table showing number of cases by year
  row_num_i <- cases_i %>%
    summarise(n_cases = n()) %>%
    mutate(year = i)
  
  cases_i <- NULL
  row_num_all <- rbind(row_num_all, row_num_i)
}

# criminal cases (uses crime case df before cleaning) 
row_num_criminal_all <- NULL

for (i in year_list) {
  crime_i <- fread(paste0("data/dev/crime_merged/crime_", i, "_merged.csv"))
  
  # create table showing number of cases by year
  row_num_criminal_i <- crime_i %>%
    summarise(n_criminal = n()) %>%
    mutate(year = i)
  
  crime_i <- NULL
  row_num_criminal_all <- rbind(row_num_criminal_all, row_num_criminal_i)
}

# combine both results

crime_case_share <- row_num_all %>% 
  left_join(row_num_criminal_all, by = "year") %>% 
  mutate(share_criminal = n_criminal/n_cases) %>% 
  select(year, n_cases, n_criminal, share_criminal)

write_csv(crime_case_share, "stat_table/crime_case_share.csv")

ggplot(crime_case_share, aes(x = year, y = share_criminal)) +
  geom_line() +
  geom_point() +
  labs(title = "Share of criminal cases in all cases",
       x = "Year",
       y = "Share of criminal cases") +
  theme_minimal()

ggsave("fig/crime_case_share.png", width = 6, height = 4, units = "in", bg = "white")


