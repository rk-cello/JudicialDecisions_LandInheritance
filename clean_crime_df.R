# clean crime case df

#### notes ####
# 

#### environment setup ####
# set working directory
setwd("/Users/reinakishida/Dropbox/Judicial_Decisions_and_Gender_Norms/analysis")

year_list <- c(2010:2018)
date_vars <- c("date_of_filing", "date_of_decision", "date_first_list", 
               "date_last_list", "date_next_list")

# load packages
pacman::p_load(data.table, tidyverse, lubridate)

#### inspect date columns ####
# visualize
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

#### clean crime case data (each year) ####

for (i in year_list) {
  crime_i <- fread(paste0("data/dev/crime_merged/crime_", i, "_merged.csv"))
  
  # Create dummies for dates falling outside 2010-2018
  for (j in date_vars) {
    crime_i <- crime_i %>% 
      mutate(
        !!sym(j) := as.IDate(!!sym(j)),
        !!sym(paste0(j, "_outside_dummy")) := if_else(!!sym(j) < as.IDate("2010-01-01") | !!sym(j) >= as.IDate("2019-01-01"), 1, 0)
      ) 
  }
  
  crime_i <- crime_i %>% 
    mutate(
      female_adv_def_dummy = if_else(female_adv_def == 1, 1, if_else(female_adv_def == 0, 0, NA)),
      female_adv_pet_dummy = if_else(female_adv_pet == 1, 1, if_else(female_adv_pet == 0, 0, NA)),
      female_judge_dummy = if_else(female_judge == "1 female", 1, if_else(female_judge == "0 nonfemale", 0, NA)),
    ) %>%
    select(-c(female_defendant, female_petitioner, female_adv_def, female_adv_pet, female_judge))
  
  crime_i <- crime_i %>%
    select(ddl_case_id, year, state_code, state_name, dist_code, district_name_key_merge, court_no, court_name, cino,
           judge_position, starts_with("female"),
           starts_with("type"), starts_with("purpose"), starts_with("disp"), starts_with("act"), starts_with("section"), 
           starts_with("date"), ends_with("outside_dummy"), bailable_ipc, number_sections_ipc, ddl_filing_judge_id, ddl_judge_id,
           ends_with("date")) %>%
    rename(
      dist_name = district_name_key_merge,
      crn_number = cino,
      type_id = type_name,
      type_name = type_name_s,
      purpose_id = purpose_name,
      purpose_name = purpose_name_s,
      disp_id = disp_name,
      disp_name = disp_name_s,
      act_id = act,
      act_name = act_s,
      section_id = section,
      section_name = section_s
    ) 
     
  crime_i <- crime_i %>%
    mutate(date_of_decision_year = year(date_of_decision),
           across(ends_with("date"), ~ as.IDate(.))) %>%
    mutate(
      tenure_length = as.numeric(end_date - start_date), 
      tenure_length_negative = if_else(tenure_length < 0, 1, 0),
      red_flag1 = if_else(date_of_decision < date_of_filing, 1, 0),
      red_flag2 = if_else(date_of_decision_year > year + 1, 1, 0),
      case_duration = as.numeric(date_of_decision - date_of_filing)
      )
  
  # save
  write_csv(crime_i, paste0("data/dev/crime_merged/crime_", i, "_merged_clean.csv"))
  rm(crime_i)
}

#### visualize case duration####
output_dir <- "fig/case_duration"
dir.create(output_dir, showWarnings = FALSE)

for (i in year_list) {
  crime_i <- fread(paste0("data/dev/crime_merged/crime_", i, "_merged_clean.csv"))
  
  Histogram
  ggplot(crime_i, aes(x = case_duration)) +
    geom_histogram(bins = 50) +
    labs(title = paste0("Histogram of Criminal Case Duration in ", i),
         x = "Case Duration (days)",
         y = "Frequency") +
    theme_minimal()
  ggsave(paste0("fig/case_duration/hist_case_duration_", i, ".png"), width = 6, height = 4, units = "in", bg = "white")
  
  # Histogram of outliers
  ggplot(crime_i, aes(x = case_duration)) +
    geom_histogram(bins = 50) +
    scale_y_continuous(limits = c(0, 50)) +
    labs(title = paste0("Outliers of Criminal Case Duration in ", i),
         x = "Case Duration (days)",
         y = "Frequency") +
    theme_minimal()
  ggsave(paste0("fig/case_duration/hist_outlier_case_duration_", i, ".png"), width = 6, height = 4, units = "in", bg = "white")
  
  # Close-up histogram
  ggplot(crime_i, aes(x = case_duration)) +
    geom_histogram(bins = 50) +
    scale_x_continuous(limits = c(-100, 5000)) +
    labs(title = paste0("Criminal Case Duration in ", i),
         x = "Case Duration (days)",
         y = "Frequency") +
    theme_minimal()
  ggsave(paste0("fig/case_duration/hist_closeup_case_duration_", i, ".png"), width = 6, height = 4, units = "in", bg = "white")
  
  # Close-up CDF
  ggplot(crime_i, aes(x = case_duration)) + 
    stat_ecdf(geom = "step") +
    scale_x_continuous(limits = c(-100, 5000)) +
    labs(title = paste0("CDF of Criminal Case Duration in ", i),
         x = "Case Duration (days)",
         y = "Cumulative Probability") +
    theme_minimal()
  ggsave(paste0("fig/case_duration/cdf_closeup_case_duration_", i, ".png"), width = 6, height = 4, units = "in", bg = "white")
  
  
  rm(crime_i)
}




#### generate case duration red flag across years ####
crime_df <- NULL

for (i in year_list) {
  crime_i <- fread(paste0("data/dev/crime_merged/crime_", i, "_merged_clean.csv")) %>% 
    mutate(conviction = if_else(disp_name == "convicted", 1, 0)) %>% 
    select(year, ddl_case_id, ddl_judge_id, conviction,
           female_def_dummy, female_pet_dummy, female_adv_def_dummy, female_adv_pet_dummy,
           case_duration)
  
  crime_df <- rbind(crime_df, crime_i)
  rm(crime_i)
}

crime_df <- crime_df %>% 
  mutate(
    red_flag = case_when(
      case_duration < 0 ~ 1,
      case_duration > quantile(case_duration, 0.99, na.rm = TRUE) ~ 1,
      TRUE ~ 0
    )) 

output_dir <- "data/dev/crime_appended"
dir.create(output_dir, showWarnings = FALSE)
write_csv(crime_df, "data/dev/crime_appended/crime_appended_redflag.csv")


#### notes ####
crime_2011 %>% 
  summarise(
    n_criminal = n(),
    n_red_flag1 = sum(red_flag1, na.rm = TRUE),
    n_red_flag2 = sum(red_flag2, na.rm = TRUE),
    n_filing_outside = sum(date_of_filing_outside_dummy, na.rm = TRUE),
    n_decision_outside = sum(date_of_decision_outside_dummy, na.rm = TRUE),
    n_tenure_length_negative = sum(tenure_length_negative, na.rm = TRUE),
    share_red_flag1 = n_red_flag1 / n_criminal,
    share_red_flag2 = n_red_flag2 / n_criminal,
    share_filing_outside = n_filing_outside / n_criminal,
    share_decision_outside = n_decision_outside / n_criminal,
    share_tenure_length_negative = n_tenure_length_negative / n_criminal
  ) %>% 
  select(starts_with("share_"))

#### notes ####
ggplot(crime_2010, aes(x = case_duration)) +
  geom_histogram(bins = 50) +
  scale_y_continuous(limits = c(0, 50)) +
  labs(title = paste0("Outliers of Criminal Case Duration in ", 2010),
       x = "Case Duration (days)",
       y = "Frequency") +
  theme_minimal()

ggplot(crime_2010, aes(x = case_duration)) +
  geom_histogram(bins = 50) +
  scale_x_continuous(limits = c(-100, 5000)) +
  labs(title = paste0("Criminal Case Duration in ", 2010),
       x = "Case Duration (days)",
       y = "Frequency") +
  theme_minimal()

ggplot(crime_2010, aes(x = case_duration)) + 
  stat_ecdf(geom = "step") +
  scale_x_continuous(limits = c(-100, 5000)) +
  labs(title = paste0("CDF of Criminal Case Duration in ", 2010),
       x = "Case Duration (days)",
       y = "Cumulative Probability") +
  theme_minimal()


#### Boxplot
for (i in year_list) {
  crime_i <- fread(paste0("data/dev/crime_merged/crime_", i, "_merged_clean.csv"))
  
  ggplot(crime_i, aes(x = case_duration)) + 
    geom_boxplot(outliers = FALSE) +
    labs(title = paste0("Boxplot of Criminal Case Duration in ", i),
         x = "Case Duration (days)",
         y = "Frequency") +
    theme_minimal()
  
  ggsave(filename = paste0("fig/case_duration/cdf_case_duration_", i, ".png"),
         width = 6, height = 4, units = "in", bg = "white")
  
  rm(crime_i)
}
