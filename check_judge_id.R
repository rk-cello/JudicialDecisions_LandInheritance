# check if ddl_judge_id is selective

#### notes ####
### check unique ddl_judge_id
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


### check judge_id = NA
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

#### environment setup ####
setwd("/Users/reinakishida/Dropbox/Judicial_Decisions_and_Gender_Norms/analysis")
pacman::p_load(data.table, tidyverse, broom)

#### load data ####
judge_case_merge_key <- fread("data/raw/keys/judge_case_merge_key.csv")
judges_clean <- fread("data/raw/judges_clean.csv")

#### summary of judge ID NAs ####
year_list <- c(2010:2018)
crime_judge_na_all <- NULL

for (i in year_list) {
  crime_i <- fread(paste0("data/dev/crime_merged/crime_", i, "_merged_clean.csv"))
  
  # judges NA dummy
  crime_i <- crime_i %>% 
    mutate(judge_id_na = if_else(is.na(ddl_judge_id), 1, 0))
  
  # summary of judge ID NAs
  crime_judge_na_i <- crime_i %>% 
    summarise(n_criminal = n(),
              n_judge_NA = sum(judge_id_na),
              n_judge_nonNA = n_criminal - n_judge_NA,
              n_unique_judge = n_distinct(ddl_judge_id, na.rm = TRUE),
              judge_NA_share = n_judge_NA / n_criminal,
              judge_nonNA_share = 1 - judge_NA_share,
              criminal_per_unique_judge = n_criminal / n_unique_judge) %>%
    mutate(year = i) %>% 
    select(year, everything())
  
  # bind summary
  crime_judge_na_all <- rbind(crime_judge_na_all, crime_judge_na_i)

  crime_i <- NULL
}

write_csv(crime_judge_na_all, "stat_table/crime_judgeID_NA.csv")


#### balance test ####
for (i in year_list) {
  
  crime_i <- fread(paste0("data/dev/crime_merged/crime_", i, "_merged_clean.csv")) %>% 
    mutate(judge_id_na = if_else(is.na(ddl_judge_id), 1, 0),
           conviction = if_else(disp_name == "convicted", 1, 0)) %>% 
    select(judge_id_na, female_def_dummy, female_pet_dummy, female_adv_def_dummy, female_adv_pet_dummy, conviction)
  
  ### total counts per group
  counts_i <- crime_i %>% 
    group_by(judge_id_na) %>% 
    summarise(n = n(), .groups = "drop")
    
  ### summary
  summary_i <- crime_i %>% 
    group_by(judge_id_na) %>% 
    summarise(across(everything(), 
                     list(mean = ~ mean(., na.rm = TRUE), sd = ~ sd(., na.rm = TRUE)), 
                     .names = "{.col}_{.fn}"), .groups = "drop") %>%
    pivot_longer(-judge_id_na, names_to = c("variable", ".value"), names_pattern = "(.+)_(mean|sd)") %>%
    pivot_wider(names_from = judge_id_na, values_from = c(mean, sd)) %>% 
    cross_join(counts_i %>% pivot_wider(names_from = judge_id_na, values_from = n)) %>%
    rename(n_0 = `0`, n_1 = `1`)
  
  
  ### function to compute Welch t-test p-value
  welch_t_pvalue <- function(m0, m1, sd0, sd1, n0, n1) {
    se <- sqrt((sd0^2 / n0) + (sd1^2 / n1))
    t_stat <- (m0 - m1) / se
    df <- ((sd0^2 / n0 + sd1^2 / n1)^2) / 
      (((sd0^2 / n0)^2) / (n0 - 1) + ((sd1^2 / n1)^2) / (n1 - 1))
    p_val <- 2 * pt(-abs(t_stat), df)
    return(p_val)
  }
  
  ### apply t-test rowwise
  summary_with_p_i <- summary_i %>%
    rowwise() %>%
    mutate(
      mean_diff = mean_1 - mean_0,
      p_value = welch_t_pvalue(
        m0 = mean_0,
        m1 = mean_1,
        sd0 = sd_0,
        sd1 = sd_1,
        n0 = n_0,
        n1 = n_1
      )
    ) %>%
    ungroup()
  
  write_csv(summary_with_p_i, paste0("stat_table/balance/crime_", i, "_balance.csv"))
  rm(crime_i, summary_i, summary_with_p_i)
}
  
  


#### notes: ####
