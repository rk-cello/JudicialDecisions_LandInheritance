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

year_list <- c(2010:2018)

#### load data ####
judge_case_merge_key <- fread("data/raw/keys/judge_case_merge_key.csv")
judges_clean <- fread("data/raw/judges_clean.csv")

#### summary of judge ID NAs ####
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


#### balance test by judge_id_na (each year) ####
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
  
  
#### balance test by judge_id_na (combined years) ####
crime_df <- NULL

for (i in year_list) {
  crime_i <- fread(paste0("data/dev/crime_merged/crime_", i, "_merged_clean.csv")) %>% 
    mutate(judge_id_na = if_else(is.na(ddl_judge_id), 1, 0),
           conviction = if_else(disp_name == "convicted", 1, 0)) %>% 
    select(year, judge_id_na, female_def_dummy, female_pet_dummy, female_adv_def_dummy, female_adv_pet_dummy, conviction)
  
  crime_df <- rbind(crime_df, crime_i)
  rm(crime_i)
}

# test by judge_id_na
  ### total counts per group
  counts_i <- crime_df %>% 
    select(-year) %>% 
    group_by(judge_id_na) %>% 
    summarise(n = n(), .groups = "drop")
  
  ### summary
  summary_i <- crime_df %>% 
    select(-year) %>% 
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
  
  write_csv(summary_with_p_i, paste0("stat_table/balance/crime_allyears_balance.csv"))
  rm(summary_i, summary_with_p_i)

  
#### plot conviction rate ####
  # time trend of conviction rate by judge_id_na with standard error
crime_df %>% 
  group_by(year, judge_id_na) %>% 
  summarise(conviction_rate = mean(conviction, na.rm = TRUE), 
            se = sd(conviction, na.rm = TRUE) / sqrt(n()), .groups = "drop") %>% 
  ggplot(aes(x = year, y = conviction_rate, color = as.factor(judge_id_na))) +
  geom_line() +
  geom_point() +
  geom_errorbar(aes(ymin = conviction_rate - se, ymax = conviction_rate + se), width = 0.1) +
  labs(title = "Conviction Rate by Judge ID NA",
       x = "Year",
       y = "Conviction Rate") +
  ylim(0, 0.15) +
  scale_color_manual(name = "Judge ID NA", 
                     values = c("0" = "blue", "1" = "red"),
                     labels = c("0" = "Non-NA", "1" = "NA")) +
  theme_minimal()

ggsave("fig/conviction_rate_judge_id_na.png", width = 6, height = 4, bg = "white")
  


#### notes: ####
