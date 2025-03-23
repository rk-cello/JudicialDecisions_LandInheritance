# check if ddl_judge_id is selective

#### notes ####
# a lot of missing data on judges assigned to each case (both decision and filing)

#### environment setup ####
# set working directory
setwd("/Users/reinakishida/Dropbox/Judicial_Decisions_and_Gender_Norms/analysis")

# load packages
# pacman::p_load(data.table, tidyverse, tableone, stargazer, vtable, fastDummies)
# pacman::p_load(data.table, tidyverse, fastDummies)
pacman::p_load(data.table, tidyverse, broom)

#### load data ####
judge_case_merge_key <- fread("data/raw/keys/judge_case_merge_key.csv")
judges_clean <- fread("data/raw/judges_clean.csv")


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


#### Crime Cases: Summary Stats ####
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

# save
write_csv(crime_judge_na_all, "stat_table/crime_judgeID_NA.csv")


#### Numeric balance ####
for (i in year_list) {
  
  crime_i <- fread(paste0("data/dev/crime_merged/crime_", i, "_merged_clean.csv")) %>% 
    mutate(judge_id_na = if_else(is.na(ddl_judge_id), 1, 0),
           filing_judge_id_na = if_else(is.na(ddl_filing_judge_id), 1, 0)) %>% 
    select(-c(year, ddl_judge_id, ddl_filing_judge_id, tenure_length))  # Removing tenure_length
  
  # Ensure necessary variables are factor
  crime_i <- crime_i %>%
    mutate(judge_id_na = as.factor(judge_id_na),
           across(starts_with("female"), as.numeric), # Convert female dummy to numeric
           across(where(is.character), as.factor), # Convert character to factor
           # across(ends_with("dummy"), as.factor), # Convert dummy variables to factor
           across(c(state_code, dist_code, court_no, type_name, purpose_name, disp_name, act, section), as.factor)) 
  
  ### 1. Numeric Summary
  num_summary_i <- crime_i %>% 
    group_by(judge_id_na) %>% 
    summarise(across(where(is.numeric), 
                     list(mean = ~ mean(., na.rm = TRUE), 
                          sd = ~ sd(., na.rm = TRUE)), 
                     .names = "{.col}_{.fn}"), .groups = "drop") %>%
    pivot_longer(-judge_id_na, names_to = c("variable", ".value"), names_pattern = "(.+)_(mean|sd)") %>%
    pivot_wider(names_from = judge_id_na, values_from = c(mean, sd))
  
  # Ensure numeric columns exist before calculating differences
  if (all(c("mean_1", "mean_0") %in% names(num_summary_i))) {
    num_summary_i <- num_summary_i %>%
      mutate(diff = mean_1 - mean_0)
  } else {
    num_summary_i$diff <- NA  # Avoids errors if columns are missing
  }
  
  ### 2. Check If `judge_id_na` Has Exactly Two Levels Before Running T-Test
  valid_ttest <- length(unique(crime_i$judge_id_na)) == 2
  
  ### 3. Remove Constant Numeric Variables Before Running T-Test
  non_constant_numeric_vars <- crime_i %>%
    select(where(is.numeric)) %>%
    summarise(across(everything(), ~ length(unique(.)) > 1)) %>%
    pivot_longer(everything(), names_to = "variable", values_to = "has_variation") %>%
    filter(has_variation) %>%
    pull(variable)
  
  ### 4. Run T-Test Only If judge_id_na Has Two Levels & Numeric Variables Have Variation
  if (valid_ttest & length(non_constant_numeric_vars) > 0) {
    t_test_i <- crime_i %>%
      filter(judge_id_na %in% c("0", "1")) %>% # Ensure only 0 and 1 groups are present
      select(all_of(non_constant_numeric_vars), judge_id_na) %>%
      summarise(across(where(is.numeric), 
                       ~ broom::tidy(t.test(. ~ judge_id_na))$p.value), .groups = "drop") %>%
      pivot_longer(everything(), names_to = "variable", values_to = "p_value")
  } else {
    t_test_i <- tibble(variable = character(), p_value = numeric())  # Empty tibble if no valid t-test variables
  }
  
  ### 5. Merge Numeric Summary with T-Test Results
  num_balance_i <- num_summary_i %>% 
    left_join(t_test_i, by = "variable")
  
  ### 6. Save the Output
  write_csv(num_balance_i, paste0("stat_table/balance/numeric/crime_", i, "_numbalance.csv"))
  
  ### 7. Cleanup
  rm(crime_i, num_summary_i, t_test_i, num_balance_i)
}


#### Factor balance ####
for (i in year_list) {
  crime_i <- fread(paste0("data/dev/crime_merged/crime_", i, "_merged_clean.csv")) %>% 
    mutate(judge_id_na = if_else(is.na(ddl_judge_id), 1, 0),
           filing_judge_id_na = if_else(is.na(ddl_filing_judge_id), 1, 0)) %>% 
    select(-c(year, ddl_case_id, ddl_judge_id, ddl_filing_judge_id, tenure_length))  # Removing tenure_length
  
  # Ensure necessary variables are factor
  crime_i <- crime_i %>%
    mutate(judge_id_na = as.factor(judge_id_na),
           across(starts_with("female"), as.numeric), # Convert female dummy to numeric
           across(where(is.character), as.factor), # Convert character to factor
           # across(ends_with("dummy"), as.factor), # Convert dummy variables to factor
           across(c(state_code, dist_code, court_no, type_name, purpose_name, disp_name, act, section), as.factor)) 
  
  # Get factor variables excluding 'judge_id_na'
  factor_vars <- crime_i %>% 
    select(where(is.factor), -judge_id_na) %>% 
    names()
  
  for (var in factor_vars) {
    crime_i_select <- crime_i %>%
      select(all_of(var), judge_id_na) 
    
    # Create contingency table
    tab <- table(crime_i_select$judge_id_na, crime_i_select[[var]])
    
    # Save contingency table as CSV
    write_csv(as.data.frame(tab), paste0("stat_table/balance/factor/crime_", i, "_", var, "_balance.csv"))
    
    # Ensure table is not empty
    if (sum(tab) == 0) next  
    
    # Avoid zero counts
    if (any(tab == 0)) tab <- tab + 0.5
    
    # Choose appropriate test
    if (any(chisq.test(tab)$expected < 5)) {
      if (nrow(tab) <= 2 && ncol(tab) <= 2) {
        chi_result <- fisher.test(tab)  # Use Fisher’s for small tables
      } else {
        chi_result <- chisq.test(tab, simulate.p.value = TRUE, B = 10000)  # Monte Carlo for large tables
      }
    } else {
      chi_result <- chisq.test(tab)
    }
    
    # Save test output
    test_output <- capture.output(chi_result)
    writeLines(test_output, paste0("txt/balance/crime_", i, "_", var, "_test.txt"))
  }
  
  rm(crime_i, factor_vars, var, crime_i_select, tab, chi_result, test_output)
  
}
  


#### Balance Test ####
for (i in year_list) {
  
  crime_i <- fread(paste0("data/dev/crime_merged/crime_", i, "_merged_clean.csv")) %>% 
    mutate(judge_id_na = if_else(is.na(ddl_judge_id), 1, 0),
           conviction = if_else(disp_name == "convicted", 1, 0)) %>% 
    select(judge_id_na, female_def_dummy, female_pet_dummy, female_adv_def_dummy, female_adv_pet_dummy, conviction)
  
  ### Total counts per group
  counts_i <- crime_i %>% 
    group_by(judge_id_na) %>% 
    summarise(n = n(), .groups = "drop")
    
  ### Summary
  summary_i <- crime_i %>% 
    group_by(judge_id_na) %>% 
    summarise(across(everything(), 
                     list(mean = ~ mean(., na.rm = TRUE), sd = ~ sd(., na.rm = TRUE)), 
                     .names = "{.col}_{.fn}"), .groups = "drop") %>%
    pivot_longer(-judge_id_na, names_to = c("variable", ".value"), names_pattern = "(.+)_(mean|sd)") %>%
    pivot_wider(names_from = judge_id_na, values_from = c(mean, sd)) %>% 
    cross_join(counts_i %>% pivot_wider(names_from = judge_id_na, values_from = n)) %>%
    rename(n_0 = `0`, n_1 = `1`)
  
  
  ### Run T-Test
  # Function to compute Welch t-test p-value
  welch_t_pvalue <- function(m0, m1, sd0, sd1, n0, n1) {
    se <- sqrt((sd0^2 / n0) + (sd1^2 / n1))
    t_stat <- (m0 - m1) / se
    df <- ((sd0^2 / n0 + sd1^2 / n1)^2) / 
      (((sd0^2 / n0)^2) / (n0 - 1) + ((sd1^2 / n1)^2) / (n1 - 1))
    p_val <- 2 * pt(-abs(t_stat), df)
    return(p_val)
  }
  
  # Apply t-test rowwise
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
  
  
  # t_test_i <- crime_i %>%
  #   # filter(judge_id_na %in% c("0", "1")) %>% # Ensure only 0 and 1 groups are present
  #   summarise(across(female_def_dummy:conviction, 
  #                     ~ broom::tidy(t.test(. ~ judge_id_na))$p.value), .groups = "drop") %>%
  #   pivot_longer(everything(), names_to = "variable", values_to = "p_value")
  # 
  # 
  # ### Merge Numeric Summary with T-Test Results
  # balance_i <- summary_i %>% 
  #   left_join(t_test_i, by = "variable")
  
  ### 6. Save the Output
  write_csv(summary_with_p_i, paste0("stat_table/balance/crime_", i, "_balance.csv"))
  
  ### 7. Cleanup
  rm(crime_i, summary_i, summary_with_p_i)
}
  
  


#### notes: ####
# How to conduct balance tests for factor (categorical) variables?? -> all into binary?
# female dummies are not included in the balance test (grouping factor judge_id_na doesn't have two levels)
# whether female_judge_dummy = NA
crime_2010_femNA <- crime_2010 %>% 
    filter(is.na(female_judge_dummy))

sum(crime_2010_femNA$judge_id_na == 1) # 564410
sum(crime_2010_femNA$judge_id_na == 0) # 12523

crime_2010_femnonNA <- crime_2010 %>% 
  filter(!is.na(female_judge_dummy))

sum(crime_2010_femnonNA$judge_id_na == 1) # 0
sum(crime_2010_femnonNA$judge_id_na == 0) # 260481

# whether female_judge_dummy = 1
crime_2010_fem1 <- crime_2010 %>%
  filter(female_judge_dummy == 1)

sum(crime_2010_fem1$judge_id_na == 1) # 0
sum(crime_2010_fem1$judge_id_na == 0) # 71989

crime_2010_fem0 <- crime_2010 %>%
  filter(female_judge_dummy == 0)

sum(crime_2010_fem0$judge_id_na == 1) # 0
sum(crime_2010_fem0$judge_id_na == 0) # 188492

