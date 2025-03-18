# check if ddl_judge_id is selective

#### notes ####
# a lot of missing data on judges assigned to each case (both decision and filing)

#### environment setup ####
# set working directory
setwd("/Users/reinakishida/Dropbox/Judicial_Decisions_and_Gender_Norms/analysis")

# load packages
pacman::p_load(data.table, tidyverse, tableone, stargazer, vtable, fastDummies)
pacman::p_load(data.table, tidyverse, fastDummies)
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
              judge_NA_share = n_judge_NA / n_criminal,
              judge_nonNA_share = 1 - judge_NA_share) %>%
    mutate(year = i) %>% 
    select(year, everything())
  
  # bind summary
  crime_judge_na_all <- rbind(crime_judge_na_all, crime_judge_na_i)

  crime_i <- NULL
}

# save
write_csv(crime_judge_na_all, "stat_table/crime_judgeID_NA.csv")


#### notes: removing NAs in vars for summary stats ####
crime_i <- NULL
crime_judge_na_mean_i <- NULL
crime_judge_na_mean_all <- NULL

for (i in year_list) {
  crime_i <- fread(paste0("data/dev/crime_merged/crime_", i, "_merged_clean.csv")) %>% 
    mutate(judge_id_na = if_else(is.na(ddl_judge_id), 1, 0))
  
  # summary of means by judge_id_na
  crime_judge_na_mean_i <- crime_i %>% 
    group_by(judge_id_na) %>% 
    summarise_all(n, mean, sd, na.rm = TRUE) %>% 
    mutate(year = i)
  
  # bind summary
  crime_judge_na_mean_all <- rbind(crime_judge_na_mean_all, crime_judge_na_mean_i)
  
  crime_i <- NULL
}

# long to wide
crime_judge_na_mean_wide <- crime_judge_na_mean_all2 %>% 
  select(year, everything()) %>% 
  pivot_wider(names_from = judge_id_na, 
              values_from = c(names(crime_judge_na_mean_all), -year, -judge_id_na))



#### notes: balance test ####
# for (i in year_list) {
#     crime_i <- fread(paste0("data/dev/crime_merged/crime_", i, "_merged_clean.csv"))
#     
#     # judges NA dummy
#     crime_i <- crime_i %>% 
#       mutate(judge_id_na = if_else(is.na(ddl_judge_id), 1, 0))
#     
#     # exclude year and judge_id_na before passing to CreateTableOne
#     vars_to_include <- setdiff(names(crime_i), c("year", "judge_id_na"))
#     
#     # balance table
#     balance_i <- CreateTableOne(vars = vars_to_include, 
#                                 strata = "judge_id_na", 
#                                 data = crime_i, 
#                                 test = TRUE)
#   # convert to data frame
#   balance_df_i <- print(balance_i, smd = TRUE, quote = TRUE, noSpaces = FALSE)
#   write_csv(as.data.frame(balance_df_i), paste0("stat_table/balance/crime_", i, "_balance.csv"))
#   
#   rm(crime_i)
# }
# 
# vars1 <- c("state_code", "dist_code")
# 
# vars2 <- c("court_no", "cino", "judge_position", "type_name", "purpose_name")
# 
# vars3 <- c("disp_name", "date_of_filing", "date_of_decision", "date_first_list")
# 
# vars4 <- c("date_last_list", "date_next_list", "female_def_dummy", "female_pet_dummy")
# 
# vars5 <- c("female_adv_def_dummy", "female_adv_pet_dummy", "female_judge_dummy")
# 
# vars6 <- c("act", "section", "bailable_ipc", "number_sections_ipc")
# 
# vars7 <- c("ddl_filing_judge_id", "ddl_judge_id", "start_date")
# 
# vars8 <- c("end_date", "act_s", "section_s", "purpose_name_s")
# 
# vars9 <- c("type_name_s", "disp_name_s", "count")
# 
# vars10 <- c("pc11_state_name", "pc11_state_id", "pc11_district_name")
# 
# vars11 <- c("pc11_district_id", "district_name", "court_name")

# vars_to_test <- c(vars1, vars2, vars3, vars4, vars5, vars6, vars7, vars8, vars9, vars10, vars11)
# 
# vars_to_test <- c(vars1, vars2)
# 
# crime_i <- NULL
# for (i in year_list) {
  crime_i <- fread(paste0("data/dev/crime_merged/crime_", i, "_merged_clean.csv")) %>% 
    mutate(
      judge_id_na = if_else(is.na(ddl_judge_id), 1, 0),
      across(everything(), ~ ifelse(is.na(.), "NA", .))
      ) %>% 
    dummy_cols(select_columns = vars1,
      remove_first_dummy = FALSE, remove_selected_columns = FALSE) %>% 
    mutate(across(where(is.integer), as.numeric))
  
  # Conduct t-tests for multiple variables efficiently
  t_test_results_i <- rbindlist(lapply(vars_to_test, function(var) {
    
    # Skip variables with all NA values
    if (all(is.na(crime_i[[var]]))) return(NULL)
    
    test <- tryCatch(
      t.test(crime_i[[var]] ~ crime_i$judge_id_na, var.equal = TRUE),
      error = function(e) return(NULL)  # Skip problematic variables
    )
    
    if (is.null(test)) return(NULL)  # Skip if test failed
    
    test <- t.test(crime_i[[var]] ~ crime_i$judge_id_na, var.equal = TRUE)
    data.table(
      Variable = var,
      Mean_Group_0 = mean(crime_i[[var]][crime_i$judge_id_na == 0], na.rm = TRUE),
      Mean_Group_1 = mean(crime_i[[var]][crime_i$judge_id_na == 1], na.rm = TRUE),
      Mean_Diff = diff(test$estimate),
      T_Value = test$statistic,
      P_Value = test$p.value,
      Conf_Lower = test$conf.int[1],
      Conf_Upper = test$conf.int[2]
    )
  }))
  
  write.table(t_test_results_i, paste0("stat_table/balance/crime_", i, "_balance.csv"),
              sep = ",", row.names = FALSE, col.names = TRUE, quote = TRUE)

  rm(t_test_results_i, crime_i)
#### notes: compute summary stats by judge_id_na ####
for (i in year_list) {
  crime_i <- fread(paste0("data/dev/crime_merged/crime_", i, "_merged_clean.csv")) %>% 
    mutate(judge_id_na = if_else(is.na(ddl_judge_id), 1, 0),
           filing_judge_id_na = if_else(is.na(ddl_filing_judge_id), 1, 0)) %>% 
    select(-c(year, ddl_judge_id, ddl_filing_judge_id))
  
  # Ensure judge_id_na is a factor for categorical summaries
  crime_i <- crime_i %>%
    mutate(judge_id_na = as.factor(judge_id_na))
  
  # summary for numeric
  num_summary_i <- crime_i %>% 
    group_by(judge_id_na) %>% 
    summarise(across(where(is.numeric), 
                     list(mean = ~ mean(., na.rm = TRUE), 
                            sd = ~ sd(., na.rm = TRUE), 
                            n = ~ sum(!is.na(.))), 
                     .names = "{.col}_{.fn}"), .groups = "drop") %>%
    pivot_longer(-judge_id_na, names_to = c("variable", ".value"), names_pattern = "(.+)_(mean|sd)") %>%
    pivot_wider(names_from = judge_id_na, values_from = c(mean, sd)) 
  
  # Ensure numeric columns exist before calculating differences
  if ("mean_1" %in% names(num_summary_i) & "mean_0" %in% names(num_summary_i)) {
    num_summary_i <- num_summary_i %>%
      mutate(diff = mean_1 - mean_0)  # Assuming 1 = NA, 0 = non NA
  } else {
    num_summary_i$diff <- NA  # Avoids errors if columns are missing
  }
  
  # t-test
  t_test_i <- crime_i %>% 
    select(where(is.numeric)) %>% 
    summarise(across(everything(), 
                     ~ broom::tidy(t.test(. ~ crime_i$judge_id_na))$p.value, , .groups = "drop")) %>% 
    pivot_longer(everything(), names_to = "variable", values_to = "p_value")
  
  # merge t-test results with summary
  num_balance_i <- num_summary_i %>% 
    left_join(t_test_i, by = "variable") 
  
  
  # summary for factor
  fac_summary_i <- crime_i %>% 
    group_by(judge_id_na) %>% 
    summarise(across(where(is.factor), 
                     ~ list(prop.table(table(.))), .names = "{.col}_prop", .groups = "drop")) %>% 
    unnest(cols = everything()) %>%  # Flatten the list structure
    pivot_longer(-judge_id_na, names_to = c("variable", ".value"), names_pattern = "(.+)_prop") %>%
    pivot_wider(names_from = judge_id_na, values_from = prop)
    
  # chisq-test
  chisq_test_i <- crime_i %>% 
    select(where(is.factor)) %>%
    summarise(across(everything(), 
                     ~ chisq.test(table(crime_i$judge_id_na, .))$p.value, .groups = "drop")) %>%  
    pivot_longer(everything(), names_to = "variable", values_to = "p_value")
    
  # merge chisq-test results with summary
  fac_balance_i <- fac_summary_i %>% 
    left_join(chisq_test_i, by = "variable")
  
  # bind summary
  balance_i <- bind_rows(num_balance_i, fac_balance_i) 
  
  # save
  write_csv(balance_i, paste0("stat_table/balance/crime_", i, "_balance.csv"))
  
  rm(crime_i, num_summary_i, t_test_i, num_balance_i, fac_summary_i, chisq_test_i, fac_balance_i, balance_i)
}

for (i in year_list) {
  
  crime_i <- fread(paste0("data/dev/crime_merged/crime_", i, "_merged_clean.csv")) %>% 
    mutate(judge_id_na = if_else(is.na(ddl_judge_id), 1, 0),
           filing_judge_id_na = if_else(is.na(ddl_filing_judge_id), 1, 0)) %>% 
    select(-c(year, ddl_judge_id, ddl_filing_judge_id),
           -tenure_length)  # remove tenure_length
  
  # Ensure necessary variables are factor
  crime_i <- crime_i %>%
    mutate(judge_id_na = as.factor(judge_id_na),
           across(where(is.character), ~ as.factor(.)), # Convert character to factor
           across(ends_with("dummy"), ~ as.factor(.)), # Convert dummy variables to factor
           across(c(state_code, dist_code, court_no, type_name, purpose_name, disp_name, act, section), 
                  ~ as.factor(.)) # Convert integer (code) to factor
           ) 
  
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
  
  ### 6. Factor Summary (Handling NA Properly)
  fac_summary_i <- crime_i %>% 
    group_by(judge_id_na) %>% 
    summarise(across(where(is.factor), 
                     ~ list(prop.table(table(.))), .names = "{.col}_prop"), .groups = "drop") %>% 
    unnest(cols = everything()) %>%  # Flatten the list structure
    pivot_longer(-judge_id_na, names_to = c("variable", ".value"), names_pattern = "(.+)_prop") %>%
    pivot_wider(names_from = judge_id_na, values_from = prop)
  
  ### 7. Chi-Square Test for Factor Variables
  if (valid_ttest) {
    chisq_test_i <- crime_i %>% 
      select(where(is.factor)) %>%
      summarise(across(everything(), ~ chisq.test(table(crime_i$judge_id_na, .))$p.value), .groups = "drop") %>%
      pivot_longer(everything(), names_to = "variable", values_to = "p_value")
  } else {
    chisq_test_i <- tibble(variable = character(), p_value = numeric())  # Empty tibble if no valid test
  }
  
  ### 8. Merge Factor Summary with Chi-Square Test Results
  fac_balance_i <- fac_summary_i %>% 
    left_join(chisq_test_i, by = "variable")
  
  ### 9. Combine Numeric and Factor Balance Tables
  balance_i <- bind_rows(num_balance_i, fac_balance_i) 
  
  ### 10. Save the Output
  write_csv(balance_i, paste0("stat_table/balance/crime_", i, "_balance.csv"))
  
  ### 11. Cleanup
  rm(crime_i, num_summary_i, t_test_i, num_balance_i, fac_summary_i, chisq_test_i, fac_balance_i, balance_i)
}



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

