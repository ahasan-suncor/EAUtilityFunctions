# Databricks notebook source
library(testthat)

# COMMAND ----------

testthat::test_that("clean_process_data_with_outliers returns the correct data frame", {
  test_data <- data.frame(a = c(1, 2, 3, 4, 5)
                        , b = c(2, 4, 6, 8, 10)
                        , c = c(3, 6, 9, 12, 15)
                         )

  outliers_info <- list(a = c(2, 4)
                      , b = c(3, 7)
                      , c = c(10, 14)
                      )

  expected_output <- data.frame(a = c(NA, 2, 3, 4, NA)
                              , b = c(NA, 4, 6, NA, NA)
                              , c = c(NA, NA, NA, 12, NA)
                               )

  cleaned_data <- clean_process_data_with_outliers(test_data, outliers_info)

  expect_equal(cleaned_data, expected_output)

  })

test_that("clean_process_data_with_outliers does not modify columns not in outliers_info", {
  test_data <- data.frame(a = c(1, 2, 3, 4, 5)
                        , b = c(2, 4, 6, 8, 10)
                        , c = c(3, 6, 9, 12, 15)
                        , d = c(4, 8, 12, 16, 20)
                           )

  outliers_info <- list(a = c(2, 4)
                      , b = c(3, 7)
                      , c = c(10, 14)
                        )

  cleaned_data <- clean_process_data_with_outliers(test_data, outliers_info)

  # Expect that column d is not modified
  expect_identical(cleaned_data$d, test_data$d)
})
