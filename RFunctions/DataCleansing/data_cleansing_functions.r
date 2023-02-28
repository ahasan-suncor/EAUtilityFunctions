# Databricks notebook source
library(dplyr)
library(rlang)

# COMMAND ----------

#' Cleans the column values in a Spark DataFrame based on outlier ranges and returns the result.
#'
#' @param df A Spark DataFrame to filter.
#' @param outliers_info A named list containing the outlier ranges for each column to filter.
#'                      The names of the list are the column names to filter.
#'                      The values of the list are 2-element numeric vectors representing
#'                      the minimum and maximum values for each column.
#' @return A filtered DataFrame. Column values that are out of range are replaced with NA.
#'
#' @import dplyr
#' @import rlang
clean_process_data_with_outliers <- function(df, outliers_info) {
  # Loop through each column name and outlier range in the dictionary and ilter the Spark DataFrame based on the outlier range
  for (col_name in names(outliers_info)) {

    min_val <- outliers_info[[col_name]][[1]]
    max_val <- outliers_info[[col_name]][[2]]

    df <- df %>%
          dplyr::mutate(!!sym(col_name) := ifelse(!!sym(col_name) >= min_val & !!sym(col_name) <= max_val, !!sym(col_name), NA))
  }

  return(df)
}
