#' @title A wrapper for dplyr::count to calculate percentage of
#' @export count_pct

count_pct <- function(data, ..., col = n) {
  grouping_vars_expr <- quos(...)
  col_expr <- enquo(col)
  
  data %>%
    group_by(!!! grouping_vars_expr) %>%
    mutate(pct = (!! col_expr) / sum(!! col_expr),
           n = n()) %>%
    ungroup() %>% 
    arrange(desc(pct))
}