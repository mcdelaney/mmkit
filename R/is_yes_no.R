#' @title is_yes_no
#' @description Returns yes, no or NA.
#' @param blank_vals A list of values to be considered blank.
#' Defaults to: c(NA, null, 'NA', 'na', '').
#' @export

is_yes_no <- function(x) {
 x[!tolower(x) %in% c("yes", "no")] <- NA
 return (tolower(x))
}
