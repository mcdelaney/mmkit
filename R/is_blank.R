#' @title is_blank
#' @description Returns a boolean value indicating if a string is blank, or otherwise invalid.
#' By default, looks for any matches against: NA, null, 'NA', 'na', or ''.
#' @param string A string to validate.
#' @param blank_vals A list of values to be considered blank.
#' Defaults to: c(NA, null, 'NA', 'na', '').
#' @export

is_blank <- function(string, blank_vals = c(NA, 'NA', 'na', '')) {

 is.null(string) || string %in% c(NA,'', 'NA', 'na', 'null')

}
