#' @title coalesce
#' @description Function to coalesce a vector to a single value, taking the
#' first non-NA entry, e.g., coalesce(NA, NA, "California") returns "California".
#' @param ... A list of values to coalesce, in order of preference.
#' @export

coalesce <- function(...) {
 coalesceSingle <- function(x, y) {
  if (is.null(x)) {
   warning("NULL argument in coalesce")
   return(y)
  }
  if (is.null(y)) {
   warning("NULL argument in coalesce")
   return(x)
  }
  ifelse(is.na(x), y, x)
 }
 Reduce(coalesceSingle, list(...))
}
