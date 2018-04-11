#' @title first_not_na
#' @description Return the first element of a vector that is not NA
#' @param vec A vector that may have initial NA values
#' @export

first_not_na = function(vec) {
 i <- Position(function(z) ! is.na(z), vec)
 vec[i]
}
