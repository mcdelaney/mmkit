#' @title get_uuid
#' @description Create a uuid.
#' @export


uuid <- function() {

 hex_digits <- c(as.character(0:9), letters[1:6])
 y_digits <- hex_digits[9:12]
 paste(
  paste0(sample(hex_digits, 8), collapse=''),
  paste0(sample(hex_digits, 4), collapse=''),
  paste0('4', sample(hex_digits, 3), collapse=''),
  paste0(sample(y_digits,1), sample(hex_digits, 3), collapse=''),
  paste0(sample(hex_digits, 12), collapse=''),
  sep='-')
}



