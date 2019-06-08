#' @title hot_encode
#' @description A utility to create coerce a vector to a one-hot encoded dataframe.
#' @param  input_col A character of factor vector to be coerced to a one-hot encoded
#' dataframe. Result should be bound to the original dataframe for modeling purposes.
#' @param output_label String used to label encoded columns. Eg: if variable to be
#' encoded is 'state', then the resulting column names will be: state_CA, state_MD,
#' state_AL, etc.
#' @param blank_to_na Converts blank strings to NA. Default = TRUE.
#' @export

hot_encode <- function(input_col, output_label, blank_to_na = TRUE) {

 message(sprintf("One hot encoding data for %s...", output_label))
 if (is.null(input_col)) {
   stop("Input column does not exist! Check variable names!")
 }
 input_class <- class(input_col)
 if (!input_class %in% c('character', 'factor')) {
  stop("Input data class is incorrect.  Must be either factor or character!")
 }

 if (blank_to_na) {
  message("Recoding blank value as NA...")
  input_col[input_col == ""] <- NA
 }

 if (input_class == "character") {
  message("Coercing character to factor...")
  input_col <- as.factor(input_col)
 }

 message("Starting factor level comparison...")
 vals = lapply(X = input_col, FUN = function(X) X == levels(input_col))

 scaff = data.frame(matrix(unlist(vals), nrow = length(vals), byrow = T))
 names(scaff) <- paste0(output_label, "_", levels(input_col))

 message("Encoded data successfully created...")

  return(scaff)
}
