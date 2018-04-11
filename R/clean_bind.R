#' @title clean_bind
#' @description A safer way to invoke bind_rows that fill not fail if dataframes are null.
#' @param  frames A list of data frames to bind
#' @import dplyr
#' @export

clean_bind <- function(frames){
  bind_rows(frames[unlist(lapply(X = frames, is.data.frame))])
 }
