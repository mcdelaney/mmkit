#' @title sql_lst
#' @description A convenience funtion that combines a list or vector into a string \\n
#' which is then suitable for use in a sql statement
#' @param  X A vector or list
#' @export

sql_lst <- function(X){ 
  if (inherits(X, "character")) {
    paste0("('", paste(X, collapse = "','"), "')")
  }else{
    paste0("(", paste(X, collapse = ","), ")")
  }
}
