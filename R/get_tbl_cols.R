#' @title get_tbl_cols
#' @description A funtion to return a character vector of database table colum names.
#' @param  table_name Full schema and table name
#' @param  conn A valid redshift connection.  defaults to rs
#' @param  match_string Optional regex string to match column names against.
#'  If specified, only returns matching names
#' @import DBI
#' @export

get_tbl_cols <- function(table_name, conn, match_string = NULL){

 cols <- try({
  DBI::dbListFields(conn$connection$con, name = strsplit(table_name, "\\.")[[1]])
 })

 if (inherits(cols, "try-error")) {
  return(message("ERROR: table or database connection do not exist"))
 }else{

  if (!is.null(match_string)) {
   cols <- cols[grepl(x = cols, pattern = match_string, ignore.case = TRUE)]

  }

  return(cols)

 }

}
