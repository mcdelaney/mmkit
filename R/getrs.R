#' @title getrs
#' @description A convenience function to execute database queries.
#' Supports str_form substitution params in statement by default.
#' @param statement SQL query string of data to return.
#' @param ... Values to be passed as substitution params. See str_form for more detail.
#' @param conn A valid connection to a redshift database.
#' @import DBI
#' @export

getrs <- function(statement, ..., conn = rs) {
 DBI::dbGetQuery(conn = conn$connection$con, str_form(statement, ...))
}
