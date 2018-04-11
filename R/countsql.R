#' @title countsql
#' @description SQL Convenience function. Passes vars to SQL GROUP BY verb.
#' Direct complaints to Mark Thompson.
#' @param col Character. The column(s) to group by
#' @param table Character. The table to query (Schema optional)
#' @param aggCol Character. Optional. If left out, returns number of rows in group, similar to n(),
#' otherwise, returns sum of aggCol.
#' @param aggFunc Aggregation function to be applied to data. Defaults to sum.
#' @param conn Optional. dplyr connection object.
#' @export


countsql <- function(col, table, aggCol=1, aggFunc='sum', conn=NULL) {

 if (is.null(conn)) {
  conn <- findConn()
 }

 res  <- getrs(conn = conn, sprintf(
  "select %1$s, %2$s(%3$s) from %4$s group by %1$s",
  col, aggFunc, aggCol, table))
 return(res)
}
