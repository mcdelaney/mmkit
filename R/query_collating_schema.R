#' @title query_collating_schema
#' @description Runs the same query across multiple schemas in one DB and
#' collates results into one dataframe
#' @param con Database connection from \code{mmkit::db_conn}
#' @param query_template SQL query string templatized for \code{mmkit::str_form}
#' @param schemas Data frame with a column called \code{schema}
#' @param n Passed to \code{dplyr::collect}, in case your result set is large
#' @examples
#' \dontrun{
#' con <- mmkit::db_conn(...)
#' s <- get_lms_schemas(con)
#' qu <- "SELECT foo FROM {{schema}}.foo_table"
#' query_collating_schema(con, qu, s)
#' }
#' @author Ilya Goldin
#' @export

query_collating_schema <- function(con, query_template, schemas, n=Inf) {
 res <- schemas %>%
  distinct(schema) %>%
  group_by(schema) %>%
  mutate(query=mmkit::str_form(query_template, schema=schema)) %>%
  do(get_query_aux(con, .$schema, .$query, n))
 res
}

get_query_aux <- function (con, schema, query, n) {
 r <- dplyr::tbl(con$connection, dplyr::sql(query)) %>%
  dplyr::collect(n=n)
 message(schema, ": ", nrow(r), " rows")
 r
}

#' @title get_lms_schemas
#' @description List all known LMS schema names
#' @param con Database connection from \code{mmkit::db_conn}
#' @author Ilya Goldin
#' @export

get_lms_schemas <- function(con) {
 qu <- "
    SELECT DISTINCT(table_schema) AS schema
    FROM information_schema.tables
    WHERE table_schema LIKE '%_lms_prod'"
 s <- dplyr::tbl(con$connection, dplyr::sql(qu)) %>%
  collect()
 s
}
