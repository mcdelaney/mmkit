#' @title histvals
#' @description Return either a list of history tracked fields,
#' or specify a field and get unique values.
#' @param obj A staging table name. Schema is not required as salesforce_stg is assumed.
#' @param field History tracked field name.
#' Optional, if specified, returns unique values for field name listed.
#' @param conn A redshift database connection. Defaults to rs.
#' @import RPostgreSQL
#' @import dplyr
#' @export

histvals <- function(conn = rs, obj, field = NULL){

 if (is.null(field)) {

  getrs(conn = conn, "select sum(1) as total, field
                      from salesforce_stg.{{obj}}
                      group by field", obj = obj) %>%
   arrange(total) %>%
   as.data.frame(.) %>%
   print(.)

 }else{

  getrs(conn = conn, "select sum(1) as total, field, newvalue
                       from salesforce_stg.{{obj}}
                       where lower(field) in ('{{field}}')
                       group by field, newvalue",
        obj = obj, field = tolower(field)) %>%
   arrange(total) %>%
   as.data.frame(.) %>%
   print(.)
 }
}
