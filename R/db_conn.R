#' @title db_con
#' @description Delivering dead simple database connections since June 7th 2016.
#' @param  database Name of database for which the connection should created.  Must correspond to a creds.json file key.
#' @param  cred_location Path to a json credentials file with keys corresponding to database names.
#' Each database entry should have the following fields: db_name, user, password, host, port
#' Defaults to creds.json in user root directory.
#' @param init_sql A sql statement to execute on connection; optional.
#' @export db_conn

db_conn <- function(database, ..., 
                    cred_location = "~/creds.json",
                    database_type = "postgres", 
                    init_sql = NA, 
                    dataset=NA) {

  mmkit::DbConn$new(database=database, 
                    ..., 
                    cred_location=cred_location,
                    database_type = database_type, 
                    init_sql = init_sql, 
                    dataset=dataset)
}
