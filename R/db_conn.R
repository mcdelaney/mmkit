#' Delivering dead simple database connections since June 7th 2016.
#' @param  database Name of database for which the connection should created.  Must correspond to a creds.json file key.
#' @param  cred_location Path to a json credentials file with keys corresponding to database names.
#' Each database entry should have the following fields: db_name, user, password, host, port
#' Defaults to creds.json in user root directory.
#' @param database_type One of postgres, mysql or ms_sql (for fdw connections). Defaults to postgres
#' @param init_sql A sql statement to execute on connection; optional.
#' @field connection A dplyr src connection object used to execute queries.
#' @field con A DBI compliant connection, extracted from dplyr_src.
#' @import DBI
#' @import methods
#' @export db_conn
#' @exportClass db_conn

db_conn <- setRefClass(
 "db_conn",
 fields = c("connection", "con"),
 methods = list(
  initialize = function(database, ..., cred_location = "~/creds.json",
                        database_type = "postgres", init_sql = NA) {
   creds <- mmkit::read_creds(database, ..., cred_location)

   message(sprintf("Creating connection to %s.%s", database, creds$db_name))

   connection <<- switch(database_type,
                         "postgres" = conn_psql(creds = creds),
                         "rpostgres" = conn_rpostgres(creds = creds),
                         "redshift" = conn_redshift(creds = creds),
                         "mysql" = conn_mysql(creds = creds),
                         "ms_sql" = conn_mssql(creds = creds),
                         stop("Database type must be postgres, rpostgres,
                              mysql, redshift or ms_sql"))

   if (database_type == "ms_sql") {
    con <<- connection
   }else{
    con <<- connection$con
   }

   if (!is.na(init_sql)) {
    message(sprintf('Executing sql: %s', init_sql))
    .self$send(init_sql)
   }

  },

  query = function(statement, ...){
   "Execute query using initialized database connection. Str_form templating is supported by default."
   DBI::dbGetQuery(conn = .self$connection$con, str_form(statement, ...))
  },

  #' @title send
  #' @description Execute arbitrary sql, expecting no response.
  #' Str_form substitution params supported by default.
  #' @export
  send = function(statement, ...){
   "Send a statement using initialized database connection. Str_form templating is supported by default."
   DBI::dbSendQuery(conn = .self$connection$con, str_form(statement, ...))
  },

  #' @description Return a list of columns from the requested table.
  #' @param table Name of table for which columns should be returned.
  #' @param match_string String to search for in columns.
  #' Partial matching applied. Optional.
  #' @export
  get_cols = function(table, match_string = NA) {
   "Return a list of columns in the specified table.
   Can optionally apply a regex match to find a specific column."
   if (grepl("\\.", table)) {

    schema = strsplit(table, "\\.")[[1]][1]
    table = strsplit(table, "\\.")[[1]][2]
    cols = DBI::dbGetQuery(conn = .self$connection$con,
                           str_form("SELECT column_name AS col
                                    FROM information_schema.columns
                                    WHERE table_name = '{{table}}' AND
                                    table_schema = '{{schema}}'
                                    ORDER BY ordinal_position",
                                    table = table, schema = schema))$col

   }else{

    cols = DBI::dbGetQuery(conn = .self$connection$con,
                           str_form("SELECT column_name AS col
                                    FROM information_schema.columns
                                    WHERE table_name = '{{table}}'
                                    ORDER BY ordinal_position",
                                    table = table))$col
   }

   if (is.na(match_string)) {

    return(cols)

   }else{

    return(cols[grepl(match_string, x = cols)])

   }
  },

  #' @description Return a list of tables from the requested schema.
  #' @param schema Name of schema for which tables should be returned. If blank,
  #' all tables will be returned.
  #' @param match_string String to search for in columns.
  #' Partial matching applied. Optional.
  #' @export
  get_tables = function(schema=NA, match_string=NA) {

   where = ifelse(is.na(schema), '', str_form("WHERE table_schema = '{{s}}'",
                                              s = schema))

   tables = DBI::dbGetQuery(conn = .self$connection$con,
                           str_form("SELECT table_schema, table_name
                                    FROM information_schema.tables
                                    {{where}}", where = where))

   if (where == '') {
    tables = paste(tables$table_schema, tables$table_name, sep = ".")
   }else{
    tables = tables$table_name
   }
   if (!is.na(match_string)) {
    tables = tables[grepl(match_string, tables)]
   }

   return(tables)
  }
 )
)

#' @title read_creds
#' @description Read database credentials from a file like creds.json
#' See params in \code{\link{mmkit::db_conn}}
#' @export read_creds
read_creds <- function(database, ..., cred_location = "~/creds.json") {
   required_fields <- c("db_name", "user", "password", "host", "port")

   creds = tryCatch(
    jsonlite::fromJSON(cred_location),
    error = function(e) {
     stop("mmkit::read_creds: Invalid cred_location")
    })

   if (!database %in% names(creds)) {
    stop(sprintf("Credentials file has no key for database: %s!", database))
   }else{
    creds <- creds[[database]]
   }

   if ('cluster_name' %in% names(creds)) {
    creds$host <- creds$cluster_name
    creds <- creds[!names(creds) %in% "cluster_name"]
   }

   if ('username' %in% names(creds)) {
    creds$user <- creds$username
    creds <- creds[!names(creds) %in% "username"]
   }

   if ('database' %in% names(creds)) {
    creds$db_name <- creds$database
    creds <- creds[!names(creds) %in% "database"]
   }

   db_sub_params <- list(...)

   if (length(db_sub_params) > 0) {
    creds <- lapply(X = creds, FUN = function(X) {
     str_form(X, db_sub_params)
    })
   }

   if (!all(required_fields %in% names(creds))) {
    stop(sprintf("Credentials entry for %s missing keys: %s!",
                 database, paste0(required_fields[!required_fields %in% names(creds)],
                                  collapse = ", ")))
   }

   invisible(creds)
}

#' @title conn_msql
#' @description DBI interface for microsoft sql server.
#' @param creds A list with entries for db_name, host, user, password, and port.

conn_mssql <- function(creds){

 if (!"RJDBC" %in% as.character(installed.packages(fields = "Name")[,1])) {
  stop("No RJDBC installation found...Please install before trying again.")
 }

 drv <- RJDBC::JDBC("com.microsoft.sqlserver.jdbc.SQLServerDriver",
                    system.file("mssql_sqljdbc4.jar", package = "mmkit"),
                    identifier.quote = "`")

 RJDBC::dbConnect(drv = drv, str_form("jdbc:sqlserver://{{host}}:{{port}}",
                                      host = creds$host, port = creds$port),
                  user = creds$user, password = creds$password)
}


#' @title conn_redshift
#' @description DBI interface for amazon redshift.
#' @param creds A list with entries for db_name, host, user, password, and port.

conn_redshift <- function(creds){

 if (!"RJDBC" %in% as.character(installed.packages(fields = "Name")[,1])) {
  stop("No RJDBC installation found...Please install before trying again.")
 }

 drv <- RJDBC::JDBC("com.amazon.redshift.jdbc42.Driver",
                    system.file("RedshiftJDBC42-no-awssdk-1.2.10.1009.jar",
                                package = "mmkit"),
                    identifier.quote = "`")

 dbcon = DBI::dbConnect(drv = drv, paste0("jdbc:redshift://", creds$host),
                        port = creds$port, database = creds$db_name,
                        UID = creds$user, PWD = creds$password,
                        ConnSchema = creds$db_name)

 dbplyr::src_dbi(con = dbcon)
}

#' @title conn_mysql
#' @description DBI interface for mysql.
#' @param creds A list with entries for db_name, host, user, password, and port.

conn_mysql <- function(creds){

 if (!"RMySQL" %in% as.character(installed.packages(fields = "Name")[,1])) {
  stop("No RMySQL installation found...Please install before trying again.")
 }

 dplyr::src_mysql(dbname = creds$db_name, host = creds$host,
                  port = creds$port, user = creds$user,
                  password = creds$password)

}


#' @title conn_psql
#' @description DBI interface for postgresql.
#' @param creds A list with entries for db_name, host, user, password, and port.
#' @import RPostgreSQL

conn_psql <- function(creds){
 dplyr::src_postgres(dbname = creds$db_name, host = creds$host,
                     port = creds$port, user = creds$user,
                     password = creds$password)
}

#' @title conn_rpsql
#' @description DBI interface for PostgreSQL using the RPostgres library.
#' @param creds A list with entries for db_name, host, user, password, and port.

conn_rpostgres <- function(creds){
 # Based on https://github.com/hadley/dplyr/issues/2292

 if (0 == length(find.package("RPostgres", quiet = TRUE))) {
  stop("RPostgres not found. Install from http://github.com/rstats-db/RPostgres")
 }

 "%||%" <- function(x, y) if (is.null(x)) y else x

 db_disconnector <- function(con, name, quiet = FALSE) {
  reg.finalizer(environment(), function(...) {
   if (!quiet) {
    message("Auto-disconnecting ", name, " connection ",
            "(", paste(con@Id, collapse = ", "), ")")
   }
   dbDisconnect(con)
  })
  environment()
 }

 src_postgres2 <- function(dbname = NULL, host = NULL, port = NULL, user = NULL,
                           password = NULL, ...) {
  if (!requireNamespace("RPostgres", quietly = TRUE)) {
   stop("RPostgres package required to connect to postgres db", call. = FALSE)
  }

  user <- user %||% ""

  con <- dbConnect(RPostgres::Postgres(), host = host %||% "", dbname = dbname %||% "",
                   user = user, password = password %||% "", port = port %||% "", ...)
  info <- dbGetInfo(con)

  dbplyr::src_sql("postgres", con, info = info,
          disco = db_disconnector(con, "postgres"))
 }

 if (!require(dplyr)) {
  warning("dplyr not available; could not redefine dplyr::src_postgres")
  return()
 }

 unlockBinding("src_postgres", as.environment("package:dplyr"))
 assignInNamespace("src_postgres", src_postgres2, ns = "dplyr",
                   as.environment("package:dplyr"))
 assign("src_postgres", src_postgres2, "package:dplyr")
 lockBinding("src_postgres", as.environment("package:dplyr"))
 warning("dplyr::src_postgres has been redefined to use RPostgres!")

 dplyr::src_postgres(dbname = creds$db_name, host = creds$host,
                     port = creds$port, user = creds$user,
                     password = creds$password)
}
