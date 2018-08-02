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
                          database_type = "postgres", init_sql = NA, dataset=NA) {
      creds <- mmkit::read_creds(database, cred_location=cred_location)
      
      message(sprintf("Creating connection to %s.%s", database, creds$db_name))
      
      database_type <- ifelse(is.null(creds[['database_type']]), 
                              database_type, creds$database_type)
      
      connection <<- switch(database_type,
                            "postgres" = conn_postgres(creds = creds),
                            "redshift" = conn_redshift(creds = creds),
                            "mysql" = conn_mysql(creds = creds),
                            "ms_sql" = conn_mssql(creds = creds),
                            "bigquery" = conn_bigquery(creds = creds, 
                                                       cred_location = cred_location,
                                                       dataset = dataset),
                            stop("Database type must be postgres, rpostgres,
                              mysql, redshift or ms_sql"))
      
      
      con <<- connection
      
      if (!is.na(init_sql)) {
        message(sprintf('Executing sql: %s', init_sql))
        .self$send(init_sql)
      }
      
    },
    
    #' @title read_sql
    #' @description Read a sql statement from disk.
    read_sql = function(filepath){
      "Read a sql file from disk."
      paste0(readLines(filepath), collapse='\n')
    },
    #' @title query
    #' @description Execute arbitrary sql, expecting a dataframe in response.
    #' Str_form substitution params supported by default.
    #' @export
    query = function(statement_or_path, ...){
      "Execute query using initialized database connection. Str_form templating is supported by default."
      
      if (stringr::str_sub(statement_or_path, start = (-4))=='.sql' && file.exists(statement_or_path)) {
        statement = .self$read_sql(statement_or_path)  
      }else{
        statement = statement_or_path
      }
      
      DBI::dbGetQuery(conn = .self$connection, str_form(statement, ...))
    },
    
    #' @title disconnect
    #' @description Disconnect from current database.
    #' @export
    disconnect = function(){
      DBI::dbDisconnect(self$con)
      return(invisible())
    },
    
    #' @title send
    #' @description Execute arbitrary sql, expecting no response.
    #' Str_form substitution params supported by default.
    #' @export
    send = function(statement_or_path, ...){
      "Send a statement using initialized database connection. Str_form templating is supported by default."
      
      if (stringr::str_sub(statement_or_path, start = (-4))=='.sql' && file.exists(statement_or_path)) {
        statement = .self$read_sql(statement_or_path)  
      }else{
        statement = statement_or_path
      }
      
      DBI::dbExecute(conn = .self$connection, str_form(statement, ...))
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
        cols = DBI::dbGetQuery(conn = .self$connection,
                               str_form("SELECT column_name AS col
                                        FROM information_schema.columns
                                        WHERE table_name = '{{table}}' AND
                                        table_schema = '{{schema}}'
                                        ORDER BY ordinal_position",
                                        table = table, schema = schema))$col
        
      }else{
        
        cols = DBI::dbGetQuery(conn = .self$connection,
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
    get_tables = function(schema=NA, match_string=NA, exclude_system=TRUE) {
      
      
      conditions = c(ifelse(is.na(schema), NA, 
                            str_form("table_schema in ('{{s}}')", s = schema)),
                     ifelse(!exclude_system, NA, 
                            "table_schema not in ('information_schema', 'pg_catalog')")
      )
      
      if (!all(is.na(conditions))) {
        where <- paste0("WHERE ", paste0(conditions[!is.na(conditions)], collapse = " AND "))
      }else{
        where = ''
      }
      
      tables = DBI::dbGetQuery(conn = .self$connection,
                               str_form("SELECT table_schema, table_name
                                        FROM information_schema.tables
                                        {{where}}", where = where))
      
      if (is.na(schema)) {
        tables$table_name = paste(tables$table_schema, tables$table_name, sep = ".")
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
  
  if ('database' %in% names(creds)) {
    creds$db_name <- creds$database
    creds <- creds[!names(creds) %in% "database"]
  }
  
  if (database %in% names(creds)) {
    creds <- creds[[database]]
  }
  
  if ('dbname' %in% names(creds)) {
    creds[['db_name']] = creds[['dbname']]
  }
  
  if ('cluster_name' %in% names(creds)) {
    creds$host <- creds$cluster_name
    creds <- creds[!names(creds) %in% "cluster_name"]
  }
  
  if ('username' %in% names(creds)) {
    creds$user <- creds$username
    creds <- creds[!names(creds) %in% "username"]
  }
  
  db_sub_params <- list(...)
  
  if (length(db_sub_params) > 0) {
    creds <- lapply(X = creds, FUN = function(X) {
      str_form(X, db_sub_params)
    })
  }
  
  # if (!all(required_fields %in% names(creds))) {
  #   stop(sprintf("Credentials entry for %s missing keys: %s!",
  #                database, paste0(required_fields[!required_fields %in% names(creds)],
  #                                 collapse = ", ")))
  # }
  
  return(creds)
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
  
  url <- sprintf("jdbc:redshift://%s:%s/%s?tcpKeepAlive=true&ssl=true&sslfactory=com.amazon.redshift.ssl.NonValidatingFactory", 
                 creds$host, creds$port, creds$db_name)
  
  DBI::dbConnect(drv, url, creds$user, creds$password)
}

#' @title conn_mysql
#' @description DBI interface for mysql.
#' @param creds A list with entries for db_name, host, user, password, and port.

conn_mysql <- function(creds){
  
  if (!"RMySQL" %in% as.character(installed.packages(fields = "Name")[,1])) {
    stop("No RMySQL installation found...Please install before trying again.")
  }
  
  DBI::dbConnect(drv = RMySQL::MySQL(), 
                 dbname = creds$db_name, host = creds$host,
                 port = creds$port, user = creds$user,
                 password = creds$password)
  
}


#' @title conn_postgres
#' @description DBI interface for postgresql.
#' @param creds A list with entries for db_name, host, user, password, and port.
#' @import RPostgres

conn_postgres <- function(creds){
  
  if (!"RPostgres" %in% as.character(installed.packages(fields = "Name")[,1])) {
    stop("No RPostgres installation found...Please install before trying again.")
  }
  
  DBI::dbConnect(drv = RPostgres::Postgres(),
                 dbname = creds$db_name, host = creds$host,
                 port = creds$port, user = creds$user,
                 password = creds$password)
}


#' @title conn_bigquery
#' @description DBI interface for bigquery.
#' @param creds Path to a service account token with additional keys for project_id, and dataset.
#' @import bigrquery

conn_bigquery <- function(creds, cred_location, dataset){
  
  if (!"bigrquery" %in% as.character(installed.packages(fields = "Name")[,1])) {
    stop("No bigrquery installation found...Please install before trying again.")
  }
  
  message("Setting service account token...")
  bigrquery::set_service_token(cred_location)
  
  message("Creating bigquery connection to dataset: ", dataset)
  DBI::dbConnect(drv = bigrquery::bigquery(), 
                 project = creds$project_id,
                 dataset = dataset,
                 billing=creds$project_id)

}
