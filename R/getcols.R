#' @title getcols
#' @description SQL Convenience function. Gets column names and types from a specified table
#' @param table Character. The table to query (Schema optional)
#' @param long Logical. Should the data be displayed in long format? Defaults to FALSE
#' @param conn Connection object.
#' @export


getcols <- function(table, long = FALSE, conn = NULL) {
    if (is.null(conn)) {
        conn <- findConn()
    }

    statement <- "SELECT column_name col, udt_name rs_type
                  FROM information_schema.columns
                  {{where_clause}}"

    if (grepl("\\.",table)) {
        table  <- strsplit(table,"\\.")[[1]]
        params <- sprintf(" WHERE table_schema = '%s' AND table_name = '%s'
                          ORDER BY ordinal_position",
                          table[1], table[2])
    } else {
        params <- sprintf(" WHERE table_name = '%s'
                          ORDER BY ordinal_position",
                          table)
    }

    res <- getrs(conn = conn, statement, where_clause = params)

    if (long) { return(res) }

    res <- data.frame(t(res))
    names(res) <- res[1, ]
    return(res[-1, ])
}
