#' @title findConn
#' @description Finds and returns an active dplyr Redshift connection object
#' @param env The environment to search in. Defaults to .GlobalEnv
#' @import dplyr
#' @import RPostgreSQL
#' @export


findConn <- function(env = .GlobalEnv) {
    pos <- Filter(
        function(x) {
            grepl("bi-rs-prod",x$info$host) && DBI::dbIsValid(x$con)
        },
        lapply(
            Filter(
                function(x) inherits(get(x, envir = env), c("src_postgres")),
                ls(envir = env)
            ),
            get,
            envir = env
        )
    )

    switch(length(pos) + 1,
           stop("No active Redshift connection detected", call. = F),
           return(pos[[1]])
    )
}
