#' BQReader
#' @docType class
#' @description Bigquery database interface.
#' @param  py_version_path Path to the python installation
#' @import DBI
#' @importFrom R6 R6Class
#' @import reticulate
#' @import arrow
#' @name BQReader
#' @export
#' @return Object of \code{\link{R6Class}} with methods for query execution.
#' @format \code{\link{R6Class}} object.

BQReader <- R6::R6Class("BQReader", list(
  bq = NA,
  initialize = function(py_version_path = NA) {
    if (is.na(py_version_path)) {
      path = Sys.getenv("PY_PATH")
      if(path == "") {
        path = "python3"
      }
    } else{
      path = py_version_path
    }
    message("Using python path: ", path)
    reticulate::use_python(path, required = TRUE)
    self$bq <- reticulate::import("etllib")$Big()
  },
  query = function(sql, ...) {
    result <- self$bq$query(mmkit::str_form(sql, ...), arrow = TRUE)
    return(data.frame(result))
  }))
