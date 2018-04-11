#' @title UberConf wrapper
#' @name UberConfWriter
#'
#' @description OO API for modifying and writing UberConf
#' @docType class
#' @importFrom R6 R6Class
#' @export
#' @format \code{\link{R6Class}} object.
#' @param uber an object of class \code{UberConf}
#' @param pdo Program degree offering like \code{unc-mba}
#' @param key_value list like \code{list(sis=list(sis_bucket="au-mba"))}
#' @param path Path on disk, passed through \code{path.expand}
#' @param keep.null If false, removes entries that are set to NULL
#' @examples
#' \dontrun{
#' uw <- UberConfWriter$new(UberConf$new())
#' n <- list(sis = list(sis_bucket = NULL, university_id = NULL))
#' uw$set_all_key_value(n)
#' uw$write_json_all("mydir")
#' }
NULL

#' @name new
#' @rdname UberConfWriter
#' @description \code{new} Constructor
NULL

#' @name get_all_pdo_names
#' @rdname UberConfWriter
#' @description \code{get_all_pdo_name} Get all known program degree offering
#' names
NULL

#' @name get_pdo
#' @rdname UberConfWriter
#' @description \code{get_pdo} Get full UberConf tree for some program degree
#' offering. Does not compute any derived values.
NULL

#' @name set_all_key_value
#' @rdname UberConfWriter
#' @description \code{set_all_key_value} Sets a constant value for all program
#' degree offerings.
NULL

#' @name set_pdo_key_value
#' @rdname UberConfWriter
#' @description \code{set_pdo_key_value} Sets a value for one program degree
#' offering.
NULL

#' @name write_json_all
#' @rdname UberConfWriter
#' @description \code{write_json_all} Writes JSON files for all program degree
#' offerings.
NULL

#' @name write_json_pdo
#' @rdname UberConfWriter
#' @description \code{write_json_pdo} Writes JSON file for one program degree
#' offering.
NULL

UberConfWriter <- R6::R6Class(
  "UberConfWriter",

  public = list(
    initialize = function(uber) {
      stopifnot("UberConf" %in% class(uber))
      private$all$uber <- uber$get_all()
    },

    get_all_pdo_names = function() {
      names(private$all$uber)
    },

    get_pdo = function(pdo) {
      private$all$uber[[pdo]]
    },

    set_all_key_value = function(key_value, keep.null=TRUE) {
      all_names <- self$get_all_pdo_names()
      for (pdo in all_names) {
        self$set_pdo_key_value(pdo, key_value, keep.null)
      }
    },

    set_pdo_key_value = function(pdo, key_value, keep.null=TRUE) {
      dat <- self$get_pdo(pdo)
      dat <- modifyList(dat, key_value, keep.null=keep.null)
      # Move "fields" to the end
      fields <- dat$fields
      dat$fields <- NULL
      dat$fields <- fields
      private$all$uber[[pdo]] <- dat
    },

    write_json_all = function(path="data") {
      all_names <- self$get_all_pdo_names()
      for (pdo in all_names) {
        self$write_json_pdo(path, pdo)
      }
    },

    write_json_pdo = function(path, pdo) {
      dat <- self$get_pdo(pdo)
      out <- jsonlite::toJSON(dat, auto_unbox = T, pretty = T, null = "null")
      f <- path.expand(file.path(path, paste0(pdo, ".json")))
      writeLines(out, f)
    }
  ),

  private = list(
    all = list()
  )
)
