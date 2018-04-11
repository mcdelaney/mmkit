#' @title UberConf wrapper
#' @name UberConf
#'
#' @description OO API for reading UberConf
#' @docType class
#' @importFrom R6 R6Class
#' @export
#' @format \code{\link{R6Class}} object.
#' @param ... Passed to \code{\link{get_uber_conf}}
#' @param pdo Program degree offering like \code{unc-mba}
#' @param key UberConf key like \code{salesforce_org}
#' @param path Path on disk, passed through \code{path.expand}
#' @param global Read UberConf data into an environment, so there's
#' a global copy. If FALSE, read data into a list, i.e., a copy that
#' only exists within one instance of this class.
#' @param reload Force a reload of a global copy
#' @examples
#' \dontrun{
#' u <- UberConf$new() # Check out from GitHub
#' u <- UberConf$new(path="~/git/uber-conf/data") # Read from disk
#' u$get_pdo_value("unc-mba", "salesforce_org") # Key defined in UberConf
#' u$get_pdo_value("unc-mba", "segment_site") # Key yields derived value
#' }
NULL

#' @name new
#' @rdname UberConf
#' @description \code{new} Constructor
NULL

#' @name get_all_pdo_names
#' @rdname UberConf
#' @description \code{get_all_pdo_name} Get all known program degree offering
#' names
NULL

#' @name get_pdo
#' @rdname UberConf
#' @description \code{get_pdo} Get full UberConf tree for some program degree
#' offering. Does not compute any derived values.
NULL

#' @name get_all_value
#' @rdname UberConf
#' @description \code{get_all_value} Get UberConf value across all program
#' degree offerings. Computes derived values as in \code{get_pdo_value}.
NULL

#' @name get_pdo_value
#' @rdname UberConf
#' @description \code{get_pdo_value} Get UberConf value for a program degree
#' offering and specific key. Computes derived values for some known keys.
NULL

UberConf <- R6::R6Class(
  "UberConf",

  public = list(

    initialize = function(..., global=TRUE, reload=FALSE, path=NULL) {
      private$all <- if (global) new.env() else list()

      if (! global | reload | (global & is.null(private$all$uber))) {
        private$all$uber <- if (is.null(path)) {
          mmkit::get_uber_conf(...)
        } else {
          dots <- list(...)
          args <- list(path=path)
          if ("keep_prelaunch_programs" %in% names(dots)) {
            args$keep_prelaunch_programs <- dots$keep_prelaunch_programs
          }
          do.call(self$load_from_path, args)
        }
      }
    },

    load_from_path = function(path, keep_prelaunch_programs=FALSE) {
      path <- path.expand(path)
      json_files <- list.files(path, pattern = "*.json")
      if (length(json_files) < 20) {
        stop("Error collecting uber-conf data...")
      }
      json = list()
      for (f in json_files) {
        tmp = jsonlite::fromJSON(file.path(path, f))
        if (keep_prelaunch_programs == FALSE && 'pre_launch' %in% names(tmp) &&
            tmp$pre_launch == TRUE) {
         message(sprintf('Dropping pre-launch program: %s', f))
        } else {
          json[[gsub("\\.json", "", f)]] <- tmp
        }
      }
      json$last_commit_hash <- NULL
      json
    },

    get_all = function() {
      private$all$uber
    },

    get_all_pdo_names = function() {
      names(private$all$uber)
    },

    get_pdo = function(pdo) {
      private$all$uber[[pdo]]
    },

    get_all_value = function(key) {
      sapply(self$get_all_pdo_names(), function(pdo) self$get_pdo_value(pdo, key))
    },

    get_pdo_value = function(pdo, key) {
      # Some keys are defined here programmatically; Uber-Conf as back-up
      switch(
        key,
        db_lms_hostname = {
          paste0(pdo, "-lms-dbm-prod.c9x6oqufralp.us-west-2.rds.amazonaws.com")
        },
        db_lms_schema = {
          sf_org <- self$get_pdo_value(pdo, "salesforce_org")
          prefix <- gsub(pattern="-", replacement="_", sf_org, fixed=TRUE)
          paste0(prefix, "_lms_prod")
        },
        segment_schema = {
          prefix <- gsub(pattern="-", replacement="_", pdo, fixed=TRUE)
          paste0(prefix, "_lms_prod")
        },
        segment_site = {
          sf_org <- self$get_pdo_value(pdo, "salesforce_org")
          paste0(sf_org, "-lms-prod.2u.com")
        },
        private$all$uber[[pdo]][[key]]
      )
    }
  ),

  private = list(
    all = NULL
  )
)
