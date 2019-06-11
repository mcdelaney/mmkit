#' @title str_form
#' @description A poor mans version of python jinja2 templating
#' @param  file_or_string A path to a file, or a string that includes substitution parameters
#' @param ... An arbitrary number of named values corresponding to the substitution
#' params specified in the template. Can also be a named list.
#' @param variable_identifier Identifiers to denote paramters requiring substitution. Defaults to {{ and }}.
#' @param default_char default: pipe
#' @param is_sql If TRUE, str_form will collapse list arguments to string automatically.
#' @export

str_form <- function(file_or_string, ..., variable_identifier = c("{{", "}}"),
                     default_char = "|", is_sql=FALSE){
  
  if (!(grepl(variable_identifier[1], file_or_string, fixed = TRUE) &
        grepl(variable_identifier[2], file_or_string, fixed = TRUE))) {
    # message("No substitution params found in string. Returning as is...")
    return(file_or_string)
  }
  
  template <- read_template(file_or_string)
  
  params_requested <- vars_requested(template,
                                     variable_identifier = variable_identifier,
                                     default_char = default_char)
  
  params_supplied <- list(...)
  params_supplied <- params_supplied[!unlist(lapply(params_supplied, is.null))]
  
  if (inherits(params_supplied[[1]], "list") || 
      inherits(params_supplied[[1]], "environment")) {
    params_supplied <- params_supplied[[1]]
  }
  
  for (param in names(params_requested)) {
    
    if (is_sql && length(params_supplied[[param]]) > 1) {
      params_supplied[[param]] = mmkit::sql_lst(params_supplied[[param]])
    }
    
    pattern <- paste0(variable_identifier[1],
                      "\\s*?",
                      param,
                      "\\s*?" ,
                      variable_identifier[2],
                      "|",  # or match with default in place
                      variable_identifier[1],
                      "\\s*?",
                      param,
                      "\\s*?\\",
                      default_char,
                      ".*?",
                      variable_identifier[2])
    
    if (param %in% names(params_supplied)) {
      template <- gsub(pattern, params_supplied[[param]], template, perl = TRUE)
    } else if (!is.na(params_requested[[param]])) {
      template <- gsub(pattern, params_requested[[param]], template, perl = TRUE)
      warning(paste0("Requested parameter '", param, "' not supplied -- using default variable instead"))
    } else {
      warning(paste0("Requested parameter '", param, "' not supplied -- leaving template as-is"))
    }
    
    
  }
  
  template
}

#' @title vars_requested
#' @description Variables requested by str_form template
#' @param  file_or_string A path to a file, or a string that includes substitution parameters
#' @export

vars_requested <- function(file_or_string, 
                           variable_identifier = c("{{", "}}"),
                           default_char = "|"){
  
  template <- mmkit:::read_template(file_or_string)
  
  regex_expr <- paste0(variable_identifier[1], "(.*?)", variable_identifier[2])
  
  params <- regmatches(template, gregexpr(regex_expr, template, perl = TRUE))[[1]]
  
  params <- gsub(regex_expr, "\\1", params, perl = TRUE)
  
  params_splitted <- strsplit(params, default_char, fixed = TRUE)
  
  param_list <- list()
  
  for (param in params_splitted) {
    key <- mmkit:::trim(param[[1]])
    if (length(param) > 1) {
      value <- mmkit:::trim(param[[2]])
    } else{
      value <- NA
    }
    param_list[key] <- value
  }
  
  param_list
}

### DEFINE SOME STUFF
read_template <- function(file_or_string) {
  # check if input is file, else assume it is a string
  if (is.character(file_or_string) && file.exists(file_or_string)) {
    template <- paste0(readLines(file_or_string),collapse = "\n")
  } else if (is.character(file_or_string) && nchar(file_or_string > 0)) {
    template <- file_or_string
  } else {
    stop("the thing you gave as input does not exist...")
  }
  template
}

trim <- function(x) {gsub("^\\s+|\\s+$", "", x) }
