#' @title clean_country
#' @description Conforms country names by matching against a number of internatinoal standards.
#' Specifically: is02c, iso3c, FIPS, cocw, and IOC.  Additionally, can return continent and region.
#' @param  country A vector of unconformed state values.
#' @param name_type One of country_name, continent, or region.  Defaults to country_name.
#' @param return_original Should the input value be returned if a match cannot be found?
#' Defaults to FALSE.  If no match found, NA is returned.
#' @export


clean_country <- function(country, name_type = "country_name", return_original = FALSE) {
  
  if (!name_type %in% c("country_name", "region", "continent")) {
    stop("Invalid use of name_type parameter. Must be one of: continent, region, or country_name")
  }
  
  country_data <- readRDS(paste0(system.file(package = "mmkit"), "/country_data.Rds"))
  
  country_df <- data.frame(orig = country)
  country_unique = data.frame(orig = unique(country), new = NA)
  
  country_unique$new <- unlist(lapply(country_unique$orig, FUN = function(X) {
    if (X %in% c(NA, "")) {
      ifelse(return_original, country, NA)
      return(NA)
    }else{
      tryCatch({
        val = country_data$data[match(X, country_data$data$value),][[name_type]]
        if (is.na(val)) {
          val = country_data$regex[grepl(X, country_data$regex$value, ignore.case = TRUE),][[name_type]][1]
        }
        return(val)
      }, error = function(e) {
        ifelse(return_original, country, NA)
      })
    }
  }))
  
  country_df <- dplyr::left_join(country_df, country_unique, by = "orig")
  
  return(country_df$new)
}



#' @title clean_state
#' @description Converts messy state data to abbreviated state names.
#' @param  state A vector of unconformed state values.
#' @param no_match_val The value to be returned if input value cannot be matched.
#' Defaults to 'unmatched'
#' @param name_type Type of state value to be returned.
#'  One of: abbrev, division, region, or full_name.
#'  Defaults to abbrev.
#' @export


clean_state <- function(state, no_match_val = "unmatched", name_type = "abbrev"){
  
  if (!any(name_type %in% c("abbrev", "division", "region", "full_name"))) {
    stop("Incorrect name_type. Must be one of: division, region, abbrev, or full_name!")
  }
  
  ## Create lookup data for matching ##
  state_lookup <- data.frame(
    abbrev = c(rep(c(as.character(state.abb), "DC"), 2), "blank"),
    full_name = c(rep(c(as.character(state.name), "District of Columbia"), 2), "blank"),
    division = c(rep(c(as.character(state.division), "South Atlantic"), 2), "blank"),
    region = c(rep(c(as.character(state.region), "South"), 2), "blank"),
    lookup = tolower(c(state.name, "district of columbia", state.abb, "DC", "blank"))
  )
  
  ## Recode stupid values + remove periods and leading/lagging whitespace ##
  state[is.na(state) | state %in% c("","NA")] <- "blank"
  state <- tolower(gsub("^\\s+|\\s+$|\\.", "", state))
  
  if (is.null(name_type)) {
    state <- unlist(state_lookup$full_name[match(state, state_lookup$lookup)])
  }else{
    state <- unlist(state_lookup[[name_type]][match(state, state_lookup$lookup)])
  }
  
  ## Recode blanks or NA to whatever no_match_val argument specifies ##
  state[is.na(state) | state %in% "blank"] <- no_match_val
  
  return(state)
  
}



#' @title extract_numeric
#' @description Converts character value to numeric by removing character and or
#' punctuation, splitting and averaging if the value is a range (eg: 2.5-3.0 = 2.75),
#' and imposing a cap on value size.  Also allows the setting of a limit above which NA
#' should be returned.  Is intended to be used for conforming gpa, work experience,
#' and other values where numbers are represented as strings that may or may not be a range.
#' @param x A vector of unconformed values.
#' @param max_value The maximum value that should be returned. Defaults to infinity.
#' eg: for gpa values, the cap should be 4.0.
#' @param na_cap A numeric above which NA sbould be returned. Defaults to null. Should
#' be used to invalidate values that don't make sense for the context; eg: a gpa value = 75.

extract_numeric <- function(x, max_value = Inf, na_cap = NULL){
  
  if (!length(x)) return(x)
  
  x <- gsub("[A-z]", "", x)
  x_split <- strsplit(x, "[^0-9.]")
  x_num <- lapply(x_split, as.numeric)
  x_num <- round(sapply(x_num, mean, na.rm = TRUE), 2)
  x_num[is.nan(x_num)] <- NA
  
  if (all(is.na(x_num))) return(x_num)
  if (!is.null(na_cap)) {
    x_num <- ifelse(!is.na(x_num ) & x_num <= na_cap, pmin(max_value, x_num), NA)
  }else{
    x_num <- pmin(max_value, x_num)
  }
  
  return(x_num)
}


#' @title impute_gender
#' @description Imputes expected gender based on first names from social security
#' database.
#' @param  names A list of first names against which to match.
#' @param  fill.na Optionally replace missing values with string
#' @export

impute_gender <- function(names, fill.na = NA){
  # Format input vector
  names <- gsub(" .*", "", tolower(names), perl = TRUE)
  # Match names to reference data frame
  genders <- mmkit::classified_names$gender[match(names, mmkit::classified_names$firstname)]
  if (!is.na(fill.na)) genders <- ifelse(is.na(genders), fill.na, genders)
  return(genders)
}

