#' @title simpleCap
#' @description Utility to convert strings to proper case.

simpleCap <- function(x) {
 s <- strsplit(x, " ")[[1]]
 paste(toupper(substring(s, 1,1)), substring(s, 2),
       sep = "", collapse = " ")
}


#' @title convert_git_url
#' @description Utility to convert git ssh address to https.

convert_git_url <- function(x) {
 x <- gsub("git@github.com:", "https://github.com/", x, fixed = T)
 x <- gsub("\\.git$", ".com", x, fixed = T)
 return(x)
}


#' @title read_text
#' @description Utility to read text file from package inst directory,
#' and collapse with newline

read_text <- function(x) {
 paste0(readLines(
  paste0(system.file(package = "mmkit"), x)), collapse = "\n")
}
