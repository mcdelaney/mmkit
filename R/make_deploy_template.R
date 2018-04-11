#' @title make_deploy_template
#' @description Writes template file to deploy.yaml. Should be filled out before calling
#' deploy_project.
#' @param execution_files Paths to the files you want to execute in your job. Optional,
#' but must be added by hand later if not set.
#' @param output_path Path to deploy.yaml file that should be created.
#' Defaults to jenkins/deploy.yaml.
#' @param overwrite Remove existing file if found. Default = FALSE.
#' @export

make_deploy_template <- function(execution_files = NULL,
                                 output_path = "jenkins/deploy.yaml",
                                 overwrite = FALSE) {

 message(sprintf("Generating template file at %s...", output_path))

 if (file.exists(output_path)) {
  if (overwrite) {
   file.remove(output_path)
  }else{
   stop("deploy.yaml already exists!")
  }
 }

 validate_execution_files(execution_files = execution_files)
 dir.create("jenkins/", showWarnings = FALSE)

 tmp = mmkit:::read_text("/jenkins/deploy_template.yaml")

 tmp <- str_form(tmp, gather_project_info())
 job_tmp = mmkit:::read_text("/jenkins/deploy_job_tmp.txt")

 if (is.null(execution_files)) {
  tmp <- paste(tmp, job_tmp, sep = "\n")
 }

 for (file in execution_files) {

  job = gsub("\\.r$", "", file, ignore.case = T)
  if (grepl("run\\.r$", file, ignore.case = T)) {
   job = paste0(job, "_etl")
  }

  job_entry = str_form(job_tmp, file_path = file, job = job)
  tmp <- paste(tmp, job_entry, sep = "\n")

  args = parse_arguments(file)
  if (length(args) > 0) {
   for (i in 1:length(args)) {

    tmp <- paste(tmp, str_form("          - {{arg}}", arg = args[i]),
                 sep = ifelse(i == 1, "", "\n"))
   }
  }
 }

 writeLines(tmp, output_path)

 message(sprintf("Template file saved to %s...\n",output_path),
         "Check that it is accurate, then call deploy_project()")
}

#' @title gather_project_info
#' @description Use git2r to get project info to pre-populate deploy template.

gather_project_info <- function() {
 tryCatch({

  github_url = system("git config --get remote.origin.url", intern = TRUE)

  git_info = list(github_url = github_url,
                  project_name = gsub("^.*\\/|\\.git","",  github_url),
                  user_email = system("git config --get --global user.email",
                                      intern = TRUE),
                  branch = "master")

  return(git_info)
 }, error = function(e) {
  return(list(github_url = NULL, branch = NULL, project_name = NULL))
 })
}


#' @title validate_execution_files
#' @description Checks that files specified for execution exist.

validate_execution_files <- function(execution_files = NULL) {

 if (is.null(execution_files)) {
  return(message("No files specified... skipping validation"))
 }

 for (file in execution_files) {
  if (!(file.exists(file) &&
        grepl("make_option\\(", paste0(readLines(file), collapse = "\n")))) {
   stop(sprintf("Execution file %s does not exist or missing arguments!", file))
  }
 }

 message("Files validated successfully")
}

#' @title parse_arguments
#' @description Returns a list of optparse arguments from specified file

parse_arguments <- function(file) {
 message(sprintf("Parsing arugments for file %s", file))
 code = paste0(readLines(file), collapse = " ")
 args <- try({
  match_regex <- '\\"--[A-z]*?\\"'
  args = unlist(lapply(regmatches(code, gregexpr(match_regex, code))[[1]],
                       function(m) regmatches(m,regexec(match_regex, m))))
  args <- gsub('"|--',"", args)
 })

 if (inherits(args, 'try-error')) {
  stop(sprintf("Error parsing function arguments in file: %s", file))
 }
 message("Arguments found: \n", paste0(args, collapse = "\n"))
 return(args)

}

