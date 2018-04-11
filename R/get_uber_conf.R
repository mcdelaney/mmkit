#' @title get_uber_conf
#' @description Collects and consolidates uber-conf program files from github.
#' @param access_token Personal access token to github request.
#' Defaults to Sys.getenv('GITHUB_PAT')
#' @param branch Optional; branch of uber-conf repo from which to pull.
#' Default = master.
#' @param keep_prelaunch_programs Boolean value indicating whether unlaunched programs
#' should be returned in list of objects.  Default=FALSE.
#' @param force_update Force uberconf to get new data. Default = TRUE
#' @import jsonlite
#' @export


get_uber_conf <- function(access_token = Sys.getenv("GITHUB_PAT"),
                          branch = 'master',
                          keep_prelaunch_programs = FALSE,
                          force_update = TRUE) {

 tryCatch({
  message("Collecting uber-conf files from github...")
  uber_file_loc <- paste0(system.file(package = "mmkit"), "/uber-conf.json")

  message("Requesting most recent commit...")
  check_commit <- httr::RETRY(verb="GET", times = 3,
   url = sprintf("https://api.github.com/repos/2uinc/uber-conf/git/refs/heads/%s", branch),
   httr::authenticate(user = "username", password = access_token))

  message("Checking for cached file...")
  if (file.exists(uber_file_loc)) {
   file = jsonlite::fromJSON(uber_file_loc)
   if (!force_update && "last_commit_hash" %in% names(file) && check_commit$status == 200 &&
       file$last_commit_hash == httr::content(check_commit)$object$sha) {
    message("No commits since last pull... using cached copy...")
    file <- file[!names(file) %in% "last_commit_hash"]
    return(file)
   }
  }

  tmpdir = paste0(tempdir(), "/uberconf_", uuid())
  message("Cached file not found... pulling from github to: ", tmpdir)
  system(str_form("git clone --depth 1 \\
                  https://username:{{token}}@github.com/2uinc/uber-conf.git \\
                  --branch={{branch}} {{tmp_dir}}",
                  token = access_token, tmp_dir = tmpdir, branch = branch))
  message("Data collected successfully...Reading file...")
  json_files <- list.files(paste0(tmpdir, "/data"), pattern = "*.json")
  if (length(json_files) < 20) {
   stop("Error collecting uber-conf data...")
  }

  data = list()
  for (file in json_files) {
   tmp = fromJSON(paste0(tmpdir, "/data/", file))
   if (keep_prelaunch_programs == FALSE && 'pre_launch' %in% names(tmp) &&
       tmp$pre_launch == TRUE) {
    message(sprintf('Dropping pre-launch program: %s', file))
   }else{
    data[[gsub("\\.json", "", file)]] <- tmp
   }
  }

  if (!is.null(httr::content(check_commit)$object$sha)) {
   data$last_commit_hash <- httr::content(check_commit)$object$sha
  }

  writeLines(toJSON(data, auto_unbox = TRUE, pretty = TRUE, null = "null"),
             uber_file_loc)
  unlink(tmpdir, recursive = TRUE)

  return(data[!names(data) %in% "last_commit_hash"])
 }, error = function(e) {
  warning(geterrmessage())
  warning("Error collecting uber-conf from github... trying local cache...")
  Sys.sleep(sample(100:500, size = 1)/100)
  file = fromJSON(uber_file_loc)
  if (length(file) < 2) {
   stop("Uber-conf file not found...")
  }
  message("Local cache read successfully...")

  return(file)
 })

}
