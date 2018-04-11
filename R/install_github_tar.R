#' @title install_github_tar
#' @description Installs a repo from github by cloning the most recent release,
#' then installing from the local tarball.  This method avoids problems associated
#' with devtools::install_github when packages have private github sourced imports.
#' Primarily designed for use in deployment.
#' @param  repo Name of the repo that should be installed.
#' @param  access_token A github pat or other valid access token to use if repo is private.
#' By default, checks for environment variable GITHUB_PAT.
#' @param release Version to install; Defaults to latest.
#' @param  owner Owner of the repo; Defaults to 2uinc.
#' @export

install_github_tar <- function(repo, access_token = Sys.getenv("GITHUB_PAT"),
 release = "latest", owner = "2uinc") {
 auth_ = httr::authenticate("token" , access_token)
 url = str_form("https://api.github.com/repos/{{owner}}/{{repo}}/releases/{{version}}",
  owner = owner, repo = repo, version = release)
 message('Requesting latest release from: \n\t', url)
 release = httr::GET(url, auth_)
 httr::stop_for_status(release)
 release_content = httr::content(release)

 if (!"tarball_url" %in% names(release_content)) {
  stop(sprintf("Release not found for repo: %s/%s with release: %s",
   owner, repo, release))
 }else{
  download_url <- release_content$tarball_url
 }

 save_loc <- str_form("{{dir}}/{{repo}}.tar.gz",
                      dir = tempdir(), repo = repo)
 message("Release found at : \n\t", download_url)
 message("Saving to: ", save_loc)

 httr::GET(download_url, httr::write_disk(save_loc), auth_)
 install.packages(save_loc, repos = NULL)
 file.remove(save_loc)
 message("Installation Successful")
}
