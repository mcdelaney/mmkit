#' @title expand_grid
#' @description Does the same thing as expand.grid, just without the insane requirement
#' to specify stringsAsFactors = FALSE.
#' @param ... Arguments to be passed to expand.grid.
#' @export


expand_grid <- function(...) {
  expand.grid(...,stringsAsFactors = FALSE)
}
