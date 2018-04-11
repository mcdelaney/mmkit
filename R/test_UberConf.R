settings <- list(
   path = path.expand("~/git/uber-conf/data")
)

expected <- list(
  pdo_names = c("au-mba", "au-mir", "den-mba", "den-msw", "for-edu", "gu-msn",
    "gwu-mha", "gwu-mph", "hu-cba", "nu-mac", "nyu-mac", "nyu-mhc",
    "nyu-msl", "nyu-otd", "pep-mls", "pep-psy", "rice-mba", "sc-aba",
    "sc-com", "sc-mba", "sc-mph", "sc-msn", "sc-msw", "smu-mds",
    "syr-eng", "syr-ist", "syr-law", "syr-mac", "syr-mba", "syr-mids",
    "syr-mpa", "ucb-cyb", "ucb-mids", "ud-mba", "unc-mba", "unc-mpa",
    "usc-des", "usc-dpt", "usc-mat", "usc-msc", "usc-msn", "usc-msw",
    "vu-edu", "wu-llm", "yu-med")
)

test_UC_new_global_github = function() {
  u <- UberConf$new()
  have <- u$get_all_pdo_names()
  all(expected$pdo_names %in% have)
}

test_UC_new_local = function() {
  u <- UberConf$new(global=FALSE, path=settings$path)
  have <- u$get_all_pdo_names()
  all(expected$pdo_names %in% have)
}

test_UC_write = function() {
  u <- UberConf$new(global=FALSE, path=settings$path)
  uw <- UberConfWriter$new(u)
  tmpdir = paste0(tempdir(), "/uberconf_test_", mmkit::uuid())
  dir.create(tmpdir)
  uw$write_json_all(path=tmpdir)
  u2 <- UberConf$new(global=FALSE, path=tmpdir)
  identical(u$get_all(), u2$get_all())
}

test_UC_set_null = function() {
  u <- UberConf$new(global=FALSE, path=settings$path)
  uw <- UberConfWriter$new(u)
  tmpdir = paste0(tempdir(), "/uberconf_test_", mmkit::uuid())
  dir.create(tmpdir)
  uw$set_all_key_value(key_value=list(lms_schema_prod=NULL))
  uw$write_json_all(path=tmpdir)
  u2 <- UberConf$new(global=FALSE, path=tmpdir)
  all(is.null(unlist(u2$get_all_value("lms_schema_prod"))))
}