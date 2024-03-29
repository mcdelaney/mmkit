% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/coalesce.R
\name{coalesce}
\alias{coalesce}
\title{coalesce}
\usage{
coalesce(...)
}
\arguments{
\item{...}{A list of values to coalesce, in order of preference.}
}
\description{
Function to coalesce a vector to a single value, taking the
first non-NA entry, e.g., coalesce(NA, NA, "California") returns "California".
}
