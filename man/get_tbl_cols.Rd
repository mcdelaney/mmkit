% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/get_tbl_cols.R
\name{get_tbl_cols}
\alias{get_tbl_cols}
\title{get_tbl_cols}
\usage{
get_tbl_cols(table_name, conn, match_string = NULL)
}
\arguments{
\item{table_name}{Full schema and table name}

\item{conn}{A valid redshift connection.  defaults to rs}

\item{match_string}{Optional regex string to match column names against.
If specified, only returns matching names}
}
\description{
A funtion to return a character vector of database table colum names.
}
