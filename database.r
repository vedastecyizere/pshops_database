#connecting to the data warehouse

source("settings.R")

library(odbc)
library(dplyr)

con <- dbConnect(odbc(),
                 Driver = "SQL Server",
                 Server = db_server,
                 Database = db_name,
                 UID    = db_user,
                 PWD    = db_pass)