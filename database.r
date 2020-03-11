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

#Let's learn some codes
clientinfo1 <- dbGetQuery(con,
                          "SELECT * FROM v_ClientSales WHERE (CountryName = 'Rwanda' AND SeasonName = '2020')")

clientbundles <- dbGetQuery(con,
                          "SELECT * FROM v_ClientBundles WHERE (CountryName = 'Rwanda' AND SeasonName = '2018')")


dim(clientinfo1)
table(clientinfo1$DistrictName, useNA = "ifany")
names(clientinfo1)
head(clientinfo1)
nrow(clientbundles)
length(unique(clientbundles$GlobalClientID[clientbundles$DistrictName == "RRT P-Shops"]))

names(clientbundles)
table(clientbundles$DistrictName, useNA = "ifany")
table(clientbundles$BundleName, useNA = "ifany")

#Now, let's traditionally load the RRT data - using roster data

# Load libraries
libs <- c("lubridate", "plyr", "zoo", "reshape2", "ggplot2", "dplyr","doBy","reshape")
lapply(libs, require, character.only = T)

###################################### my working Directories.
dd <- "D:/GitHub/pshops_database/data"
wd <- "D:/GitHub/pshops_database/rdata"
od <- "D:/GitHub/pshops_database/output"


sc <- read.csv(paste(dd, "rrt_sc_3.9.20.csv", sep = "/"), header = TRUE,   
               stringsAsFactors = FALSE, na.strings = "") 


vrl <- read.csv(paste(dd, "rrt_vr_light_3.9.20.csv", sep = "/"), header = TRUE,   
                stringsAsFactors = FALSE, na.strings = "") 

cb <- read.csv(paste(dd, "rrt_client_bundles_2.28.20.csv", sep = "/"), header = TRUE,   
                stringsAsFactors = FALSE, na.strings = "") 

pdb <- read.csv(paste(dd, "payment_deadline_by_bundle_3.9.20.csv", sep = "/"), header = TRUE,   
               stringsAsFactors = FALSE, na.strings = "")
pds <- read.csv(paste(dd, "payment_deadline_by_season_3.9.20.csv", sep = "/"), header = TRUE,   
               stringsAsFactors = FALSE, na.strings = "")

##################################################
#Part 0: Data preparation
################################################
dim(sc)
dim(cb)
dim(vrl)
View(vrl)
table(sc$DistrictName, useNA = "ifany")
table(cb$DistrictName, useNA = "ifany")

#In both datasets, let's remove Murambi sector soince we are no longer operating fomr there

#Let's remove Murambi sector and where credit is 0
sc <- subset(sc, SectorName != "Murambi" & TotalCredit > 0) #Here, need also to remove cash only clients
vrl <- subset(vrl, Sector != "Murambi")
cb <- subset(cb, SectorName != "Murambi")

#to check if it works
table(sc$SectorName, useNA = "ifany")
summary(sc$TotalCredit)
table(vrl$Sector, useNA = "ifany")
table(cb$SectorName)
dim(sc)
dim(cb)
dim(vrl)
View(pdb)
View(pds)

#################################################################################
#Part 1; setting deadline by bundles
###############################################################################

#1 - Setting the credit type
#to do this, we will have to merge the client bundles dataframe with payment deadline by bunles
head(cb$BundleName)
head(pdb$BundleName)


#checking if bundles names matches
test <- merge(data.frame(id = unique((cb$BundleName)), x = 1),
              data.frame(id = unique(pdb$BundleName), y = 1), by = "id", all = T)

test$dif <- test$x + test$y

test2 <- test[which(is.na(test$dif)), ]
nrow(test2)
test2 #No NAs on the client bundles  datbase's side

#Let's go ahead a do merge
cb1 <- merge(cb, pdb, by = "BundleName", all.x = T)

#to check if it works
dim(cb1)
dim(cb)
dim(pdb)
names(cb1)
table(cb1$Credit.deadline, useNA = "ifany") 

#Then, let's create a column showing original credit type
cb1$cr.typ <- ifelse(cb1$BundleCredit == 0, 0, cb1$Credit.deadline)

#To check if it works
table(cb1$cr.typ, useNA = "ifany")
length(cb1$BundleName[cb1$BundleCredit == 0])

#Dealing with dates
table(cb1$BundleSignUpDate)
cb1$BundleSignUpDate <- substr(cb1$BundleSignUpDate, 1, 10)

#to check if it works
head(cb1$BundleSignUpDate)

#Then, let's change to date
cb1$BundleSignUpDate <- as.Date(cb1$BundleSignUpDate, "%m/%d/%Y")

#Now, let's create a column showing the credit deadline
cb1$crt.mapping <- ifelse(cb1$BundleSignUpDate <= as.Date("12/31/2017", "%m/%d/%Y") , as.Date("03/31/2018", "%m/%d/%Y"), 0)
cb1$crt.mapping <- ifelse(cb1$BundleSignUpDate > as.Date("12/31/2017", "%m/%d/%Y") & cb1$BundleSignUpDate <= as.Date("06/30/2018", "%m/%d/%Y")
                          , as.Date("09/30/2018", "%m/%d/%Y"), cb1$crt.mapping)

cb1$crt.mapping <- ifelse(cb1$BundleSignUpDate > as.Date("06/30/2018", "%m/%d/%Y") & cb1$BundleSignUpDate <= as.Date("12/31/2018", "%m/%d/%Y")
                          , as.Date("03/31/2019", "%m/%d/%Y"), cb1$crt.mapping)

cb1$crt.mapping <- ifelse(cb1$BundleSignUpDate > as.Date("12/31/2018", "%m/%d/%Y") & cb1$BundleSignUpDate <= as.Date("06/30/2019", "%m/%d/%Y")
                          , as.Date("07/30/2019", "%m/%d/%Y"), cb1$crt.mapping)
cb1$crt.mapping <- ifelse(cb1$BundleSignUpDate > as.Date("06/30/2019", "%m/%d/%Y") & cb1$BundleSignUpDate <= as.Date("11/30/2019", "%m/%d/%Y")
                          , as.Date("03/01/2020", "%m/%d/%Y"), cb1$crt.mapping)

cb1$crt.mapping <- ifelse(cb1$BundleSignUpDate > as.Date("11/30/2019", "%m/%d/%Y") & cb1$BundleSignUpDate <= as.Date("06/30/2020", "%m/%d/%Y")
                          , as.Date("07/30/2020", "%m/%d/%Y"), cb1$crt.mapping)

cb1$crt.mapping <- ifelse(cb1$BundleSignUpDate > as.Date("06/30/2020", "%m/%d/%Y") & cb1$BundleSignUpDate <= as.Date("11/30/2020", "%m/%d/%Y")
                          , as.Date("03/30/2021", "%m/%d/%Y"), cb1$crt.mapping)

cb1$crt.mapping <- ifelse(cb1$BundleSignUpDate > as.Date("11/30/2020", "%m/%d/%Y") & cb1$BundleSignUpDate <= as.Date("06/30/2021", "%m/%d/%Y")
                          , as.Date("07/30/2021", "%m/%d/%Y"), cb1$crt.mapping)


#To check if it works
table(cb1$crt.mapping, useNA = "ifany")

#Let's first turn these into dates
cb1$crt.mapping <- as.Date(cb1$crt.mapping)

#to check if it works
table(cb1$crt.mapping, useNA = "ifany")
length(cb1$crt.mapping[cb1$BundleSignUpDate > as.Date("06/30/2019", "%m/%d/%Y") & 
                         cb1$BundleSignUpDate <= as.Date("11/30/2019", "%m/%d/%Y")])
#Now, I see that things are good







