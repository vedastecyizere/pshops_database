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
#Part 1; setting deadline by bundles - Credit bundles
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

View(cb1)
#Now, I see that things are good

#Now, let's add some conditions here
table(cb1$cr.typ)

cb1$crt.mapping <- ifelse(cb1$cr.typ == 0, as.Date(0), cb1$crt.mapping)

#Let's check what we got
table(cb1$crt.mapping, useNA = "ifany")
View(cb1)

#Let's turn these into dates again
cb1$crt.mapping <- as.Date(cb1$crt.mapping)

#Let's check if it worked
table(cb1$crt.mapping, useNA = "ifany")


#Creating a variable with numeric values in the credit type column
cb1$cr.typ1 <- as.numeric(cb1$cr.typ)

#Checks
table(cb1$cr.typ)
table(cb1$cr.typ1, useNA = "ifany")

#Let's replace NAs with 0
cb1$cr.typ1 <- ifelse(is.na(cb1$cr.typ1), 0, cb1$cr.typ1)

#to check if it works
table(cb1$cr.typ1, useNA = "ifany")
View(cb1)

cb1$crt.mapping1 <- ifelse(cb1$cr.typ1 == 6, cb1$BundleSignUpDate + months(6), cb1$crt.mapping)
cb1$crt.mapping1 <- ifelse(cb1$cr.typ1 == 24, cb1$BundleSignUpDate + months(24), cb1$crt.mapping1)

#Turn them into dates
cb1$crt.mapping1 <- as.Date(cb1$crt.mapping1)


#Change this back to date
cb1$crt.mapping <- as.Date(cb1$crt.mapping)

View(cb1)
table(cb1$crt.mapping1, useNA = "ifany") #1NA presented
table(cb1$BundleSignUpDate, useNA = "ifany")
table(cb1$crt.mapping, useNA = "ifany")


cb1$crt.dln <- paste(substr(cb1$crt.mapping, 1, 8), substr(cb1$BundleSignUpDate, 9, 10), sep = "")

#To check if it works
View(cb1)
#change it to date
cb1$crt.dln <- as.Date(cb1$crt.dln)
class(cb1$crt.dln)

########################################################################################
#Part 2: Playing around with DSC and clients bundles 
########################################################################################

#Part 2.1: Current credit details 
###########################################################################################

#Adding the current deadline to the season client
#...........................................................................

#Since clients bundles are on bundle names level, let's put them on clients level
#by determining the max credit deadline

#First, let's create SHS POS date
table(cb1$BundleName, useNA = "ifany")
cb1$shs <- ifelse(grepl("SHS", cb1$BundleName), 1, 0)

#To check if it works
table(cb1$shs, useNA = "ifany")


cbsm <- cb1 %>%
        group_by(OAFID) %>%
        summarise(crnt.dln = max(crt.dln),
                  shs.po.date = max(BundleSignUpDate[grepl("SHS", BundleName) & BundleCredit > 0]),
                  skp.po.date = max(BundleSignUpDate[grepl("SKP", BundleName) & BundleCredit > 0]),
                  skm.po.date = max(BundleSignUpDate[grepl("SKM", BundleName) & BundleCredit > 0]))

#to check if it works
head(cbsm)
length(cb1$BundleName[cb1$OAFID == 3876])
max(cb1$crt.dln[cb1$OAFID == 3876])
cb1$crt.dln[cb1$OAFID == 3876]
cb1$BundleName[cb1$OAFID == 3876]
View(cbsm)
cb1$BundleName[cb1$OAFID == 4862]
cb1$BundleCredit[cb1$OAFID == 4862]
length(unique(cbsm$OAFID[is.na(cbsm$crnt.dln)]))
length(unique(cbsm$OAFID[is.na(cbsm$skp.po.date)])) #No NAs

#Now, I'm confident my numberrs are alright. 

#Now, let's merge this list with our season client
length(unique(sc$OAFID))

#Let's first check if we do have any client in the SC who is not in the client bundles
test <- merge(data.frame(id = unique((sc$OAFID)), x = 1),
              data.frame(id = unique(cbsm$OAFID), y = 1), by = "id", all = T)

test$dif <- test$x + test$y

test2 <- test[which(is.na(test$dif)), ]
nrow(test2)
test2 

# #Let's merge them
# sc1 <- merge(sc, cbsm, by = "OAFID", all.x = T)
# 
# #To check if it works
# dim(sc1)
# dim(sc)
# dim(cbsm)
# class(sc1$crnt.dln)
# head(sc1$crnt.dln)

#....................................................................
#Generating the last POS date and amount paid by the current deadline using the repayment light

head(vrl)
length(unique(vrl$OAFID))
length(vrl$OAFID)


#On the vertical repayment, let's add the current deadline variable
vrl$crt.dln <- cbsm$crnt.dln[match(vrl$OAFID, cbsm$OAFID)]


#To check if it works
head(vrl$crt.dln)
head(vrl)
vrl$crt.dln[vrl$OAFID == "9632"]
cbsm$crnt.dln[cbsm$OAFID == "9632"]
#Now, I see that things are alright

#Now, let's calculate the amount paid per client as of the current deadline
#First, let's deal with dates within the repaymet light
head(vrl$RepaymentDate)
class(vrl$RepaymentDate)

vrl$date <- substr(vrl$RepaymentDate, 1, 10)
vrl$date <- gsub(" ", "", vrl$date)
vrl$date <- gsub("-", "/", vrl$date)
vrl$date <- as.Date(vrl$date)

#to check if it works
head(vrl$date)
head(vrl$RepaymentDate)
class(vrl$date)

#Let's use this code to avoid warnings
RobustMax <- function(x) {if (length(x)>0) max(x) else -Inf}
RobustMin <- function(x) {if (length(x)>0) min(x) else -Inf}

vrl1 <- vrl %>%
        group_by(OAFID) %>%
        summarise(amt.pd.by.crt.dln = sum(Amount[date <= crt.dln]),
                  lst.po.dt = RobustMax(date[Type == "Receipt"]),
                  #adding the prepayment amount
                  prep.amount = sum(Amount[date == lst.po.dt]),
                  #adding number of credit taken - Type = Receipt
                  n.credit = length(Type[Type == "Receipt"]),
                  fst.crt.ev.tkn = RobustMin(date[Type == "Receipt"]),
                  #Amount paid by last PO date 
                  amt.pd.by.lst.pos.date = sum(Amount[date < lst.po.dt]),
                  past.pos.date.1 = RobustMax(date[Type == "Receipt" & date < lst.po.dt]))
                                           
#to check if it works
head(vrl1)
tail(vrl1)
max(vrl$date[vrl$Type == "Receipt" & vrl$OAFID == 3880])
sum(vrl$Amount[vrl$OAFID == "12824" & vrl$date == max(vrl$date[vrl$OAFID == "12824" & vrl$Type == "Receipt"])])
View(vrl1)
min(vrl$date[vrl$Type == "Receipt" & vrl$OAFID == 3881])
length(vrl$Type[vrl$Type == "Receipt" & vrl$OAFID == 3881])
#Things are great

#...........................................................................................
#Calculating the past deadline 1 on the client bundles
#'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
#First, we need to add the past POS date 1 on the client bundles
cb1$past.pos.date.1 <- vrl1$past.pos.date.1[match(cb1$OAFID, vrl1$OAFID)]

head(cb1$OAFI, n = 20)
head(cb1$past.pos.date.1, n = 20)
head(cb1$crt.dln)


#Now, let's re-run the cbsm summary table
cbsm <- cb1 %>%
        group_by(OAFID) %>%
        summarise(crnt.dln = RobustMax(crt.dln),
                  shs.po.date = RobustMax(BundleSignUpDate[grepl("SHS", BundleName) & BundleCredit > 0]),
                  skp.po.date = RobustMax(BundleSignUpDate[grepl("SKP", BundleName) & BundleCredit > 0]),
                  skm.po.date = RobustMax(BundleSignUpDate[grepl("SKM", BundleName) & BundleCredit > 0]),
                  pst.dln.1 = RobustMax(crt.dln[BundleSignUpDate <= past.pos.date.1]))
View(cbsm)
#turn back solar deadline into date format
cbsm$shs.po.date <- as.Date(cbsm$shs.po.date)
cbsm$skp.po.date <- as.Date(cbsm$skp.po.date)
cbsm$skm.po.date <- as.Date(cbsm$skm.po.date)

#to check if it works
View(cbsm) #Fixed

#Now, let's transfer the past deadline 1 to the vertical repayment
vrl$pst.dln.1 <- cbsm$pst.dln.1[match(vrl$OAFID, cbsm$OAFID)]

#......................................................


#let's now re-run the vertical repayment summary so we can add amount paid by dealdine 1
vrl1 <- vrl %>%
        group_by(OAFID) %>%
        summarise(amt.pd.by.crt.dln = sum(Amount[date <= crt.dln]),
                  lst.pos.dt = RobustMax(date[Type == "Receipt"]),
                  #adding the prepayment amount
                  prep.amount = sum(Amount[Type == "Receipt" & date == lst.pos.dt]),
                  #adding number of credit taken - Type = Receipt
                  n.credit = length(Type[Type == "Receipt"]),
                  fst.crt.ev.tkn = RobustMin(date[Type == "Receipt"]),
                  #Amount paid by last PO date 
                  amt.pd.by.lst.pos.date = sum(Amount[date < lst.pos.dt]),
                  past.pos.date.1 = RobustMax(date[Type == "Receipt" & date < lst.pos.dt]),
                  amt.pd.by.pst.dln.1 = sum(Amount[date <= pst.dln.1]),
                  past.prep.amount.1 = sum(Amount[Type == "Receipt" & date == past.pos.date.1]))

#To check if it works
View(vrl1)





#Now, let's add these to the SC
sc2 <- merge(sc1, vrl1, by = "OAFID", all.x = T)

#To check if it works
head(sc2)
dim(sc2)
dim(sc1)
dim(vrl1)
length(unique(sc2$GlobalClientID[is.na(sc2$lst.po.dt)]))
length(unique(sc2$GlobalClientID[is.na(sc2$amt.pd.by.crt.dln)])) #No payment made so far
length(unique(sc2$GlobalClientID[is.na(sc2$crnt.dln)]))

#These NAs are due to clients that are not on both datasets

#Let's turn all NAs into Zero
sc2$crnt.dln <- ifelse(is.na(sc2$crnt.dln), 0, sc2$crnt.dln)
sc2$lst.po.dt <- ifelse(is.na(sc2$lst.po.dt), 0, sc2$lst.po.dt)
sc2$amt.pd.by.crt.dln <- ifelse(is.na(sc2$amt.pd.by.crt.dln), 0, sc2$amt.pd.by.crt.dln)

#To check if it works
head(sc2$crnt.dln)

#let's turn our dates columns to 
sc2$crnt.dln <- as.Date(sc2$crnt.dln)
sc2$lst.po.dt <- as.Date(sc2$lst.po.dt)

#to check if it works
head(sc2$crnt.dln)
head(sc2$lst.po.dt)
#-----------------------------------------------------------------------------


