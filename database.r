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

#....................................................................
#Part 2.2 Generating the last POS date and amount paid by the current deadline using the repayment light
#------------------------------------------------------------------

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
                  lst.pos.dt = RobustMax(date[Type == "Receipt"]),
                  #adding the prepayment amount
                  prep.amount = sum(Amount[Type == "Receipt" & date == lst.pos.dt]),
                  #adding number of credit taken - Type = Receipt
                  n.credit = length(Type[Type == "Receipt"]),
                  fst.crt.ev.tkn = RobustMin(date[Type == "Receipt"]),
                  #Amount paid by last PO date 
                  amt.pd.by.lst.pos.date = sum(Amount[date < lst.pos.dt]),
                  past.pos.date.1 = RobustMax(date[Type == "Receipt" & date < lst.pos.dt]))
                                           
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
#Part 2.3: Calculating the past deadline 1 & past credit 1 on the client bundles
#'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
#First, we need to add the last POS date and  past POS date 1 on the client bundles
cb1$lst.pos.dt <- vrl1$lst.pos.dt[match(cb1$OAFID, vrl1$OAFID)]
cb1$past.pos.date.1 <- vrl1$past.pos.date.1[match(cb1$OAFID, vrl1$OAFID)]

head(cb1$OAFI, n = 20)
head(cb1$lst.pos.dt, n = 20)
head(cb1$past.pos.date.1, n = 20)
head(cb1$crt.dln)


#Now, let's re-run the cbsm summary table
cbsm <- cb1 %>%
        group_by(OAFID) %>%
        summarise(crnt.dln = RobustMax(crt.dln),
                  shs.po.date = RobustMax(BundleSignUpDate[grepl("SHS", BundleName) & BundleCredit > 0]),
                  skp.po.date = RobustMax(BundleSignUpDate[grepl("SKP", BundleName) & BundleCredit > 0]),
                  skm.po.date = RobustMax(BundleSignUpDate[grepl("SKM", BundleName) & BundleCredit > 0]),
                  pst.dln.1 = RobustMax(crt.dln[BundleSignUpDate <= past.pos.date.1]),
                  lst.credit = sum(BundleCredit[BundleSignUpDate <= lst.pos.dt]),
                  pst.crdt.1 = sum(BundleCredit[BundleSignUpDate <= past.pos.date.1]))
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
                  past.prep.amount.1 = sum(Amount[Type == "Receipt" & date == past.pos.date.1]),
                  past.pos.date.2 = RobustMax(date[Type == "Receipt" & date < past.pos.date.1]),
                  amt.pd.by.past.pos.date.1 = sum(Amount[date < past.pos.date.1]))

#To check if it works
View(vrl1)

###......................................................................
#Part 2.4: Transferring the past POS date 2 to the client bundles
#------------------------------------------------------------------------
cb1$past.pos.date.2 <- vrl1$past.pos.date.2[match(cb1$OAFID, vrl1$OAFID)]

head(cb1$OAFID, n = 20)
head(cb1$crt.dln)
head(cb1$past.pos.date.2, n = 20)
View(cb1)

#Then, to calculate the client deadline 2, we need to re-run again the client bundles summary
cbsm <- cb1 %>%
        group_by(OAFID) %>%
        summarise(crnt.dln = RobustMax(crt.dln),
                  shs.po.date = RobustMax(BundleSignUpDate[grepl("SHS", BundleName) & BundleCredit > 0]),
                  skp.po.date = RobustMax(BundleSignUpDate[grepl("SKP", BundleName) & BundleCredit > 0]),
                  skm.po.date = RobustMax(BundleSignUpDate[grepl("SKM", BundleName) & BundleCredit > 0]),
                  pst.dln.1 = RobustMax(crt.dln[BundleSignUpDate <= past.pos.date.1]),
                  pst.dln.2 = RobustMax(crt.dln[BundleSignUpDate <= past.pos.date.2]),
                  lst.credit = sum(BundleCredit[BundleSignUpDate <= lst.pos.dt]),
                  pst.crdt.1 = sum(BundleCredit[BundleSignUpDate <= past.pos.date.1]),
                  pst.crdt.2 = sum(BundleCredit[BundleSignUpDate <= past.pos.date.2]))
View(cbsm)

#------------------------------------------------------------
#Part 2.5: Generating the total amount paid by the past deadline 2
#..............................................................

#To do this, we need to transfer the pst deadline 2 variable to the vertical repayment
vrl$pst.dln.2 <- cbsm$pst.dln.2[match(vrl$OAFID, cbsm$OAFID)]

#........................................................................
#Now, let's re-run the vertical repayment summary so we can add amount paid by dealdine 2

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
                  past.prep.amount.1 = sum(Amount[Type == "Receipt" & date == past.pos.date.1]),
                  past.pos.date.2 = RobustMax(date[Type == "Receipt" & date < past.pos.date.1]),
                  amt.pd.by.past.pos.date.1 = sum(Amount[date < past.pos.date.1]),
                  amt.pd.by.pst.dln.2 = sum(Amount[date <= pst.dln.2]),
                  past.prep.amount.2 = sum(Amount[Type == "Receipt" & date == past.pos.date.2]))

#To check if it works
View(vrl1)
vrl1$past.pos.date.2[vrl1$OAFID == 3882]
vrl$pst.dln.2[vrl$OAFID == 3882]


#######################################################################################
#Part 3: Merging Vertical repayment and client bundles to the season clients
####################################################################################

sc1 <- merge(sc, cbsm, by = "OAFID", all.x = T) %>%
        merge(vrl1, by = "OAFID", all.x = T)

#To check if it works
dim(sc1)
dim(sc)
dim(cbsm)
dim(vrl1)
class(sc1$crnt.dln)
head(sc1$crnt.dln)
View(sc1)

#Now, we have all summary in one place. We only remain with creating additional columns but in
#one document

#Let's save this object
save(sc1, file = paste(wd,"sc_with_vrl_and_cbs.Rdata", sep ="/"))

load(file = paste(wd,"sc_with_vrl_and_cbs.Rdata", sep ="/"))

###################################################################################

#Part 4: Generating additional metrics
#########################################################################
View(sc1)
names(sc1)

#Part 4.1 Current details' metrics
#------------------------------------------------------------------------
#calculating the crdit type first

#Let's first generate totals by type
sc1$tot.ag <- sc1$X20_DAP.credit.kg +                   
              sc1$X20A_H.1520.Cash.kg +                 
              sc1$X20A_H.1520.credit.kg +               
              sc1$X20A_H.1602.Cash.kg +                 
              sc1$X20A_H.1602.credit.kg +               
              sc1$X20A_H.513.Cash.kg +                  
              sc1$X20A_H.513.Credit.kg +                
              sc1$X20A_H.628.Credit.kg +                
              sc1$X20A_H.629.Cash.kg +                  
              sc1$X20A_H.629.Credit.kg +                
              sc1$X20A_KS.Chozi.Cash.kg +               
              sc1$X20A_KS.Chozi.credit.kg +             
              sc1$X20A_KS.Njoro.II.Cash.kg +            
              sc1$X20A_KS.Njoro.II.credit.kg +          
              sc1$X20A_M101.Cash.kg +                   
              sc1$X20A_M101.credit.kg +                 
              sc1$X20A_NPK.credit.kg +                  
              sc1$X20A_PAN.53.Cash.kg +                 
              sc1$X20A_PAN.53.credit.kg +               
              sc1$X20A_PAN.691.credit.kg +              
              sc1$X20A_PICS.cash.qty +                  
              sc1$X20A_PICS.credit.qty +                
              sc1$X20A_Pool.9A.credit.kg +              
              sc1$X20A_RHM.104.Cash.kg +                
              sc1$X20A_RHM.104.credit.kg +              
              sc1$X20A_RHM.1402.Cash.kg +               
              sc1$X20A_RHM.1402.credit.kg +             
              sc1$X20A_RHM.1407.Cash.kg +               
              sc1$X20A_RHM.1407.credit.kg +             
              sc1$X20A_SC.403.Cash.kg +                 
              sc1$X20A_SC.403.Credit.kg +               
              sc1$X20A_SC.637.Cash.kg +                 
              sc1$X20A_SC.637.Credit.kg +                
              sc1$X20A_Travertine.cash.kg +             
              sc1$X20A_Travertine.credit.kg +           
              sc1$X20A_Urea.credit.kg +                 
              sc1$X20A_WH.101.Credit.kg +               
              sc1$X20A_WH.403.Credit.kg +               
              sc1$X20A_WH.505.Credit.kg +               
              sc1$X20A_WH.507.Cash.kg +                 
              sc1$X20A_WH.507.Credit.kg +               
              sc1$X20A_WH.509.Cash.kg +                 
              sc1$X20A_WH.509.Credit.kg +               
              sc1$X20A_WH.605.credit.kg +               
              sc1$X20A_Wheat.Local.Varieties.credit.kg +
              sc1$X20A_ZM.607.credit.kg +     
              sc1$Cypermethrin100ml_cash.kg +           
              sc1$Cypermethrin100ml_credit.kg +         
              sc1$DAP.Cash.kg +                         
              sc1$DAP.Credit.kg +                       
              sc1$Dithane_cash.kg +                     
              sc1$Dithane_credit.kg +                   
              sc1$H628.Cash.kg +                        
              sc1$H628.Credit.kg +                      
              sc1$KS.Njoro.I.Cash.kg +                  
              sc1$KS.Njoro.I.Credit.kg +                
              sc1$NPK17.Cash.kg +                       
              sc1$NPK17.Credit.kg +                     
              sc1$PAN.691.Cash.kg +                     
              sc1$PAN.691.Credit.kg +                   
              sc1$PICS.Cash.qty +                       
              sc1$PICS.credit.qty +                     
              sc1$Pool.9A.Cash.kg +                     
              sc1$Pool.9A.Credit.kg +                   
              sc1$Pumps10_cash.qty +                    
              sc1$Pumps10_credit.qty +                  
              sc1$Pumps20_credit.qty +                  
              sc1$Pumps20Lcash.qty +                    
              sc1$Roket100ml_cash.kg +                  
              sc1$Roket100ml_credit.kg +                
              sc1$UREA.Cash.kg +                        
              sc1$UREA.Credit.kg +                      
              sc1$WH.101.Cash.kg +                      
              sc1$WH.101.Credit.kg +                    
              sc1$WH.403.cash.kg +                      
              sc1$WH.403.credit.kg +                    
              sc1$WH.505.cash.kg +                      
              sc1$WH.505.Credit.kg +                    
              sc1$WH.605.cash.kg +                      
              sc1$WH.605.Credit.kg

#Generating total solar
sc1$tot.solar <- sc1$X20A_SHS.cash.kg +    +                  
                 sc1$X20A_SHS.credit.qty +                 
                 sc1$X20A_SKP.credit.qty +               
                 sc1$X20B_SKP2.credit.qty +                 
                 sc1$SHS.Cash.qty +                         
                 sc1$SHS.Credit.qty +                       
                 sc1$SKM.Cash.qty +                         
                 sc1$SKM.credit.qty +                       
                 sc1$SKP.cash.kg +                          
                 sc1$SKP.Cash.qty +                         
                 sc1$SKP.credit.LP.qty +                    
                 sc1$SKP1_cash.qty +                        
                 sc1$SKP1_credit.qty +                      
                 sc1$SKP200_cash.qty +                      
                 sc1$SKP200_Credit.qty 

sc1$tot.sml.solar <- sc1$X20A_SKP.credit.qty +               
                 sc1$X20B_SKP2.credit.qty +                        
                 sc1$SKM.Cash.qty +                         
                 sc1$SKM.credit.qty +                       
                 sc1$SKP.cash.kg +                          
                 sc1$SKP.Cash.qty +                         
                 sc1$SKP.credit.LP.qty +                    
                 sc1$SKP1_cash.qty +                        
                 sc1$SKP1_credit.qty +                      
                 sc1$SKP200_cash.qty +                      
                 sc1$SKP200_Credit.qty

sc1$tot.shs <- sc1$X20A_SHS.cash.kg +    +                  
               sc1$X20A_SHS.credit.qty +                 
               sc1$SHS.Cash.qty +                         
               sc1$SHS.Credit.qty

sc1$tot.chicken <- sc1$ChickenFeedsGrower_cash.qty +         
                   sc1$ChickenFeedsGrower_credit.qty +       
                   sc1$ChickenFeedsLayer_cash.qty +          
                   sc1$ChickenFeedsLayer_credit.qty +        
                   sc1$ChickenFeedsPrelayer_cash.qty +       
                   sc1$ChickenFeedsPrelayer_credit.qty +     
                   sc1$ChickenFeedsStarter_cash.qty +        
                   sc1$ChickenFeedsStarter_credit.qty
        
        

sc1$credit.type <- ifelse(sc1$tot.ag > 0, "Agriculture Credit", 0)
sc1$credit.type <- ifelse(sc1$tot.shs > 0, "SHS Credit", sc1$credit.type)
sc1$credit.type <- ifelse(sc1$tot.sml.solar > 0, "Small Solar Credit", sc1$credit.type)
sc1$credit.type <- ifelse(sc1$tot.chicken > 0, "Chicken Credit", sc1$credit.type)
sc1$credit.type <- ifelse(sc1$tot.ag> 0 & (sc1$tot.shs > 0 | sc1$tot.sml.solar > 0 |
                                                   sc1$tot.chicken > 0), "Combo Credit", sc1$credit.type)
#to check if it's correct
table(sc1$credit.type, useNA = "ifany")
length(unique(sc1$GlobalClientID[sc1$tot.ag > 0 & sc1$tot.shs > 0]))
length(unique(sc1$GlobalClientID[sc1$tot.ag > 0 & sc1$tot.sml.solar > 0]))
length(unique(sc1$GlobalClientID[sc1$tot.ag > 0 & sc1$tot.chicken > 0]))

#Now, I can see that things are great

#4.2 Calculating the adjusted POS date
#----------------------------------------------------------------------
sc1$adjstd.lst.pos.dt <- ifelse(day(sc1$lst.pos.dt) > 29, as.Date(paste(substr(sc1$lst.pos.dt, 1, 8),25, sep = "")), 
                                sc1$lst.pos.dt)

#To check if it works
head(sc1$adjstd.lst.pos.dt, n = 20)

#Let's first change this to date format
table(sc1$adjstd.lst.pos.dt, useNA = "ifany")
class(sc1$adjstd.lst.pos.dt)
sc1$adjstd.lst.pos.dt <- as.Date(sc1$adjstd.lst.pos.dt)

#Then, let's check
head(sc1$adjstd.lst.pos.dt, n = 100)
head(sc1$lst.pos.dt, n = 100)

#-----------------------------------------------------------------------------
#4.3 Creating a column showing the current credit length in months

##############################################################################
sc1$credit.length <- (as.yearmon(sc1$crnt.dln) - as.yearmon(sc1$adjstd.lst.pos.dt))*12

#to check if it works
head(sc1$crnt.dln)
head(sc1$adjstd.lst.pos.dt)
head(sc1$credit.length)

#Calculating the active credit
#If Totalcredit = total repaid, no active credit. 
#If the above is not the case, take total credit - total repaid by the last POS dat
sc1$actv.tot.credit <- ifelse(sc1$TotalCredit == sc1$TotalRepaid,0, 
                              sc1$TotalCredit - sc1$amt.pd.by.lst.pos.date)
#To check if it works
head(sc1$TotalCredit, n = 10)
head(sc1$TotalRepaid, n = 10)
head(sc1$amt.pd.by.lst.pos.date, n = 10)
head(sc1$actv.tot.credit, n = 10)
length(unique(sc1$OAFID[sc1$n.credit == 1 & sc1$TotalCredit > sc1$actv.tot.credit])) #1,335
length(unique(sc1$OAFID[sc1$n.credit == 1 & sc1$TotalCredit == sc1$TotalRepaid])) #1,353

#then, let's calculate the old credit
sc1$old.credit <- sc1$TotalCredit - sc1$actv.tot.credit

#to check if it works
head(sc1$TotalCredit)
head(sc1$actv.tot.credit)
head(sc1$old.credit)

#Finally, we can also add on the active total repaid
length(unique(sc1$GlobalClientID[is.na(sc1$TotalRepaid)]))
length(unique(sc1$GlobalClientID[is.na(sc1$old.credit)]))
table(sc1$old.credit, useNA = "ifany")

#Let's first turn NAs into zero
sc1$old.credit <- ifelse(is.na(sc1$old.credit), 0, sc1$old.credit)

#Then, 
sc1$actv.tot.repaid <- sc1$TotalRepaid - sc1$old.credit

#Now, let's create the updated POS date to use when creating the prepayment
length(unique(sc1$OAFID[sc1$pst.crdt.1 > sc1$lst.credit]))
length(unique(sc1$OAFID[sc1$pst.crdt.2 > sc1$pst.crdt.1]))
length(unique(sc1$OAFID[sc1$actv.tot.credit > sc1$lst.credit])) #52

#Before doing this, let's change all NAs into zero
length(unique(sc1$OAFID[is.na(sc1$pst.crdt.2)])) #106
length(unique(sc1$OAFID[is.na(sc1$pst.crdt.1)])) #106
length(unique(sc1$OAFID[is.na(sc1$lst.credit)])) #106
length(unique(sc1$OAFID[is.na(sc1$actv.tot.credit)])) #5
###############################################################

#Then, let's do
sc1$actv.tot.credit <- ifelse(is.na(sc1$actv.tot.credit), 0, sc1$actv.tot.credit)
sc1$lst.credit <- ifelse(is.na(sc1$lst.credit), 0, sc1$lst.credit)
sc1$pst.crdt.1 <- ifelse(is.na(sc1$pst.crdt.1), 0, sc1$pst.crdt.1)
sc1$pst.crdt.2 <- ifelse(is.na(sc1$pst.crdt.2), 0, sc1$pst.crdt.2)

#Let's me check if it works
length(unique(sc1$OAFID[is.na(sc1$pst.crdt.2)])) #0
length(unique(sc1$OAFID[is.na(sc1$pst.crdt.1)])) #0
length(unique(sc1$OAFID[is.na(sc1$lst.credit)])) #0
length(unique(sc1$OAFID[is.na(sc1$actv.tot.credit)])) #0

#it works


#For dates
length(unique(sc1$OAFID[is.na(sc1$lst.pos.dt)])) #5
length(unique(sc1$OAFID[is.infinite(sc1$lst.pos.dt)])) #1
length(unique(sc1$OAFID[is.na(sc1$past.pos.date.1)])) #5
length(unique(sc1$OAFID[is.infinite(sc1$past.pos.date.1)])) #3,668
length(unique(sc1$OAFID[is.na(sc1$past.pos.date.2)])) #5
length(unique(sc1$OAFID[is.infinite(sc1$past.pos.date.2)])) #4,158

table(sc1$lst.pos.dt, useNA = "ifany")
table(sc1$past.pos.date.1, useNA = "ifany")
table(sc1$past.pos.date.2, useNA = "ifany")

#Turning these into zero
sc1$lst.pos.dt <- ifelse(is.na(sc1$lst.pos.dt), 0, sc1$lst.pos.dt)
sc1$lst.pos.dt <- ifelse(is.infinite(sc1$lst.pos.dt), 0, sc1$lst.pos.dt)

sc1$past.pos.date.1 <- ifelse(is.na(sc1$past.pos.date.1), 0, sc1$past.pos.date.1)
sc1$past.pos.date.1 <- ifelse(is.infinite(sc1$past.pos.date.1), 0, sc1$past.pos.date.1)

sc1$past.pos.date.2 <- ifelse(is.na(sc1$past.pos.date.2), 0, sc1$past.pos.date.2)
sc1$past.pos.date.2 <- ifelse(is.infinite(sc1$past.pos.date.2), 0, sc1$past.pos.date.2)

#To check if it works
length(unique(sc1$OAFID[is.na(sc1$lst.pos.dt)])) #0
length(unique(sc1$OAFID[is.infinite(sc1$lst.pos.dt)])) #0
length(unique(sc1$OAFID[is.na(sc1$past.pos.date.1)])) #5
length(unique(sc1$OAFID[is.infinite(sc1$past.pos.date.1)])) #0
length(unique(sc1$OAFID[is.na(sc1$past.pos.date.2)])) #0
length(unique(sc1$OAFID[is.infinite(sc1$past.pos.date.2)])) #0

table(sc1$past.pos.date.2, useNA = "ifany")
table(sc1$past.pos.date.1, useNA = "ifany")
table(sc1$lst.pos.dt, useNA = "ifany")



#Then, now we can create the updated POS date
sc1$updtd.pos.date <- ifelse(sc1$actv.tot.credit > 0 & 
                               sc1$actv.tot.credit <= sc1$pst.crdt.2, sc1$past.pos.date.2, 0)

#to check if it works
table(sc1$updtd.pos.date, useNA = "ifany")
length(unique(sc1$OAFID[sc1$actv.tot.credit == 0 | sc1$actv.tot.credit > sc1$pst.crdt.2])) #Nice

#Then, 
sc1$updtd.pos.date <- ifelse(sc1$actv.tot.credit > 0 & sc1$actv.tot.credit > sc1$pst.crdt.2 &
                               sc1$actv.tot.credit <= sc1$pst.crdt.1, sc1$past.pos.date.1, sc1$updtd.pos.date)

#check
table(sc1$updtd.pos.date, useNA = "ifany")
length(unique(sc1$OAFID[sc1$updtd.pos.date > 0]))
length(unique(sc1$OAFID[sc1$actv.tot.credit > 0 & sc1$actv.tot.credit <= sc1$pst.crdt.1])) #Nice

#Lastly,
sc1$updtd.pos.date <- ifelse(sc1$actv.tot.credit > 0 & sc1$actv.tot.credit > sc1$pst.crdt.1 &
                               sc1$actv.tot.credit <= sc1$lst.credit, sc1$lst.pos.dt, sc1$updtd.pos.date)
#check
table(sc1$updtd.pos.date, useNA = "ifany")
length(unique(sc1$OAFID[sc1$updtd.pos.date > 0]))
length(unique(sc1$OAFID[sc1$actv.tot.credit > 0 & sc1$actv.tot.credit <= sc1$lst.credit]))



length(unique(sc1$OAFID[sc1$actv.tot.credit == 0 | sc1$actv.tot.credit <= sc1$pst.crdt.1 |
                                sc1$actv.tot.credit > sc1$lst.credit]))
length(unique(sc1$OAFID[sc1$updtd.pos.date != 0]))
length(unique(sc1$OAFID[sc1$lst.pos.dt != 0 & sc1$actv.tot.credit > sc1$pst.crdt.1 &
                                sc1$actv.tot.credit <= sc1$lst.credit]))

table(sc1$updtd.pos.date, useNA = "ifany")
table(sc1$actv.tot.credit, useNA = "ifany")
table(sc1$lst.credit, useNA = "ifany")
table(sc1$pst.crdt.1, useNA = "ifany")
table(sc1$pst.crdt.2, useNA = "ifany")

table(sc1$lst.pos.dt, useNA = "ifany")
table(sc1$past.pos.date.1, useNA = "ifany")
table(sc1$past.pos.date.2, useNA = "ifany")

#Let's change this into date format
sc1$updtd.pos.date <- as.Date(sc1$updtd.pos.date)

#to check if it works
head(sc1$updtd.pos.date)

#################################################################################
#4.4 calculating the prepaymment amount
#--------------------------------------------------------------------------------
#To be able to do this, we will need to add active credit & updated last POS from SC to vertical repayment
vrl$actv.tot.credit <- sc1$actv.tot.credit[match(vrl$OAFID, sc1$OAFID)]
vrl$updtd.pos.date <- sc1$updtd.pos.date[match(vrl$OAFID, sc1$OAFID)]

#To check if it works
head(vrl$OAFID)
head(vrl$updtd.pos.date)
head(vrl$actv.tot.credit)

#Now, let's re-run the code for summary vertical repayment
vrl2 <- vrl %>%
        group_by(OAFID) %>%
        summarise(amt.pd.by.crt.dln = sum(Amount[date <= crt.dln]),
                  lst.pos.dt = RobustMax(date[Type == "Receipt"]),
                  actv.tot.credit = max(actv.tot.credit),
                  #adding number of credit taken - Type = Receipt
                  n.credit = length(Type[Type == "Receipt"]),
                  fst.crt.ev.tkn = RobustMin(date[Type == "Receipt"]),
                  #Amount paid by last PO date 
                  amt.pd.by.lst.pos.date = sum(Amount[date < lst.pos.dt]),
                  past.pos.date.1 = RobustMax(date[Type == "Receipt" & date < lst.pos.dt]),
                  amt.pd.by.pst.dln.1 = sum(Amount[date <= pst.dln.1]),
                  past.prep.amount.1 = sum(Amount[Type == "Receipt" & date == past.pos.date.1]),
                  past.pos.date.2 = RobustMax(date[Type == "Receipt" & date < past.pos.date.1]),
                  amt.pd.by.past.pos.date.1 = sum(Amount[date < past.pos.date.1]),
                  amt.pd.by.pst.dln.2 = sum(Amount[date <= pst.dln.2]),
                  past.prep.amount.2 = sum(Amount[Type == "Receipt" & date == past.pos.date.2]),
                  #adding the prepayment amount
                  prep.amount = sum(Amount[Type == "Receipt" & date == updtd.pos.date]))


#to check if it works
View(vrl2)


#..................................................................................
#4.5 Calculating the monthly payment
##--------------------------------------------------------------------------------
head(sc1$actv.tot.repaid)
head(sc1$prep.amount)

#To generate we need to use the active total repaid, substract what the prepayment amount is, over the credit length(in months)
sc1$updtd.prep.amount <- vrl2$prep.amount[match(sc1$OAFID, vrl2$OAFID)]

#check
head(sc1$OAFID)
head(sc1$updtd.prep.amount)

#First, let me add the prepayment amount here, 
sc1$monthy.payment <- round((sc1$actv.tot.credit - sc1$updtd.prep.amount)/sc1$credit.length, 0)

#to check if it works
head(sc1$actv.tot.credit)
head(sc1$updtd.prep.amount)
head(sc1$monthy.payment)
head(sc1$TotalCredit)
head(sc1$TotalRepaid)
head(sc1$old.credit)
head(sc1$n.credit)
###############################################################################################
#4.6 Calculating the number of months that should have been paid
#-----------------------------------------------------
head(sc1$lst.pos.dt)

#Change this into dates first
sc1$lst.pos.dt <- as.Date(sc1$lst.pos.dt)

sc1$n.months.shld.hv.bn.pd <- ifelse(today()> sc1$crnt.dln, sc1$credit.length,
                                     (as.yearmon(today()) - as.yearmon(sc1$lst.pos.dt))*12)
#check
head(sc1$crnt.dln, n = 10)
head(sc1$lst.pos.dt, n = 10)
head(sc1$n.months.shld.hv.bn.pd, n = 10)


#Then, calculating the amount that should have been paid since last POS
sc1$amt.shld.hv.bn.pd.snc.lst.pos.b.elgible <- (sc1$monthy.payment*sc1$n.months.shld.hv.bn.pd) + 
                                                  sc1$updtd.prep.amount

#To check if it works
head(sc1$actv.tot.credit)
head(sc1$updtd.prep.amount)
head(sc1$prep.amount)
head(sc1$amt.shld.hv.bn.pd.snc.lst.pos.b.elgible)

#calculating the total credit that should have been paid to be eligible for taking another credit
sc1$credit.shld.hv.bn.pd.b.elgible <- ifelse(sc1$actv.tot.credit == 0, sc1$old.credit, 
                                             sc1$old.credit + sc1$amt.shld.hv.bn.pd.snc.lst.pos.b.elgible)

#to check if it works
head(sc1$credit.shld.hv.bn.pd.b.elgible)
head(sc1$actv.tot.credit)
head(sc1$old.credit)
head(sc1$amt.shld.hv.bn.pd.snc.lst.pos.b.elgible)

###########################################################################################
#4.7 Determining the client type - Different versions
##############################################################################
#4.7.1 Starting with Past Season 2
#-----------------------------------------------------------------------------

sc1$pst.clt.typ.2 <- ifelse(is.na(sc1$pst.dln.2) | is.infinite(sc1$pst.dln.2), "FALSE", 0)

sc1$pst.clt.typ.2 <- ifelse(!is.na(sc1$pst.dln.2) & sc1$pst.dln.2 > sc1$past.pos.date.1, "Top Up", sc1$pst.clt.typ.2)



sc1$pst.clt.typ.2 <- ifelse(!is.na(sc1$pst.dln.2) & sc1$amt.pd.by.pst.dln.2 >= sc1$pst.crdt.2 &
                                    sc1$pst.dln.2 > sc1$past.pos.date.1 &
                                    sc1$pst.crdt.2 > 0, "Returning", sc1$pst.clt.typ.2)

sc1$pst.clt.typ.2 <- ifelse(!is.na(sc1$pst.dln.2) & sc1$amt.pd.by.pst.dln.2 < sc1$pst.crdt.2 & 
                                    sc1$amt.pd.by.past.pos.date.1 < sc1$pst.crdt.2, "Blacklisted Defaulter",
                            sc1$pst.clt.typ.2)

sc1$pst.clt.typ.2 <- ifelse(!is.na(sc1$pst.dln.2) & sc1$amt.pd.by.pst.dln.2 < sc1$pst.crdt.2 & 
                                    sc1$amt.pd.by.past.pos.date.1 >= sc1$pst.crdt.2, "Forgiven Defaulter", 
                            sc1$pst.clt.typ.2)

#To check if it works
table(sc1$pst.clt.typ.2, useNA = "ifany") #Not sure the reason for the 48 Zeros. 

#vedaste to come back and understand what's happening
test <- subset(sc1, sc1$pst.clt.typ.2 == 0)

#4.7.2 Creating the Client Type for Past season 1
#-----------------------------------------------------------------------------
sc1$pst.clt.typ.1 <- ifelse(is.na(sc1$pst.dln.1) | is.infinite(sc1$pst.dln.1), "FALSE", 0)

sc1$pst.clt.typ.1 <- ifelse(!is.na(sc1$pst.dln.1) & 
                                    sc1$pst.dln.1 > sc1$lst.pos.dt, "Top Up", sc1$pst.clt.typ.1)

sc1$pst.clt.typ.1 <- ifelse(!is.na(sc1$pst.dln.1) & 
                                    sc1$amt.pd.by.pst.dln.1 >= sc1$pst.crdt.1 &
                                    sc1$pst.dln.1 > sc1$lst.pos.dt, "Returning", sc1$pst.clt.typ.1)

sc1$pst.clt.typ.1 <- ifelse(!is.na(sc1$pst.dln.1) & 
                                    sc1$amt.pd.by.pst.dln.1 < sc1$pst.crdt.1 &
                                    sc1$amt.pd.by.lst.pos.date >= sc1$pst.crdt.1, "Blacklisted Defaulter", sc1$pst.clt.typ.1)

sc1$pst.clt.typ.1 <- ifelse(!is.na(sc1$pst.dln.1) & 
                                    sc1$amt.pd.by.lst.pos.date >= sc1$pst.crdt.1 &
                                    sc1$amt.pd.by.pst.dln.1 < sc1$pst.crdt.1 & 
                                    sc1$pst.clt.typ.2 != "Forgiven Defaulter" &
                                    sc1$pst.clt.typ.2 != "Blacklisted Defaulter", "Forgiven Defaulter", sc1$pst.clt.typ.1)

sc1$pst.clt.typ.1 <- ifelse(!is.na(sc1$pst.dln.1) & 
                                    sc1$amt.pd.by.pst.dln.1 < sc1$pst.crdt.1 & 
                                    (sc1$pst.clt.typ.2 == "Blacklisted Defaulter" |
                                    sc1$pst.clt.typ.2 == "Forgiven Defaulter"), "Double Defaulter", sc1$pst.clt.typ.1)
#checks
table(sc1$pst.clt.typ.1, useNA = "ifany") #196 zeros

#vedaste to come back and understand what's happening
test1 <- subset(sc1, sc1$pst.clt.typ.1 == 0)

#4.7.3 let's create the current client type
#####################################################################################################
sc1$clt.typ <- ifelse(!is.na(sc1$crnt.dln) & 
                              today() <= sc1$crnt.dln & sc1$pst.clt.typ.1 != "Forgiven Defaulter", "Top Up", 0)

sc1$clt.typ <- ifelse(!is.na(sc1$crnt.dln) & 
                              today() > sc1$crnt.dln & sc1$TotalRepaid_IncludingOverpayments >= sc1$TotalCredit & 
                              sc1$amt.pd.by.crt.dln >= sc1$TotalCredit, "Returning", sc1$clt.typ) 


sc1$clt.typ <- ifelse(!is.na(sc1$crnt.dln) & 
                              today() > sc1$crnt.dln & sc1$TotalRepaid_IncludingOverpayments < sc1$TotalCredit, 
                      "Blacklisted Defaulter", sc1$clt.typ) 


sc1$clt.typ <- ifelse(!is.na(sc1$crnt.dln) & 
                              today() > sc1$crnt.dln & 
                              sc1$TotalRepaid_IncludingOverpayments >= sc1$TotalCredit & 
                              sc1$pst.clt.typ.1 != "Forgiven Defaulter" & 
                              sc1$pst.clt.typ.1 != "Blacklisted Defaulter" &
                              sc1$pst.clt.typ.1 != "Double Defaulter", "Forgiven Defaulter", sc1$clt.typ) 

sc1$clt.typ <- ifelse(!is.na(sc1$crnt.dln) & 
                              today() <= sc1$crnt.dln & 
                              sc1$actv.tot.credit > 0 &
                              sc1$pst.clt.typ.1 == "Forgiven Defaulter", "Active Forgiveness Credit", sc1$clt.typ) 

sc1$clt.typ <- ifelse(!is.na(sc1$crnt.dln) & 
                              today() > sc1$crnt.dln & 
                              sc1$amt.pd.by.crt.dln < sc1$TotalCredit &
                              (sc1$pst.clt.typ.1 == "Blacklisted Defaulter" | 
                                       sc1$pst.clt.typ.1 == "Forgiven Defaulter" |
                                       sc1$pst.clt.typ.1 == "Double Defaulter"),"Double Defaulter", sc1$clt.typ) 


table(sc1$clt.typ, useNA = "ifany") #1452 zeros. why?

#Vedaste; To check later

#4.8 Generating Client status based on current credit
###############################################################################
sc1$topup <- ifelse(!is.na(sc1$crnt.dln) &  
                            today() <= sc1$crnt.dln & 
                            sc1$pst.clt.typ.1 != "Forgiven Defaulter",
                            "Top-up", F)

#checks 
table(sc1$topup, useNA = "ifany")
head(sc1$topup)

sc1$returning <- ifelse(!is.na(sc1$crnt.dln) &  
                                today() > sc1$crnt.dln &
                                sc1$TotalRepaid_IncludingOverpayments >= sc1$TotalCredit &
                                sc1$amt.pd.by.crt.dln >= sc1$TotalCredit, "Returning", F)
#checks 
table(sc1$returning, useNA = "ifany")
head(sc1$returning)

sc1$bcklist.dfter <- ifelse(!is.na(sc1$crnt.dln) &  
                                today() > sc1$crnt.dln &
                                sc1$TotalRepaid_IncludingOverpayments < sc1$TotalCredit, 
                                "Blacklisted Defaulter", F)

#checks 
table(sc1$bcklist.dfter, useNA = "ifany")
head(sc1$bcklist.dfter)

#Since the last repayment date, we need to turn it into Data format first
class(sc1$LastRepayment)
head(sc1$LastRepayment)
#First, let's cut time from here
sc1$LastRepayment <- substr(sc1$LastRepayment, 1, 10)

#to check if it works
head(sc1$LastRepayment)
#Then, we do
sc1$LastRepayment <- as.Date(sc1$LastRepayment, "%m/%d/%Y")
#checks
class(sc1$LastRepayment)

sc1$forgiven.dfter <- ifelse(!is.na(sc1$crnt.dln) &  
                                     today() > sc1$crnt.dln &
                                     sc1$TotalRepaid_IncludingOverpayments >= sc1$TotalCredit & 
                                     sc1$LastRepayment > sc1$crnt.dln &
                                     sc1$pst.clt.typ.1 != "Forgiven Defaulter" & 
                                     sc1$pst.clt.typ.1 != "Blacklisted Defaulter" &
                                     sc1$pst.clt.typ.1 != "Double Defaulter", 
                                     "Forgiven Defaulter", F)
#checks 
table(sc1$forgiven.dfter, useNA = "ifany")
head(sc1$forgiven.dfter)

sc1$actv.forgiveness.crdt <- ifelse(!is.na(sc1$crnt.dln) &  
                                     today() <= sc1$crnt.dln &
                                     sc1$actv.tot.credit > 0 & 
                                     sc1$pst.clt.typ.1 == "Forgiven Defaulter", 
                                     "Active Forgiveness Credit", F)
#checks 
table(sc1$actv.forgiveness.crdt, useNA = "ifany")
head(sc1$actv.forgiveness.crdt)

#Lastly, 
sc1$double.dfter <- ifelse(!is.na(sc1$crnt.dln) &  
                                            today() > sc1$crnt.dln &
                                            sc1$amt.pd.by.crt.dln <  sc1$TotalCredit & 
                                            (sc1$pst.clt.typ.1 == "Blacklisted Defaulter" |
                                             sc1$pst.clt.typ.1 == "Forgiven Defaulter" |
                                             sc1$pst.clt.typ.1 == "Double Defaulter"), 
                                             "Double Defaulter", F)
#checks 
table(sc1$double.dfter, useNA = "ifany")
head(sc1$double.dfter)

#----------------------------------------------------------------------------
#4.9 Total MM Payment 
####################################################################

sc1$tot.mm.payment <- ifelse(sc1$actv.tot.credit > 0, 
                             sc1$actv.tot.repaid - sc1$prep.amount, 
                             sc1$old.credit - sc1$prep.amount)
#checks
head(sc1$tot.mm.payment)

#4.10 Credit eligibility - New credit terms 
##################################################################################

#Let's generate the credit eligibility 
sc1$crt.eligibility <- ifelse((sc1$clt.typ == "Blacklisted Defaulter" |
                               sc1$clt.typ == "actv.forgiveness.crdt" |      
                               sc1$clt.typ == "Double Defaulter"), "No",
                              ifelse(sc1$clt.typ == "Forgiven Defaulter", "Forgiveness Credit",
                              ifelse(!is.na(sc1$credit.shld.hv.bn.pd.b.elgible) & 
                                            sc1$TotalRepaid_IncludingOverpayments >= sc1$credit.shld.hv.bn.pd.b.elgible, "Yes", "No")))
                              
table(sc1$crt.eligibility, useNA = "ifany")

sc1$perc.ce.prepayment <- ifelse(sc1$crt.eligibility == "Yes", 0.3, 
                                 ifelse(sc1$crt.eligibility == "Forgiveness Credit", 0.5,
                                        "n/a"))       
#To check if it works
table(sc1$perc.ce.prepayment)

#calculating the minimun credit
sc1$min.credit <- ifelse(sc1$clt.typ == "Top Up" & sc1$crt.eligibility == "Yes", 0,
                         ifelse(sc1$crt.eligibility == "No", "n/a", 8000))

#to check
table(sc1$min.credit, useNA = "ifany")

#Generating the maximum credit
sc1$max.credit <- ifelse(!is.na(sc1$crt.eligibility) & sc1$crt.eligibility == "No", 0,
                         ifelse(sc1$crt.eligibility == "Forgiveness Credit", 45000,
                         ifelse(sc1$fst.crt.ev.tkn + 91 <= today(), 100000 - sc1$RemainingCredit,
                                45000 - sc1$RemainingCredit)))

#to check
table(sc1$max.credit, useNA = "ifany")
length(unique(sc1$OAFID[is.na(sc1$fst.crt.ev.tkn)]))

#About solar eligibility
#First, let's check the solar last POS date
class(sc1$shs.po.date)
class(sc1$skm.po.date)

#First, let's turn NAs and Inf into zero
sc1$shs.po.date <- ifelse(is.na(sc1$shs.po.date) | 
                                  is.infinite(sc1$shs.po.date), 0, sc1$shs.po.date)
sc1$skp.po.date <- ifelse(is.na(sc1$skp.po.date) | 
                                  is.infinite(sc1$skp.po.date), 0, sc1$skp.po.date)
sc1$skm.po.date <- ifelse(is.na(sc1$skm.po.date) | 
                                  is.infinite(sc1$skm.po.date), 0, sc1$skm.po.date)


#checks
table(sc1$shs.po.date, useNA = "ifany")
table(sc1$skp.po.date, useNA = "ifany")
table(sc1$skm.po.date, useNA = "ifany")

#Now, let's turn them to date
sc1$shs.po.date <- as.Date(sc1$shs.po.date)
sc1$skp.po.date <- as.Date(sc1$skp.po.date)
sc1$skm.po.date <- as.Date(sc1$skm.po.date)

#checks
head(sc1$shs.po.date)
class(sc1$shs.po.date)

sc1$solar.eligibility <- ifelse(sc1$shs.po.date + 365 > today() |
                                sc1$skp.po.date + 91 > today() |
                                sc1$skm.po.date + 91 > today() |
                                sc1$crt.eligibility == "Forgiveness Credit" |
                                sc1$crt.eligibility == "No", 0, 
                                ifelse(sc1$shs.po.date + 730 > today() &
                                       (sc1$skp.po.date + 91 < today() |
                                        sc1$skm.po.date + 91 < today()), "Small Solar Only", "Yes"))
#To check if it works
table(sc1$solar.eligibility, useNA = "ifany")
length(unique(sc1$OAFID[sc1$shs.po.date + 730 > today() & 
                                (sc1$skp.po.date + 91 < today() | 
                                 sc1$skm.po.date + 91 < today()) & 
                                sc1$crt.eligibility != "Forgiveness Credit" & 
                                sc1$crt.eligibility != "No"])) #0

#########################################################################
#Now, it's time to order columns
#############################################################
names(sc1)

sc2 <- sc1[c(#Season Clients data
            "OAFID", "SeasonName", "RegionName", "DistrictName", "SectorName",                             
            "FieldManager","SiteName", "FieldOfficer", "GroupName",                              
            "LastName","FirstName", "NationalID","FirstSeasonDataEntry",                   
            "FieldOfficerPayrollID", "FieldManagerPayrollID", "NewMember",                              
            "TotalEnrolledSeasons","Facilitator", "TotalCredit","TotalRepaid",                            
            "RemainingCredit", "X..Repaid", "FirstRepayment", "NbOfRepayments",                         
            "LastRepayment",  "TotalRepaid_IncludingOverpayments", "Dropped",                                
            "Deceased", "DataEntry", "ClientPhone", "AccountNumber", "ValidationCode",                         
            "GovLocationGrandParent", "GovLocationParent", "GovLocationChild",                       
            "GovLocationGrandChild", "SiteProjectCode", "GlobalClientID","X2018A_CycleCredit",                     
            "X2018B_CycleCredit", "X20_DAP.credit.kg", "X20A_H.1520.Cash.kg",                    
            "X20A_H.1520.credit.kg", "X20A_H.1602.Cash.kg", "X20A_H.1602.credit.kg",                  
            "X20A_H.513.Cash.kg", "X20A_H.513.Credit.kg", "X20A_H.628.Credit.kg",                   
            "X20A_H.629.Cash.kg", "X20A_H.629.Credit.kg", "X20A_KS.Chozi.Cash.kg",                  
            "X20A_KS.Chozi.credit.kg", "X20A_KS.Njoro.II.Cash.kg", "X20A_KS.Njoro.II.credit.kg",             
            "X20A_M101.Cash.kg", "X20A_M101.credit.kg", "X20A_NPK.credit.kg",                     
            "X20A_PAN.53.Cash.kg", "X20A_PAN.53.credit.kg", "X20A_PAN.691.credit.kg",                 
            "X20A_PICS.cash.qty", "X20A_PICS.credit.qty", "X20A_Pool.9A.credit.kg",                 
            "X20A_RHM.104.Cash.kg","X20A_RHM.104.credit.kg","X20A_RHM.1402.Cash.kg",                  
            "X20A_RHM.1402.credit.kg",  "X20A_RHM.1407.Cash.kg", "X20A_RHM.1407.credit.kg",                
            "X20A_SC.403.Cash.kg", "X20A_SC.403.Credit.kg","X20A_SC.637.Cash.kg",                    
            "X20A_SC.637.Credit.kg","X20A_SHS.cash.kg","X20A_SHS.credit.qty",                    
            "X20A_SKP.credit.qty","X20A_Travertine.cash.kg","X20A_Travertine.credit.kg",              
            "X20A_Urea.credit.kg", "X20A_WH.101.Credit.kg", "X20A_WH.403.Credit.kg",                  
            "X20A_WH.505.Credit.kg", "X20A_WH.507.Cash.kg","X20A_WH.507.Credit.kg",                  
            "X20A_WH.509.Cash.kg", "X20A_WH.509.Credit.kg","X20A_WH.605.credit.kg",                  
            "X20A_Wheat.Local.Varieties.credit.kg","X20A_ZM.607.credit.kg",                  
            "X20B_SKP2.credit.qty","ChickenFeedsGrower_cash.qty",            
            "ChickenFeedsGrower_credit.qty","ChickenFeedsLayer_cash.qty",             
            "ChickenFeedsLayer_credit.qty","ChickenFeedsPrelayer_cash.qty",          
            "ChickenFeedsPrelayer_credit.qty","ChickenFeedsStarter_cash.qty",           
            "ChickenFeedsStarter_credit.qty","Cypermethrin100ml_cash.kg",              
            "Cypermethrin100ml_credit.kg", "DAP.Cash.kg","DAP.Credit.kg",                          
            "Dithane_cash.kg","Dithane_credit.kg","H628.Cash.kg","H628.Credit.kg",                         
            "KS.Njoro.I.Cash.kg","KS.Njoro.I.Credit.kg","NPK17.Cash.kg",                          
            "NPK17.Credit.kg","PAN.691.Cash.kg", "PAN.691.Credit.kg", "PICS.Cash.qty",                          
            "PICS.credit.qty","Pool.9A.Cash.kg","Pool.9A.Credit.kg",                      
            "Pumps10_cash.qty", "Pumps10_credit.qty","Pumps20_credit.qty",                     
            "Pumps20Lcash.qty", "Roket100ml_cash.kg", "Roket100ml_credit.kg",                   
            "SHS.Cash.qty", "SHS.Credit.qty","SKM.Cash.qty","SKM.credit.qty",                         
            "SKP.cash.kg", "SKP.Cash.qty","SKP.credit.LP.qty", "SKP1_cash.qty",                          
            "SKP1_credit.qty","SKP200_cash.qty", "SKP200_Credit.qty",                      
            "TUBURA.Shops.Credit.qty", "UREA.Cash.kg","UREA.Credit.kg",                         
            "WH.101.Cash.kg", "WH.101.Credit.kg", "WH.403.cash.kg",                         
            "WH.403.credit.kg", "WH.505.cash.kg","WH.505.Credit.kg",                       
            "WH.605.cash.kg", "WH.605.Credit.kg",
            
            #Past Credit 2
            "pst.clt.typ.2", "pst.crdt.2", "past.prep.amount.2",
            "amt.pd.by.pst.dln.2", "amt.pd.by.past.pos.date.1",
            "past.pos.date.2", "pst.dln.2", 
            
            #Past Credit 1
            "pst.clt.typ.1", "pst.crdt.1", "past.prep.amount.1",
            "amt.pd.by.pst.dln.1", "amt.pd.by.lst.pos.date",
            "past.pos.date.1", "pst.dln.1", 
            
            
            #Current Credit
            "topup", "returning", "bcklist.dfter", "forgiven.dfter",                          
            "actv.forgiveness.crdt", "double.dfter",
            "clt.typ","amt.pd.by.crt.dln", "lst.pos.dt", "crnt.dln", 
            
            #Current credit details
            "credit.type", "shs.po.date","skm.po.date", "skp.po.date",
            "updtd.prep.amount" , "tot.mm.payment", "adjstd.lst.pos.dt",
            "credit.length", "monthy.payment", 
            
            #Current credit payments
            "n.credit", "n.months.shld.hv.bn.pd", "amt.shld.hv.bn.pd.snc.lst.pos.b.elgible", 
            "credit.shld.hv.bn.pd.b.elgible", 
            
            #Credit eligibility(New credit terms)
            "crt.eligibility", "perc.ce.prepayment", "min.credit",                              
            "max.credit" , "solar.eligibility", 
            
            #Credit overview
            "old.credit", "actv.tot.credit", "actv.tot.repaid", 
            "fst.crt.ev.tkn")]    

#Saving and exporting the final output
save(sc2, file = paste(wd,"pshops_database_mar_25.RData", sep ="/"))

#loading the file
#load(file = paste(wd,"pshops_database_mar_25.RData", sep ="/"))

write.table(sc2, file = paste(od, "pshops_database_mar_25.csv", sep = "/"),
            row.names = FALSE, col.names = TRUE, sep = ",")
                                                          
                                         
                                                                   
                                                                   
                                                                         
                                                                    
                                                                  
                                                                 
                                                              
                                        







