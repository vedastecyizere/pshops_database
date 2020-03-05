library(keyring)

db_user <- "Vedaste.Cyizere"
db_pass <- key_get("roster", username=db_user)
db_name <- "RosterDatawarehouse"
db_server <- "mtama.oneacrefund.org,6543"