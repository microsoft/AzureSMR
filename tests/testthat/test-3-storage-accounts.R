if(interactive()) library("testthat")


settingsFile <- system.file("tests/testthat/config.json", package = "AzureSM")
config <- read.AzureSM.config(settingsFile)

asc <- CreateAzureContext()
with(config,
     SetAzureContext(asc, TID = TID, CID = CID, KEY = KEY)
)
AzureAuthenticate(asc)

#  ------------------------------------------------------------------------

context("Not used")


