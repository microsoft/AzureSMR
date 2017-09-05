#' This is a template to view a DT object
#' 
#' @param data.frame
#' @param title Text to display in pane title

#' @importFrom miniUI miniPage gadgetTitleBar miniContentPanel
#' @importFrom DT dataTableOutput renderDataTable
#' @importFrom shiny observeEvent paneViewer runGadget
dataTableViewer <- function(x, title = "") {
  ui <- miniPage(
    gadgetTitleBar(title),
    miniContentPanel(
      DT::dataTableOutput("dt", height = "100%")
    )
  )

  server <- function(input, output, session) {
    output$dt <- DT::renderDataTable(x)
    observeEvent(input$done, stopApp())
  }

  viewer <- paneViewer()
  runGadget(ui, server, viewer = viewer)
}


addinListAllResources <- function() {
  settingsfile <- getOption("AzureSMR.config")
  assert_that(file.exists(settingsfile))
  az <- createAzureContext(configFile = settingsfile)
  z <- azureListAllResources(az)
  z$id <- NULL
  dataTableViewer(z, title = "All resources")
}

addinGetAllVMstatus <- function() {
  settingsfile <- getOption("AzureSMR.config")
  assert_that(file.exists(settingsfile))
  az <- createAzureContext(configFile = settingsfile)
  z <- azureGetAllVMstatus(az)
  z$id <- NULL
  dataTableViewer(z, title = "All virtual machines")
}