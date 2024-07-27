library(shiny)
library(DBI)
library(RSQLite)

# Source the UI and server files
source("R/ui.R")
source("R/server.R")

# Run the application
shinyApp(ui = ui, server = server)
