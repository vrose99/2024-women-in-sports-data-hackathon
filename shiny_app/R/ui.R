library(shiny)
library(plotly)
library(shinyWidgets)
library(DT)


ui <- fluidPage(
  titlePanel("Player Angle and Hit Analysis Dashboard"),
  
  fluidRow(
    column(12,
           uiOutput("playerSnapshot")
    )
  ),
  
  sidebarLayout(
    sidebarPanel(
      h3("Player Comparison Controls"),
      selectInput("playerId", "Select Player:", choices = NULL),
      radioButtons("comparison", "Compare To:", 
                   choices = list("League Average" = "league", 
                                  "Team Average" = "team", 
                                  "Another Player" = "player")),
      selectInput("comparePlayerId", "Select Player for Comparison:", choices = NULL),
      fluidRow(
        column(6,
               selectInput("xAxis", "Select X Axis:", 
                           choices = list("Launch Angle" = "launch_angle", 
                                          "Spray Angle" = "spray_angle", 
                                          "Speed (mph)" = "speed_mph", 
                                          "Count Strikes (Plate Appearance)" = "count_strikes_plateAppearance"),
                           selected = "launch_angle")
        ),
        column(6,
               selectInput("yAxis", "Select Y Axis:", 
                           choices = list("Launch Angle" = "launch_angle", 
                                          "Spray Angle" = "spray_angle", 
                                          "Speed (mph)" = "speed_mph", 
                                          "Count Strikes (Plate Appearance)" = "count_strikes_plateAppearance"),
                           selected = "spray_angle")
        )
      ),
      selectInput("pitch_eventId", "Select Pitch Event:", choices = NULL),
      selectInput("view", "Select View:", 
                  choices = list("X Plane" = "x", "Y Plane" = "y", "Z Plane" = "z")),
      sliderInput("timeRange", "Time Range", 
                  min = 0, max = 1, value = c(0, 1)) # Placeholder values
    ),
    mainPanel(
      tabsetPanel(
        tabPanel("Scatter Plot", plotlyOutput("comparisonPlot")),
        tabPanel("3D Plot",
                 plotlyOutput("batSamplePlots"),
                 plotlyOutput("normalCurvePlots")),
        tabPanel("All Player Statistics", DTOutput("playerStatsTable"))
      )
    )
  )
)

