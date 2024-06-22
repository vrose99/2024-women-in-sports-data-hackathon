library(shiny)
library(DBI)
library(RSQLite)
library(ggplot2)
library(dplyr)
library(tibble)
library(plotly)
library(shinyWidgets)
library(purrr)
library(MASS)
library(DT)

# Helper function for lookup
try_lookup <- function(dict, value) {
  if (value %in% names(dict)) {
    return(dict[[value]])
  } else {
    return('NO SWING')
  }
}

# Function to get batted ball outcome
get_batted_ball_outcome <- function(launch_angle) {
  if (launch_angle < 10) {
    return('Ground ball')
  } else if (launch_angle >= 10 & launch_angle <= 25) {
    return('Line Drive')
  } else if (launch_angle > 25 & launch_angle <= 50) {
    return('Fly ball')
  } else if (launch_angle > 50) {
    return('Pop up')
  } else {
    return('Unknown')
  }
}

id_to_name <- list(
  `172804761` = "John Smith",
  `174158975` = "Michael Johnson",
  `223971350` = "David Brown",
  `290569727` = "James Williams",
  `352830460` = "Robert Jones",
  `360906992` = "William Garcia",
  `412098649` = "Charles Miller",
  `432216743` = "Joseph Martinez",
  `451871192` = "Thomas Davis",
  `459722179` = "Christopher Rodriguez",
  `474808052` = "Daniel Hernandez",
  `485007791` = "Matthew Moore",
  `505414610` = "Anthony Taylor",
  `518481551` = "Mark Anderson",
  `545569723` = "Paul Thomas",
  `558675411` = "Steven Jackson",
  `563942271` = "Andrew White",
  `568527038` = "Joshua Harris",
  `590082479` = "Kenneth Martin",
  `617522563` = "Kevin Thompson",
  `618024297` = "Brian Garcia",
  `654287703` = "George Martinez",
  `686425745` = "Edward Robinson",
  `719146721` = "Ronald Clark",
  `719210239` = "Timothy Lewis",
  `765710437` = "Jason Lee",
  `797796542` = "Jeffrey Walker",
  `797957728` = "Ryan Hall",
  `805688901` = "Jacob Allen",
  `849653732` = "Gary Young",
  `854238128` = "Nicholas King"
)


# Function to calculate swing data
get_swing_df <- function(bat_samples_df, pitchEvent_ID) {
  pitchEvent_df <- bat_samples_df %>% filter(pitch_eventId == pitchEvent_ID) %>% arrange(time)
  if (nrow(pitchEvent_df) == 0) {
    return(data.frame())
  }
  
  if ('event' %in% colnames(pitchEvent_df)) {
    start_index <- which(pitchEvent_df$event == 'First')[1]
    end_index <- which(pitchEvent_df$event == 'Last')[length(which(pitchEvent_df$event == 'Last'))]
    swing_df <- pitchEvent_df[start_index:end_index, ]
  } else {
    swing_df <- pitchEvent_df
  }
  
  swing_df <- swing_df %>%
    mutate(
      head_speed = sqrt((head_pos_x - lag(head_pos_x))^2 + (head_pos_y - lag(head_pos_y))^2 + (head_pos_z - lag(head_pos_z))^2) / (time - lag(time)),
      handle_speed = sqrt((handle_pos_x - lag(handle_pos_x))^2 + (handle_pos_y - lag(handle_pos_y))^2 + (handle_pos_z - lag(handle_pos_z))^2) / (time - lag(time)),
      head_acceleration = (head_speed - lag(head_speed)) / (time - lag(time)),
      handle_acceleration = (handle_speed - lag(handle_speed)) / (time - lag(time)),
      angular_velocity = handle_speed / sqrt(handle_pos_x^2 + handle_pos_y^2)
    )
  
  return(swing_df)
}

# Axis labels mapping
axis_labels <- c(
  launch_angle = "Launch Angle (degrees)",
  spray_angle = "Spray Angle (degrees)",
  speed_mph = "Speed (mph)",
  count_strikes_plateAppearance = "Count Strikes (Plate Appearance)"
)

# Function to calculate avg_speed and avg_spin for a given personId_mlbId
calculate_avg_metrics <- function(df, personId) {
  df <- df %>% filter(personId_mlbId == personId)
  
  avg_speed <- mean(df$speed_mph, na.rm = TRUE)
  avg_spin_rate <- if ("spin_rpm" %in% colnames(df)) mean(df$spin_rpm, na.rm = TRUE) else NA
  total_hits <- nrow(df)
  hard_hit_rate <- mean(df$speed_mph > 95, na.rm = TRUE) * 100
  
  list(
    avg_speed = avg_speed,
    avg_spin_rate = avg_spin_rate,
    total_hits = total_hits,
    hard_hit_rate = hard_hit_rate
  )
}

server <- function(input, output, session) {
  
  conn <- dbConnect(RSQLite::SQLite(), '../wisd_data_updated.db')
  
  events_df <- dbReadTable(conn, 'events_data')
  hit_df <- dbReadTable(conn, 'hit_data')
  score_df <- dbReadTable(conn, 'score_data')
  ball_samples_df <- dbReadTable(conn, 'ball_samples_data')
  bat_samples_df <- dbReadTable(conn, 'bat_samples_data')
  
  # Create lookup dictionaries for pitch and hit events
  pitchId_batter_dict <- setNames(as.character(events_df$personId_mlbId), as.character(events_df$pitch_eventId))
  pitchId_team_dict <- setNames(as.character(events_df$teamId_mlbId), as.character(events_df$pitch_eventId))
  hitId_team_dict <- setNames(as.character(events_df$personId_mlbId), as.character(events_df$hit_eventId))
  hitId_pitchId_dict <- setNames(as.character(events_df$hit_eventId), as.character(events_df$pitch_eventId))
  
  # Create a list of valid player IDs
  valid_player_ids <- events_df %>%
    group_by(personId_mlbId) %>%
    filter(n() > 0) %>%
    pull(personId_mlbId) %>%
    unique()
  
  # Create a named list of valid player IDs for display purposes
  named_player_ids <- setNames(as.character(valid_player_ids), sapply(valid_player_ids, function(x) id_to_name[[as.character(x)]]))
  
  # Update the selectInput choices with the named list
  updateSelectInput(session, "playerId", choices = named_player_ids, selected = "617522563")
  updateSelectInput(session, "comparePlayerId", choices = named_player_ids)
  spray_angle_range <- range(events_df$spray_angle, na.rm = TRUE)
  
  # Update the slider input for time range
  observe({
    updateSliderInput(session, "timeRange",
                      min = min(bat_samples_df$time, na.rm = TRUE),
                      max = max(bat_samples_df$time, na.rm = TRUE),
                      value = c(min(bat_samples_df$time, na.rm = TRUE), max(bat_samples_df$time, na.rm = TRUE)))
  })
  
  output$playerSnapshot <- renderUI({
    req(input$playerId)
    player_team <- unique(score_df %>% filter(personId_mlbId == input$playerId) %>% pull(teamId_mlbId))
    team_color <- ifelse(player_team == 63813, "background-color: #9DE1FE;", ifelse(player_team == 90068, "background-color: #FF939E;", "background-color: white;"))
    img_src <- ifelse(player_team == 63813, 
                      "https://static.wikia.nocookie.net/muppet/images/4/4a/Kermit_baseball_Muppet_All_Stars.jpg/revision/latest/scale-to-width-down/300?cb=20230419140408", 
                      "https://static.wikia.nocookie.net/muppet/images/a/ab/MuppetSports-Animal-Baseball.jpg/revision/latest/scale-to-width-down/300?cb=20110521195325")
    
    player_metrics <- calculate_avg_metrics(hit_df, input$playerId)
    wellPanel(style = team_color,
              fluidRow(
                column(2, 
                       tags$div(
                         #tags$img(src = img_src, style = "height: 165pt; object-fit: cover; display: block; margin-left: auto; margin-right: auto;"), 
                         uiOutput("playerIdDisplay")
                       )
                ),
                column(3,
                       h4("Average Speed (mph):", round(player_metrics$avg_speed, 2)),
                       h4("Average Spin Rate:", ifelse(is.na(player_metrics$avg_spin_rate), "N/A", round(player_metrics$avg_spin_rate, 2))),
                ),
                column(3,
                       h4("Hard Hit Rate:", round(player_metrics$hard_hit_rate, 2), "%"),
                       h4("Total Hits:", player_metrics$total_hits)
      
                ),
                column(3,
                       h4("Team ID:", player_team)
                ),
              )
    )
  })
  
  output$playerIdDisplay <- renderUI({
    req(input$playerId)
    
    player_id <- input$playerId
    HTML(
      paste0(
        '<div style="text-align: center;">',
        '<span style="font-size: 18pt;">', id_to_name[[as.character(input$playerId)]], '</span><br>',
        '<span style="font-size: 12pt;">', player_id, '</span>',
        '</div>'
      )
    )
  })
  
  output$playerPosition <- renderText({
    req(input$playerId)
    hit_df %>% filter(personId_mlbId == input$playerId) %>% pull(position) %>% unique()
  })
  
  output$numberOfHits <- renderText({
    req(input$playerId)
    sum(events_df$personId_mlbId == input$playerId)
  })
  
  output$avgSpeed <- renderText({
    req(input$playerId)
    player_hits <- hit_df %>% filter(personId_mlbId == input$playerId)
    if(nrow(player_hits) == 0) return("N/A")
    mean(player_hits$speed_mph, na.rm = TRUE)
  })
  
  output$avgSpin <- renderText({
    req(input$playerId)
    player_hits <- hit_df %>% filter(personId_mlbId == input$playerId)
    if(nrow(player_hits) == 0) return("N/A")
    mean(player_hits$spin_rpm, na.rm = TRUE)
  })
  
  default_hit_event_id <- reactive({
    "22560b81-5e95-4221-9036-60b1b20ca497"
  })
  
  default_pitch_event_id <- reactive({
    "b120cf14-305c-442c-a739-c499bf61eec8"
  })
  
  output$comparisonPlot <- renderPlotly({
    req(input$playerId, input$xAxis, input$yAxis)
    
    # Filter data for the selected player
    batted_balls <- events_df %>% filter(personId_mlbId == input$playerId)
    
    # Join with hit_df and score_df to get necessary columns
    batted_balls <- batted_balls %>%
      left_join(hit_df, by = "hit_eventId") %>%
      rename_with(~ gsub("\\.x$", "", .), ends_with(".x"))
    
    batted_balls <- batted_balls %>%
      left_join(score_df, by = "pitch_eventId") %>%
      rename_with(~ gsub("\\.x$", "", .), ends_with(".x"))
    
    # Get comparison data
    if (input$comparison == "league") {
      
      comparison_data <- events_df %>%
        mutate(pitch_eventId = sapply(hit_eventId, function(x) try_lookup(hitId_pitchId_dict, x))) %>%
        left_join(hit_df, by = "hit_eventId") %>%
        left_join(score_df, by = "pitch_eventId")
    } else if (input$comparison == "team") {
      player_team <- unique(events_df %>% filter(personId_mlbId == input$playerId) %>% pull(teamId_mlbId))
      comparison_data <- events_df %>%
        filter(teamId_mlbId == player_team) %>%
        mutate(pitch_eventId = sapply(hit_eventId, function(x) try_lookup(hitId_pitchId_dict, x))) %>%
        left_join(hit_df, by = "hit_eventId") %>%
        left_join(score_df, by = "pitch_eventId")
    } else if (input$comparison == "player" && !is.null(input$comparePlayerId)) {
      comparison_data <- events_df %>%
        filter(personId_mlbId == input$comparePlayerId) %>%
        mutate(pitch_eventId = sapply(hit_eventId, function(x) try_lookup(hitId_pitchId_dict, x))) %>%
        left_join(hit_df, by = "hit_eventId") %>%
        left_join(score_df, by = "pitch_eventId")
    } else {
      comparison_data <- data.frame()
    }
    
    if(nrow(batted_balls) == 0) {
      plotly_empty() %>% 
        layout(title = list(text = "There isn't enough data for this player, please select another", 
                            x = 0.5, 
                            y = 0.5, 
                            xanchor = 'center', 
                            yanchor = 'middle'))
    } else {
      # Add batted ball outcome
      batted_balls <- batted_balls %>%
        mutate(batted_ball_outcome = sapply(launch_angle, get_batted_ball_outcome))
      # Create scatter plot
      
      plot <- ggplot() +
        geom_point(data = comparison_data, aes_string(x = input$xAxis, y = input$yAxis), color = 'grey', alpha = 0.7) +
        geom_point(data = batted_balls, aes_string(x = input$xAxis, y = input$yAxis, color = "batted_ball_outcome", key = "hit_eventId", text = "hit_eventId")) +
        labs(title = paste("Scatter Plot for Player ID:", input$playerId),
             x = axis_labels[input$xAxis],
             y = axis_labels[input$yAxis],
             color = "Batted Ball Outcome") +
        theme_minimal() +
        scale_y_continuous(limits = spray_angle_range)
      
      p <- ggplotly(plot, tooltip = "text")
      event_register(p, "plotly_click")
      p
    }
  })
  
  output$playerStatsTable <- renderDT({
    # Ensure columns exist
    if (!"pitch_eventId" %in% colnames(bat_samples_df)) {
      stop("Column 'pitch_eventId' not found in bat_samples_df")
    }
    
    if (!"pitch_eventId" %in% colnames(events_df) || !"personId_mlbId" %in% colnames(events_df)) {
      stop("Required columns not found in events_df")
    }
    
    # Join bat_samples_df with events_df to get personId_mlbId
    joined_df <- bat_samples_df %>%
      left_join(events_df, by = "pitch_eventId")
    
    # Apply the get_swing_df function to each pitch_eventId and combine the results
    swing_data_list <- lapply(unique(joined_df$pitch_eventId), function(id) {
      get_swing_df(joined_df, id)
    })
    
    swing_data_df <- do.call(rbind, swing_data_list)
    
    # Compute average angular velocity for each personId_mlbId
    avg_angular_velocity <- swing_data_df %>%
      group_by(personId_mlbId) %>%
      summarise(avg_angular_velocity = mean(angular_velocity, na.rm = TRUE))
    
    # Merge and calculate stats
    stats_df <- events_df %>%
      left_join(avg_angular_velocity, by = "personId_mlbId") %>%
      group_by(personId_mlbId) %>%
      summarise(
        avg_angular_velocity = round(mean(avg_angular_velocity, na.rm = TRUE), 3),
        num_hits = n(),
        avg_launch_angle = round(mean(launch_angle, na.rm = TRUE), 3),
        avg_spray_angle = round(mean(spray_angle, na.rm = TRUE), 3),
        avg_speed = round(mean(hit_df$speed_mph[hit_df$personId_mlbId == personId_mlbId], na.rm = TRUE), 3),
        hard_hit_rate = round(mean(hit_df$speed_mph[hit_df$personId_mlbId == personId_mlbId] > 95, na.rm = TRUE) * 100, 3)
      ) %>%
      mutate(
        Player_Name = sapply(personId_mlbId, function(id) id_to_name[[as.character(id)]]),
        Player_ID = personId_mlbId
      )
    
    # Reorder columns using dplyr::select
    stats_df <- stats_df %>%
      dplyr::select(Player_Name, Player_ID, avg_angular_velocity, num_hits, avg_launch_angle, avg_spray_angle, avg_speed, hard_hit_rate)
    
    # Update column names
    colnames(stats_df) <- c("Player Name", "Player ID", "Avg Angular Velocity", "Num Hits", "Avg Launch Angle", "Avg Spray Angle", "Avg Speed", "Hard Hit Rate (%)")
    
    datatable(stats_df)
  })
  
  
  
  output$normalCurvePlots <- renderPlotly({
    req(input$playerId, input$pitch_eventId)
    
    selected_bat_samples <- bat_samples_df %>%
      filter(pitch_eventId == input$pitch_eventId)
    
    if (nrow(selected_bat_samples) == 0) {
      return(plotly_empty() %>% 
               layout(title = list(text = "No data available for the selected pitch_eventId", 
                                   x = 0.5, 
                                   y = 0.5, 
                                   xanchor = 'center', 
                                   yanchor = 'middle')))
    }
    
    swing_df <- get_swing_df(selected_bat_samples, input$pitch_eventId)
    
    if (nrow(swing_df) == 0) {
      return(plotly_empty() %>% 
               layout(title = list(text = "No swing data available for the selected pitch_eventId", 
                                   x = 0.5, 
                                   y = 0.5, 
                                   xanchor = 'center', 
                                   yanchor = 'middle')))
    }
    
    # Calculate angular velocity
    player_angular_velocity <- swing_df$angular_velocity
    
    # Get team data if comparison is with team
    if (input$comparison == "team") {
      player_team <- unique(score_df %>% filter(personId_mlbId == input$playerId) %>% pull(teamId_mlbId))
      team_bat_samples <- bat_samples_df %>%
        filter(pitch_eventId %in% (events_df %>% filter(teamId_mlbId == player_team) %>% pull(pitch_eventId)))
      
      team_swing_df <- team_bat_samples %>%
        group_by(pitch_eventId) %>%
        do(get_swing_df(., .data$pitch_eventId))
      
      team_angular_velocity <- team_swing_df$angular_velocity
    } else if (input$comparison == "league") {
      league_bat_samples <- bat_samples_df
      league_swing_df <- league_bat_samples %>%
        group_by(pitch_eventId) %>%
        do(get_swing_df(., .data$pitch_eventId))
      
      team_angular_velocity <- league_swing_df$angular_velocity
    } else {
      team_angular_velocity <- numeric(0)
    }
    
    player_density <- density(player_angular_velocity, na.rm = TRUE)
    team_density <- if (length(team_angular_velocity) > 0) density(team_angular_velocity, na.rm = TRUE) else NULL
    player_avg_velocity <- mean(player_angular_velocity, na.rm = TRUE)
    
    plot <- plot_ly() %>%
      add_trace(x = player_density$x, 
                y = player_density$y, 
                type = 'scatter', 
                mode = 'lines', 
                name = 'Player Angular Velocity') %>%
      add_trace(x = c(player_avg_velocity, player_avg_velocity), 
                y = c(0, max(player_density$y)), 
                type = 'scatter', 
                mode = 'lines', 
                line = list(color = 'red', dash = 'dash'), 
                name = 'Average Angular Velocity') %>%
      layout(xaxis = list(title = "Angular Velocity", 
                          range = range(player_density$x)), 
             yaxis = list(title = "Density", 
                          range = c(0, max(player_density$y))))
    
    if (!is.null(team_density)) {
      plot <- plot %>%
        add_trace(x = team_density$x, 
                  y = team_density$y, 
                  type = 'scatter', 
                  mode = 'lines', 
                  line = list(dash = 'dot'),
                  name = 'Team/League Angular Velocity')
    }
    
    plot
  })
  
  output$batSamplePlots <- renderPlotly({
    req(input$playerId, input$pitch_eventId, input$timeRange)
    
    selected_bat_samples <- bat_samples_df %>%
      filter(pitch_eventId == input$pitch_eventId & time >= input$timeRange[1] & time <= input$timeRange[2])
    
    selected_ball_samples <- ball_samples_df %>%
      filter(pitch_eventId == input$pitch_eventId & time >= input$timeRange[1] & time <= input$timeRange[2])
    
    if (nrow(selected_bat_samples) == 0 || nrow(selected_ball_samples) == 0) {
      return(plotly_empty() %>% 
               layout(title = list(text = "No data available for the selected pitch_eventId", 
                                   x = 0.5, 
                                   y = 0.5, 
                                   xanchor = 'center', 
                                   yanchor = 'middle')))
    } else {
      plot <- plot_ly()
      
      plot <- plot %>%
        add_trace(data = selected_ball_samples, 
                  x = ~pos_x, 
                  y = ~pos_y, 
                  z = ~pos_z, 
                  type = "scatter3d", 
                  mode = "markers",
                  marker = list(color = ~time, colorscale = 'Blues', size = 2, showscale = TRUE),
                  name = "Ball Samples",
                  showlegend = FALSE)
      
      for (i in 1:nrow(selected_bat_samples)) {
        plot <- plot %>%
          add_trace(data = selected_bat_samples[i, ],
                    x = ~c(head_pos_x, handle_pos_x), 
                    y = ~c(head_pos_y, handle_pos_y), 
                    z = ~c(head_pos_z, handle_pos_z), 
                    type = "scatter3d", 
                    mode = "lines+markers",
                    line = list(color = 'rgba(255, 0, 0, 0.5)', width = 2),
                    marker = list(color = 'white', size = 2),
                    showlegend = FALSE)
      }
      
      plot <- plot %>%
        layout(scene = list(
          xaxis = list(title = "X Axis"),
          yaxis = list(title = "Y Axis"),
          zaxis = list(title = "Z Axis"),
          camera = list(
            eye = switch(input$view,
                         "x" = list(x = 2, y = 0, z = 0),
                         "y" = list(x = 0, y = 2, z = 0),
                         "z" = list(x = 0, y = 0, z = 2))
          )
        ))
      
      plot
    }
  })
  
  observeEvent(event_data("plotly_click"), {
    event_data <- event_data("plotly_click")
    hit_event_id <- event_data$key
    
    pitch_event_id <- events_df %>% filter(hit_eventId == hit_event_id) %>% pull(pitch_eventId)
    
    selected_ball_samples <- ball_samples_df %>%
      filter(pitch_eventId == pitch_event_id) 
    
    selected_bat_samples <- bat_samples_df %>%
      filter(pitch_eventId == pitch_event_id) 
    
    print("Selected ball samples columns: ")
    print(colnames(selected_ball_samples))
    
    print("Selected bat samples columns:")
    print(colnames(selected_bat_samples))
  })
  
  # Update hit_eventId choices based on the selected player
  observe({
    req(input$playerId)
    player_hit_events <- hit_df %>% filter(personId_mlbId == input$playerId) %>% pull(hit_eventId)
    updateSelectInput(session, "hit_eventId", choices = player_hit_events)
    
    player_pitch_events <- events_df %>% filter(personId_mlbId == input$playerId) %>% pull(pitch_eventId)
    updateSelectInput(session, "pitch_eventId", choices = player_pitch_events)
  })
  
  session$onSessionEnded(function() {
    dbDisconnect(conn)
  })
}
