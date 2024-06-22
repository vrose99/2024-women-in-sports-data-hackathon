# 2024 Women in Sports Data Submission : Team Goldeyes
### Sophia Izokun and Tori Rose
### Mentor: Lauren Lott

## Reproduction overview
This Shiny app from R can be run simply from R studio after a ingesting the raw data provided by the organizers using python. 

## Step 0: Environment
This code was originally ran on MacOS using **Python 3.12** and **RStudio 1.4.1103** with **R 4.0.3**.

 If there are any issues running this code, please reach out to Tori Rose on the WISD slack.

## Step 1: Data Ingest
```
2024-women-in-sports-data-hackathon/
├── ingest/
│   ├── README.md
│   ├── requirements.txt
│   └── ingest.py
└── LOCAL WISD DIR/
```
1. Download all data from the organizers into a local directory that contains just these files. This will define `<LOCAL WISD DIR PATH>`

2. Install requirements.txt
> pip install -r requirements.txt

3. Run `python ./ingest/init.py <LOCAL WISD DIR PATH> wisd_data_updated.db`

## Step 2: Running the App
```
2024-women-in-sports-data-hackathon/
├── shiny_app/
│   ├── app.R
│   ├── wisd_data_updated.db
│   └── R/
│       ├── global.R
│       ├── server.R
│       └── ui.R

```
In order to run the app, please open up RStudio and open up `shiny_app/R/server.R`. A gree prompt to `Run App` should appear and selecting that should launch the application.

[This website](https://shiny.posit.co/r/getstarted/shiny-basics/lesson1/index.html) is helpful for reference for running the shiny app

