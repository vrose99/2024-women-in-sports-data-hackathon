import os
import json
import pandas as pd
from sqlalchemy import create_engine
from tqdm import tqdm
import warnings
import argparse

# Suppress FutureWarnings
warnings.simplefilter(action='ignore', category=FutureWarning)

# Setup argument parser
parser = argparse.ArgumentParser(description='Process JSONL files and store data in a SQLite database.')
parser.add_argument('directory', type=str, help='The directory containing the JSONL files.')
parser.add_argument('sqlite_file', type=str, help='The SQLite file name.')

args = parser.parse_args()

# Directory containing JSONL files
directory = args.directory

# Create a connection to the SQLite database
sqlite_file = args.sqlite_file
engine = create_engine(f'sqlite:///{sqlite_file}')

# Function to parse JSONL files and create DataFrames
def parse_jsonl_to_dfs(file_path):
    pitch_dfs = []
    hit_dfs = []
    score_dfs = []
    events_dfs = []
    ball_samples_dfs = []
    bat_samples_dfs = []

    with open(file_path, 'r') as f:
        for line in f:
            json_data = json.loads(line.strip())
            
            # Parse pitch data
            pitch_data = json_data['summary_acts']['pitch']
            if pitch_data['eventId']:
                pitch_data_flat = {
                    'pitch_eventId': pitch_data.get('eventId'),
                    'type': json.dumps(pitch_data.get('type', {})),  # Convert to JSON string
                    'result': pitch_data.get('result'),
                    'action': json.dumps(pitch_data.get('action', {})),  # Convert to JSON string
                    'speed_mph': pitch_data['speed'].get('mph'),
                    'speed_kph': pitch_data['speed'].get('kph'),
                    'speed_mps': pitch_data['speed'].get('mps'),
                    'spin_rpm': pitch_data['spin'].get('rpm')
                }
                pitch_dfs.append(pd.DataFrame([pitch_data_flat]))

            # Parse hit data
            hit_data = json_data['summary_acts']['hit']
            if hit_data['eventId'] and any(hit_data['speed'].values()) and hit_data['spin']['rpm'] is not None:
                hit_data_flat = {
                    'hit_eventId': hit_data.get('eventId'),
                    'speed_mph': hit_data['speed'].get('mph'),
                    'speed_kph': hit_data['speed'].get('kph'),
                    'speed_mps': hit_data['speed'].get('mps'),
                    'spin_rpm': hit_data['spin'].get('rpm')
                }
                hit_dfs.append(pd.DataFrame([hit_data_flat]))

            # Parse score summary
            score_data = json_data['summary_score']
            if score_data:
                score_data_flat = {
                    'pitch_eventId': pitch_data.get('eventId'),
                    'hit_eventId': hit_data.get('eventId'),
                    'runs_game_team1': score_data['runs']['game']['team1'],
                    'runs_game_team2': score_data['runs']['game']['team2'],
                    'runs_innings_team1': json.dumps([inning['team1'] for inning in score_data['runs']['innings']]),
                    'runs_innings_team2': json.dumps([inning['team2'] for inning in score_data['runs']['innings']]),
                    'outs_inning': score_data['outs']['inning'],
                    'outs_play': score_data['outs']['play'],
                    'count_balls_plateAppearance': score_data['count']['balls']['plateAppearance'],
                    'count_balls_play': score_data['count']['balls']['play'],
                    'count_strikes_plateAppearance': score_data['count']['strikes']['plateAppearance'],
                    'count_strikes_play': score_data['count']['strikes']['play']
                }
                score_dfs.append(pd.DataFrame([score_data_flat]))

            # Parse events
            events_data = json_data['events']
            for event in events_data:
                if event['eventId']:
                    event_flat = {
                        'pitch_eventId': pitch_data.get('eventId'),
                        'hit_eventId': hit_data.get('eventId'),
                        'eventId': event.get('eventId'),
                        'type': event.get('type'),
                        'angle_x': event['start']['angle'][0],
                        'angle_y': event['start']['angle'][1],
                        'teamId_mlbId': event['teamId']['mlbId'],
                        'personId_mlbId': event['personId']['mlbId']
                    }
                    events_dfs.append(pd.DataFrame([event_flat]))

            # Parse ball samples
            ball_samples_data = json_data['samples_ball']
            for sample in ball_samples_data:
                if sample['pos']:
                    sample_flat = {
                        'pitch_eventId': pitch_data.get('eventId'),
                        'hit_eventId': hit_data.get('eventId'),
                        'time': sample.get('time'),
                        'pos_x': sample['pos'][0],
                        'pos_y': sample['pos'][1],
                        'pos_z': sample['pos'][2],
                        'vel_x': sample.get('vel', [None, None, None])[0],
                        'vel_y': sample.get('vel', [None, None, None])[1],
                        'vel_z': sample.get('vel', [None, None, None])[2],
                        'acc_x': sample.get('acc', [None, None, None])[0],
                        'acc_y': sample.get('acc', [None, None, None])[1],
                        'acc_z': sample.get('acc', [None, None, None])[2]
                    }
                    ball_samples_dfs.append(pd.DataFrame([sample_flat]))

            # Parse bat samples
            bat_samples_data = json_data['samples_bat']
            for sample in bat_samples_data:
                if sample.get('head') or sample.get('handle'):
                    sample_flat = {
                        'pitch_eventId': pitch_data.get('eventId'),
                        'hit_eventId': hit_data.get('eventId'),
                        'event': sample.get('event'),
                        'time': sample.get('time'),
                        'head_pos_x': sample.get('head', {'pos': [None, None, None]})['pos'][0],
                        'head_pos_y': sample.get('head', {'pos': [None, None, None]})['pos'][1],
                        'head_pos_z': sample.get('head', {'pos': [None, None, None]})['pos'][2],
                        'handle_pos_x': sample.get('handle', {'pos': [None, None, None]})['pos'][0],
                        'handle_pos_y': sample.get('handle', {'pos': [None, None, None]})['pos'][1],
                        'handle_pos_z': sample.get('handle', {'pos': [None, None, None]})['pos'][2]
                    }
                    bat_samples_dfs.append(pd.DataFrame([sample_flat]))

    return pitch_dfs, hit_dfs, score_dfs, events_dfs, ball_samples_dfs, bat_samples_dfs

# Initialize empty DataFrames
pitch_df_all = pd.DataFrame()
hit_df_all = pd.DataFrame()
score_df_all = pd.DataFrame()
events_df_all = pd.DataFrame()
ball_samples_df_all = pd.DataFrame()
bat_samples_df_all = pd.DataFrame()

# Read and parse each JSONL file with progress tracking
jsonl_files = [f for f in os.listdir(directory) if f.endswith('.jsonl')]

for filename in tqdm(jsonl_files, desc="Processing JSONL files"):
    file_path = os.path.join(directory, filename)
    pitch_dfs, hit_dfs, score_dfs, events_dfs, ball_samples_dfs, bat_samples_dfs = parse_jsonl_to_dfs(file_path)
    
    # Append data to the main DataFrames only if they contain valid data
    if pitch_dfs:
        valid_pitch_dfs = [df for df in pitch_dfs if not df.isna().all().all()]
        if valid_pitch_dfs:
            pitch_df_all = pd.concat([pitch_df_all, *valid_pitch_dfs], ignore_index=True)

    if hit_dfs:
        valid_hit_dfs = [df for df in hit_dfs if not df.isna().all().all()]
        if valid_hit_dfs:
            hit_df_all = pd.concat([hit_df_all, *valid_hit_dfs], ignore_index=True)

    if score_dfs:
        valid_score_dfs = [df for df in score_dfs if not df.isna().all().all()]
        if valid_score_dfs:
            score_df_all = pd.concat([score_df_all, *valid_score_dfs], ignore_index=True)

    if events_dfs:
        valid_events_dfs = [df for df in events_dfs if not df.isna().all().all()]
        if valid_events_dfs:
            events_df_all = pd.concat([events_df_all, *valid_events_dfs], ignore_index=True)

    if ball_samples_dfs:
        valid_ball_samples_dfs = [df for df in ball_samples_dfs if not df.isna().all().all()]
        if valid_ball_samples_dfs:
            ball_samples_df_all = pd.concat([ball_samples_df_all, *valid_ball_samples_dfs], ignore_index=True)

    if bat_samples_dfs:
        valid_bat_samples_dfs = [df for df in bat_samples_dfs if not df.isna().all().all()]
        if valid_bat_samples_dfs:
            bat_samples_df_all = pd.concat([bat_samples_df_all, *valid_bat_samples_dfs], ignore_index=True)

# Function to clean DataFrame column names
def clean_column_names(df):
    df.columns = [c.replace(' ', '_').replace('.', '_').replace('-', '_') for c in df.columns]
    return df

# Clean column names for all DataFrames
pitch_df_all = clean_column_names(pitch_df_all)
hit_df_all = clean_column_names(hit_df_all)
score_df_all = clean_column_names(score_df_all)
events_df_all = clean_column_names(events_df_all)
ball_samples_df_all = clean_column_names(ball_samples_df_all)
bat_samples_df_all = clean_column_names(bat_samples_df_all)

# Ensure no lists are present in the DataFrames
for df in [pitch_df_all, hit_df_all, score_df_all, events_df_all, ball_samples_df_all, bat_samples_df_all]:
    for column in df.columns:
        df[column] = df[column].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)

# Store DataFrames into SQLite tables
pitch_df_all.to_sql('pitch_data', engine, if_exists='replace', index=False)
hit_df_all.to_sql('hit_data', engine, if_exists='replace', index=False)
score_df_all.to_sql('score_data', engine, if_exists='replace', index=False)
events_df_all.to_sql('events_data', engine, if_exists='replace', index=False)
ball_samples_df_all.to_sql('ball_samples_data', engine, if_exists='replace', index=False)
bat_samples_df_all.to_sql('bat_samples_data', engine, if_exists='replace', index=False)

print("Data successfully stored in SQLite database.")