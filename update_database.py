import sqlite3
import requests
from datetime import datetime, timedelta
import mysql.connector
from config import db_config
from config import db_config_local
import pandas as pd
import json
import copy
import os
import logging
import sys
import traceback

def setup_logging():
    """Configure logging for both file and console output"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout)  # This will go to your log file via cron redirect
        ]
    )
    return logging.getLogger(__name__)

# Create logger
logger = setup_logging()

teams_dict = {
    'team_id': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 46, 47, 48, 49, 52, 53, 54, 55, 56, 59, 68],
    'team_name': ['New Jersey Devils', 'New York Islanders', 'New York Rangers', 'Philadelphia Flyers', 'Pittsburgh Penguins', 'Boston Bruins', 'Buffalo Sabres', 'Montreal Canadiens', 'Ottawa Senators', 'Toronto Maple Leafs', 'Atlanta Thrashers', 'Carolina Hurricanes', 'Florida Panthers', 'Tampa Bay Lightning', 'Washington Capitals', 'Chicago Blackhawks', 'Detroit Red Wings', 'Nashville Predators', 'St Louis Blues', 'Calgary Flames', 'Colorado Avalanche', 'Edmonton Oilers', 'Vancouver Canucks', 'Anaheim Ducks', 'Dallas Stars', 'Los Angeles Kings', 'Pheonix Coyotes', 'San Jose Sharks', 'Columbus Blue Jackets', 'Minnesota Wild', 'Minnesota North Stars', 'Quebec Nordique', 'Winnipeg Jets (1979)', 'Hartford Whalers', 'Colorado Rockies', 'Oakland Seals', 'Atlanta Flames', 'Kansas City Scouts', 'Cleveland Barons', 'Winnipeg Jets', 'Arizona Coyotes', 'Vegas Golden Knights', 'Seattle Kraken', 'California Golden Seals', 'Utah Hockey Club', 'Utah Mammoth'],
    'team_abbreviation': ['NJD', 'NYI', 'NYR', 'PHI', 'PIT', 'BOS', 'BUF', 'MTL', 'OTT', 'TOR', 'ATL', 'CAR', 'FLA', 'TBL', 'WSH', 'CHI', 'DET', 'NSH', 'STL', 'CGY', 'COL', 'EDM', 'VAN', 'ANA', 'DAL', 'LAK', 'PHX', 'SJS', 'CBJ', 'MIN', 'MNS', 'QUE', 'WIN', 'HFD', 'CLR', 'OAK', 'AFM', 'KCS', 'CLE', 'WPG', 'ARI', 'VGK', 'SEA', 'CGS', 'UTA','UTA']
}

# Establish connection
connection = mysql.connector.connect(
    host=db_config["host"],
    port=db_config["port"],
    user=db_config["user"],
    password=db_config["password"],
    database=db_config["database"]
)
#local connection
# connection = mysql.connector.connect(
#     host=db_config_local["host"],
#     port=db_config_local["port"],
#     user=db_config_local["user"],
#     password=db_config_local["password"],
#     database=db_config_local["database"]
# )
cursor = connection.cursor()



def get_last_date_updated_db():
    cursor.execute('''
        SELECT game_id, season_id, date
        FROM games 
        WHERE game_outcome IS NOT NULL
        ORDER BY date DESC 
        LIMIT 1;
    ''')
    result = cursor.fetchone()
    return result if result else (None, None, None) 

def get_current_season():
    url = "https://api-web.nhle.com/v1/roster-season/TOR"
    response = requests.get(url)
    seasons = response.json()
    last_season = seasons[-1]  # latest season

    return last_season

def update_seasons_table(season_id):
    year = int(str(season_id)[:4])
    cursor.execute("""
    INSERT IGNORE INTO seasons (season_id, start_year, end_year)
    VALUES (%s, %s, %s)
    """, (season_id, year, year + 1))

def update_games_table(year):
    games = []
    url_base = 'https://api-web.nhle.com/v1/club-schedule-season/'
    updated_games = []
    today = datetime.now().date() + timedelta(days=170)  # Add 5 days

    # Get existing game IDs
    season_id = str(year) + str(year + 1)
    cursor.execute("SELECT game_id FROM games WHERE season_id = %s AND game_outcome IS NOT NULL", (int(season_id),))
    played_game_ids = {row[0] for row in cursor.fetchall()}

    logger.info(f"Found {len(played_game_ids)} played games for season {season_id}")  
    

    #for each team get the games for this season
    for abv in teams_dict['team_abbreviation']:
        logger.info(f"Processing games for team: {abv} in season starting {year}")
        start_date = f"{year}-10-01"
        end_date = f"{year + 1}-04-30"
        logger.info(f"Fetching games from {start_date} to {end_date} for team {abv}")


        url = url_base + abv + '/' + season_id

        response = requests.get(url)
        logger.info(year)
        if response.text.strip():
            try:
                data = json.loads(response.text)
                if data['games']:
                    for g in data['games']:
                        if g['id'] in played_game_ids or g['id'] in updated_games:
                            logger.info(f"Skipping already played game ID: {g['id']}")
                            continue
                        elif datetime.strptime(g['gameDate'], '%Y-%m-%d').date() > today:
                            #technically this can allow future games if they are scheduled for today.
                            #probably fine as it won't update anything and it can make sure the schedule is current
                            logger.info(f"Skipping future game ID: {g['id']} scheduled on {g['gameDate']}")
                            continue
                        updated_games.append(g['id'])

                        iso_datetime = g.get('startTimeUTC', None)
                        mysql_datetime = iso_datetime.replace('T', ' ').replace('Z', '') if iso_datetime else None
                        
                        #logger.info(mysql_datetime) 
                        #logger.info(g)
                        game = (
                            g['id'],
                            g.get('season', None),
                            g.get('gameType', None),
                            g.get('gameDate', None),
                            mysql_datetime,

                            g.get('homeTeam', {}).get('id', None),
                            g.get('awayTeam', {}).get('id', None),
                            g.get('homeTeam', {}).get('score', None),
                            g.get('awayTeam', {}).get('score', None),

                            g.get('gameOutcome', {}).get('lastPeriodType', None), 
                            g.get('winningGoalie', {}).get('playerId', None), 
                            g.get('winningGoalScorer', {}).get('playerId', None), 
                            g.get('seriesStatus', {}).get('round', None),
                        )
                        if  g.get('homeTeam', {}).get('id') in teams_dict['team_id'] and g.get('awayTeam', {}).get('id') in teams_dict['team_id']:
                            games.append(game)
                        else:
                            logger.info(f"Skipping game {g['id']} due to unknown team IDs: Home {g.get('homeTeam', {}).get('id')}, Away {g.get('awayTeam', {}).get('id')}")
                        

            except json.JSONDecodeError:
                pass

    #logger.info(games)
    cursor.executemany("""
    INSERT INTO games (game_id, season_id, game_type, date, start_time_UTC, home_team_id, away_team_id, home_score, away_score, game_outcome, winning_goalie_id, winning_goal_scorer_id, series_status_round)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        season_id = VALUES(season_id),
        game_type = VALUES(game_type),
        date = VALUES(date),
        start_time_UTC = VALUES(start_time_UTC),
        home_team_id = VALUES(home_team_id),
        away_team_id = VALUES(away_team_id),
        home_score = VALUES(home_score),
        away_score = VALUES(away_score),
        game_outcome = VALUES(game_outcome),
        winning_goalie_id = VALUES(winning_goalie_id),
        winning_goal_scorer_id = VALUES(winning_goal_scorer_id),
        series_status_round = VALUES(series_status_round)
    """, games)
    connection.commit()

    logger.info("Updated games:", updated_games)
    return updated_games

def update_seasons_end_standings(season_id):
    url_base = 'https://api-web.nhle.com/v1/standings/'
    #get info for the season for each team

    # Get data from games table

    cursor.execute("""
    SELECT season_id, home_team_id, away_team_id, home_score, away_score, game_outcome, game_type, date
    FROM games
    WHERE season_id = %s
    """, (season_id,))

    games_data = cursor.fetchall()

    url = url_base + str(season_id)[:4] + "-01-10"

    season_dict = {}

    response = requests.get(url)
    if response.text.strip():
        try:
            data = json.loads(response.text)
            if data['standings']:
                season_dict = {team['teamAbbrev']['default']: team for team in data['standings']}

        except json.JSONDecodeError:
            pass

    for team_id in teams_dict['team_id']:

        team_abbreviation = teams_dict['team_abbreviation'][teams_dict['team_id'].index(team_id)]

        seasons_end_standings_data = []

        #logger.info(season_id)

        wins = 0
        preseason_wins = 0
        playoff_wins = 0
        losses = 0
        preseason_losses = 0
        playoff_losses = 0
        ot_losses = 0
        preseason_ot_losses = 0
        playoff_ot_losses = 0
        points = 0
        goals_for = 0
        preseason_goals_for = 0
        playoff_goals_for = 0
        goals_against = 0
        preseason_goals_against = 0
        playoff_goals_against = 0
        games_played = 0
        preseason_games_played = 0
        playoff_games_played = 0
        conference_name = None
        division_name = None

        if season_dict:
            team_data = season_dict.get(team_abbreviation, {})
            conference_name = team_data.get('conferenceName', None)
            division_name = team_data.get('divisionName', None)

        for game in games_data:
            if game[5] is not None:
                if game[0] == season_id:
                    if game[1] == team_id:
                        logger.info(game)
                        if game[6] == 1:
                            preseason_games_played += 1
                            preseason_goals_for += game[3]
                            preseason_goals_against += game[4]
                            if game[3] > game[4]:
                                preseason_wins += 1
                            elif game[3] < game[4]:
                                if game[5] == 'OT' or game[5] == 'SO':
                                    preseason_ot_losses += 1
                                else:
                                    preseason_losses += 1
                        elif game[6] == 2:
                            games_played += 1
                            goals_for += game[3]
                            goals_against += game[4]
                            if game[3] > game[4]:
                                wins += 1
                            elif game[3] < game[4]:
                                if game[5] == 'OT' or game[5] == 'SO':
                                    ot_losses += 1
                                else:
                                    losses += 1
                        elif game[6] == 3:
                            playoff_games_played += 1
                            playoff_goals_for += game[3]
                            playoff_goals_against += game[4]
                            if game[3] > game[4]:
                                playoff_wins += 1
                            elif game[3] < game[4]:
                                if game[5] == 'OT' or game[5] == 'SO':
                                    playoff_ot_losses += 1
                                else:
                                    playoff_losses += 1
                    elif game[2] == team_id:
                        logger.info(game)
                        if game[6] == 1:
                            preseason_games_played += 1
                            preseason_goals_for += game[4]
                            preseason_goals_against += game[3]
                            if game[4] > game[3]:
                                preseason_wins += 1
                            elif game[4] < game[3]:
                                if game[5] == 'OT' or game[5] == 'SO':
                                    preseason_ot_losses += 1
                                else:
                                    preseason_losses += 1
                        elif game[6] == 2:
                            games_played += 1
                            goals_for += game[4]
                            goals_against += game[3]
                            if game[4] > game[3]:
                                wins += 1
                            elif game[4] < game[3]:
                                if game[5] == 'OT' or game[5] == 'SO':
                                    ot_losses += 1
                                else:
                                    losses += 1
                        elif game[6] == 3:
                            playoff_games_played += 1
                            playoff_goals_for += game[4]
                            playoff_goals_against += game[3]
                            if game[4] > game[3]:
                                playoff_wins += 1
                            elif game[4] < game[3]:
                                if game[5] == 'OT' or game[5] == 'SO':
                                    playoff_ot_losses += 1
                                else:
                                    playoff_losses += 1

        points = 2 * wins + ot_losses
        #logger.info(f"Season_id: {season_id}, Team_id: {team_id}, Wins: {wins}, Losses: {losses}, OT_Losses: {ot_losses}, Points: {points}, Goals_For: {goals_for}, Goals_Against: {goals_against}")
        seasons_end_standings_data.append((season_id, team_id, wins, losses, ot_losses, points, games_played, goals_for, goals_against, preseason_wins, preseason_losses, preseason_ot_losses, preseason_goals_for, preseason_goals_against, preseason_games_played, playoff_wins, playoff_losses, playoff_ot_losses, playoff_goals_for, playoff_goals_against, playoff_games_played, conference_name, division_name))


        logger.info(f"Standings: {seasons_end_standings_data}")
        # Populate seasons_end_standings table
        cursor.executemany("""
        REPLACE INTO seasons_end_standings (season_id, team_id, wins, losses, ot_losses, points, games_played, goals_for, goals_against, preseason_wins, preseason_losses, preseason_ot_losses, preseason_goals_for, preseason_goals_against, preseason_games_played, playoff_wins, playoff_losses, playoff_ot_losses, playoff_goals_for, playoff_goals_against, playoff_games_played, conference_name, division_name)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, seasons_end_standings_data)

        connection.commit()

def update_events_table(season_id, updated_game_ids):
    #grab each game_id from the games table
    url_base = "https://api-web.nhle.com/v1/gamecenter/"#2023020204/play-by-play

    # cursor.execute("""
    #     SELECT DISTINCT game_id
    #     FROM games
    #     WHERE season_id = %s
    # """, (season_id,))

    game_ids = updated_game_ids

    total_games = len(game_ids)

    for idx, game_id in enumerate(game_ids, 1):
        if idx % 100 == 0 or idx == 1 or idx == total_games:
            logger.info(f"Processing game {idx} of {total_games} (game_id: {game_id}) for season {season_id}")
        url = url_base + str(game_id) + '/play-by-play'
        #logger.info(url)
        response = requests.get(url)

        if response.text.strip():
            try:
                data = json.loads(response.text)
                #logger.info(data)
                if 'plays' in data:
                    plays_data = []
                    for play in data['plays']:
                        # Extract play data
                        periodDescriptor = play.get('periodDescriptor', {})
                        details = play.get('details', {})
                        #logger.info(play['typeDescKey'])
                        #logger.info(details)
                        play_data = (
                            play.get('eventId', None),
                            game_id,
                            periodDescriptor.get('number', None),
                            periodDescriptor.get('periodType', None),
                            play.get('timeInPeriod', None),
                            play.get('timeRemaining', None),
                            play.get('situationCode', None),
                            play.get('homeTeamDefendingSide', None),
                            play.get('typeCode', None),
                            play.get('typeDescKey', None),
                            play.get('sortOrder', None),
                            details.get('xCoord', None),
                            details.get('yCoord', None),
                            details.get('zoneCode', None),
                            details.get('shotType', None),
                            details.get('blockingPlayerId', None),
                            details.get('shootingPlayerId', None),
                            details.get('goalieInNetId', None),
                            details.get('playerId', None),
                            details.get('scoringPlayerId', None),
                            details.get('assist1PlayerId', None),
                            details.get('assist2PlayerId', None),
                            details.get('eventOwnerTeamId', None),
                            details.get('awaySOG', None),
                            details.get('homeSOG', None),
                            details.get('hittingPlayerId', None),
                            details.get('hitteePlayerId', None),
                            details.get('reason', None),
                            details.get('secondaryReason', None),
                            details.get('losingPlayerId', None),
                            details.get('winningPlayerId', None),
                            details.get('highlightClipSharingUrl', None),
                            details.get('duration', None),

                            details.get('servedByPlayerId', None),
                            details.get('drawnByPlayerId', None),
                            details.get('committedByPlayerId', None)
                        )
                        plays_data.append(play_data)


                    # Insert play data into the database
                    cursor.executemany("""
                    INSERT IGNORE INTO events (
                        event_id, game_id, period_number, period_type, time_in_period, time_remaining, situation_code,
                        home_team_defending_side, type_code, type_desc_key, sort_order, x_coord, y_coord, zone_code,
                        shot_type, blocking_player_id, shooting_player_id, goalie_in_net_id, player_id, scoring_player_id, assist1_player_id, assist2_player_id, event_owner_team_id,
                        away_sog, home_sog, hitting_player_id, hittee_player_id, reason, secondary_reason,
                        losing_player_id, winning_player_id, highlight_clip_sharing_url, duration, served_by_player_id, drawn_by_player_id, committed_by_player_id
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, plays_data)
                    connection.commit()
            except json.JSONDecodeError:
                pass

def update_roster_players_table(season_id):
    #already have the season from the loop
    #already have list of team_ids but want only the current season's teams

    cursor.execute("""
        SELECT DISTINCT team_id
                   FROM seasons_end_standings
                   WHERE games_played > 0
                   AND season_id = %s;
    """, (season_id,))

    teams = cursor.fetchall()

    for team in teams:
        team_id = team[0]
        team_abv = teams_dict['team_abbreviation'][teams_dict['team_id'].index(team_id)]

        logger.info(f"Season ID: {season_id}, Team ID: {team_abv}")

        url = f"https://api-web.nhle.com/v1/roster/{team_abv}/{season_id}"
        #logger.info(url)
        response = requests.get(url)

        if response.text.strip():
            try:
                data = json.loads(response.text)
                #logger.info(data)
                roster_data = []
                if 'forwards' in data:
                    for player in data['forwards']:
                        # Extract player data
                        player_data = (
                            player.get('id', None),
                            team_id,
                            season_id,
                            player.get('firstName', {}).get('default', None),
                            player.get('lastName', {}).get('default', None),
                            player.get('sweaterNumber', None),
                            player.get('positionCode', None),
                            player.get('shootsCatches', None),
                            player.get('heightInInches', None),
                            player.get('weightInPounds', None),
                            player.get('heightInCentimeters', None),
                            player.get('weightInKilograms', None),
                            player.get('birthDate', None),
                            player.get('birthCity', {}).get('default', None),
                            player.get('birthCountry', None)
                        )
                        roster_data.append(player_data)

                if 'defensemen' in data:
                    for player in data['defensemen']:
                        # Extract player data
                        player_data = (
                            player.get('id', None),
                            team_id,
                            season_id,
                            player.get('firstName', {}).get('default', None),
                            player.get('lastName', {}).get('default', None),
                            player.get('sweaterNumber', None),
                            player.get('positionCode', None),
                            player.get('shootsCatches', None),
                            player.get('heightInInches', None),
                            player.get('weightInPounds', None),
                            player.get('heightInCentimeters', None),
                            player.get('weightInKilograms', None),
                            player.get('birthDate', None),
                            player.get('birthCity', {}).get('default', None),
                            player.get('birthCountry', None)
                        )
                        roster_data.append(player_data)

                if 'goalies' in data:
                    for player in data['goalies']:
                        # Extract player data
                        player_data = (
                            player.get('id', None),
                            team_id,
                            season_id,
                            player.get('firstName', {}).get('default', None),
                            player.get('lastName', {}).get('default', None),
                            player.get('sweaterNumber', None),
                            player.get('positionCode', None),
                            player.get('shootsCatches', None),
                            player.get('heightInInches', None),
                            player.get('weightInPounds', None),
                            player.get('heightInCentimeters', None),
                            player.get('weightInKilograms', None),
                            player.get('birthDate', None),
                            player.get('birthCity', {}).get('default', None),
                            player.get('birthCountry', None)
                        )
                        roster_data.append(player_data)

                #logger.info(roster_data)

                # Insert roster data into the database
                cursor.executemany("""
                INSERT IGNORE INTO roster_players (
                    player_id, team_id, season_id, firstName, lastName, sweaterNumber, positionCode,
                    shootsCatches, heightInInches, weightInPounds, heightInCentimeters,
                    weightInKilograms, birthDate, birthCity, birthCountry
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, roster_data)
                connection.commit()

                #logger.info("Rows inserted:", cursor.rowcount)

            except json.JSONDecodeError:
                logger.info(f"JSONDecodeError for season ID {season_id} and team ID {team_abv}.")
                pass

def update_players_season_table(season_id):
    url_base = "https://api.nhle.com/stats/rest/en/skater/summary?cayenneExp=playerId="#8477492

    cursor.execute("""
    SELECT DISTINCT player_id
                FROM roster_players
                WHERE season_id = %s;
    """, (season_id,))
    player_ids = cursor.fetchall()

    for p_id in player_ids:
        player_id = p_id[0]
        url = url_base + str(player_id)
        response = requests.get(url)
        logger.info(player_id)
        if response.text.strip():
            try:
                data = json.loads(response.text)
                if data['data']:
                    for season in data['data']:
                        # Extract player data

                        player = (
                            season.get('assists', None),
                            season.get('evGoals', None),
                            season.get('evPoints', None),
                            season.get('faceoffWinPct', None),
                            season.get('gameWinningGoals', None),
                            season.get('gamesPlayed', None),
                            season.get('goals', None),
                            season.get('lastName', None),
                            season.get('otGoals', None),
                            season.get('penaltyMinutes', None),
                            player_id,
                            season.get('plusMinus', None),
                            season.get('points', None),
                            season.get('pointsPerGame', None),
                            season.get('positionCode', None),
                            season.get('ppGoals', None),
                            season.get('ppPoints', None),
                            season.get('seasonId', None),
                            season.get('shGoals', None),
                            season.get('shPoints', None),
                            season.get('shootingPct', None),
                            season.get('shootsCatches', None),
                            season.get('shots', None),
                            season.get('skaterFullName', None),
                            season.get('teamAbbrevs', None),
                            season.get('timeOnIcePerGame', None)

                        )

                        cursor.execute("""
                            INSERT IGNORE INTO players_season (
                                assists, ev_goals, ev_points, faceoff_win_pct, game_winning_goals, games_played, goals, last_name, ot_goals, penalty_minutes, player_id, plus_minus, points, points_per_game, position_code, pp_goals, pp_points, season_id, sh_goals, sh_points, shooting_percentage, shoots_catches, shots, skater_full_name, team_abbreviations, time_on_ice_per_game
                            )  
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, player)
                        connection.commit()

                        #logger.info("Rows inserted:", cursor.rowcount)

            except json.JSONDecodeError:
                pass

def update_players_table(season_id):
    cursor.execute("""
        SELECT player_id, MAX(skater_full_name), MAX(position_code), MAX(shoots_catches), SUM(assists),
            SUM(goals), SUM(points), SUM(penalty_minutes), SUM(games_played), SUM(plus_minus)
            FROM players_season
            WHERE season_id = %s
            GROUP BY player_id;
        """, (season_id,))

    players = cursor.fetchall()

    for player in players:
        logger.info(player[0])
        player_id = player[0]
        skater_full_name = player[1]
        position_code = player[2]
        shoots_catches = player[3]
        total_assists = player[4]
        total_goals = player[5]
        total_points = player[6]
        total_penalty_minutes = player[7]
        total_games_played = player[8]
        total_plus_minus = player[9]

        # Get birth date, city, and country from roster_players table
        cursor.execute("""
            SELECT birthDate, birthCity, birthCountry
            FROM roster_players
            WHERE player_id = %s
            LIMIT 1;
        """, (player_id,))
        
        birth_data = cursor.fetchone()
        
        if birth_data:
            birth_date, birth_city, birth_country = birth_data
        else:
            birth_date, birth_city, birth_country = None, None, None

        # Insert or update the player in the players table
        cursor.execute("""
            INSERT IGNORE INTO players (player_id, skaterFullName, birth_date, birth_city, birth_country,
                positionCode, shootsCatches, games_played, total_assists, total_goals,
                total_points, total_penalty_minutes, total_games_played, total_plus_minus)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (player_id, skater_full_name, birth_date, birth_city, birth_country, position_code, shoots_catches, total_games_played, total_assists, total_goals, total_points, total_penalty_minutes, total_games_played, total_plus_minus))
        connection.commit()
        #logger.info("Rows inserted:", cursor.rowcount)    


def main():
    try:
        logger.info("=== Starting NHL Data Update ===")
        
        last_game_id, last_season_id, last_date = get_last_date_updated_db()
        current_season = get_current_season()

        logger.info(f"Last game ID: {last_game_id}")
        logger.info(f"Last season in DB: {last_season_id}")
        logger.info(f"Last date: {last_date}")
        logger.info(f"Current season: {current_season}")

        #last_season_int = 2024# = int(str(last_season_in_db)[:4]) not used because up to date
        current_season_int = int(str(current_season)[:4])

        #do everything for each season
        for year in range(current_season_int, current_season_int + 1):
            season_id = int(str(year) + str(year + 1))

            logger.info("update seasons table")
            #update_seasons_table(season_id)

            logger.info("update games table")
            updated_game_ids = update_games_table(year)

            #seasons_end_standings --------------------------------------------------

            logger.info("update seasons end standings table")
            update_seasons_end_standings(season_id)#could use updated_game_ids but it's a lot of extra logic for not much more efficiency

            #event table --------------------------------------------

            logger.info("update events table")
            update_events_table(season_id, updated_game_ids)#updated_game_ids limits repeating events and saves a lot of time

            #roster_players table --------------------------------------------------

            logger.info("update roster players table")
            update_roster_players_table(season_id)

            #players_season table --------------------------------------------------

            logger.info("update players season table")
            update_players_season_table(season_id)

            #players table --------------------------------------------------

            logger.info("update players table")
            update_players_table(season_id)#maybe keep track of updated players to limit updates?

    except Exception as e:
        logger.error(f"=== ERROR: NHL Data Update Failed ===")
        logger.error(f"Error: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        sys.exit(1)  # Exit with error code for cron monitoring
    
    finally:
        # Close database connection
        if 'connection' in globals() and connection.is_connected():
            cursor.close()
            connection.close()
            logger.info("Database connection closed")


if __name__ == '__main__':
    main()