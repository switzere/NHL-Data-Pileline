import sqlite3
import requests
from datetime import datetime, timedelta
import mysql.connector
from config import db_config
import pandas as pd
import json
import copy
import os

teams_dict = {
    'team_id': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 46, 47, 48, 49, 52, 53, 54, 55, 56],
    'team_name': ['New Jersey Devils', 'New York Islanders', 'New York Rangers', 'Philadelphia Flyers', 'Pittsburgh Penguins', 'Boston Bruins', 'Buffalo Sabres', 'Montreal Canadiens', 'Ottawa Senators', 'Toronto Maple Leafs', 'Atlanta Thrashers', 'Carolina Hurricanes', 'Florida Panthers', 'Tampa Bay Lightning', 'Washington Capitals', 'Chicago Blackhawks', 'Detroit Red Wings', 'Nashville Predators', 'St Louis Blues', 'Calgary Flames', 'Colorado Avalanche', 'Edmonton Oilers', 'Vancouver Canucks', 'Anaheim Ducks', 'Dallas Stars', 'Los Angeles Kings', 'Pheonix Coyotes', 'San Jose Sharks', 'Columbus Blue Jackets', 'Minnesota Wild', 'Minnesota North Stars', 'Quebec Nordique', 'Winnipeg Jets (1979)', 'Hartford Whalers', 'Colorado Rockies', 'Oakland Seals', 'Atlanta Flames', 'Kansas City Scouts', 'Cleveland Barons', 'Winnipeg Jets', 'Arizona Coyotes', 'Vegas Golden Knights', 'Seattle Kraken', 'California Golden Seals'],
    'team_abbreviation': ['NJD', 'NYI', 'NYR', 'PHI', 'PIT', 'BOS', 'BUF', 'MTL', 'OTT', 'TOR', 'ATL', 'CAR', 'FLA', 'TBL', 'WSH', 'CHI', 'DET', 'NSH', 'STL', 'CGY', 'COL', 'EDM', 'VAN', 'ANA', 'DAL', 'LAK', 'PHX', 'SJS', 'CBJ', 'MIN', 'MNS', 'QUE', 'WIN', 'HFD', 'CLR', 'OAK', 'AFM', 'KCS', 'CLE', 'WPG', 'ARI', 'VGK', 'SEA', 'CGS']
}

# Establish connection
connection = mysql.connector.connect(
    host=db_config["host"],
    user=db_config["user"],
    password=db_config["password"],
    database=db_config["database"]
)
cursor = connection.cursor()



def get_most_recent_season_from_db():
    cursor.execute("SELECT MAX(season_id) FROM games")
    result = cursor.fetchone()
    return result[0] if result else None

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

    #for each team get the games for this season
    for abv in teams_dict['team_abbreviation']:
        print(f"Processing games for team: {abv} in season starting {year}")
        start_date = f"{year}-10-01"
        end_date = f"{year + 1}-04-30"
        print(f"Fetching games from {start_date} to {end_date} for team {abv}")


        url = url_base + abv + '/' + str(year) + str(year + 1)

        response = requests.get(url)
        print(year)
        if response.text.strip():
            try:
                data = json.loads(response.text)
                if data['games']:
                    for g in data['games']:
                        #print(g)
                        game = (
                            g['id'],
                            g.get('season', ''),
                            g.get('gameType', -1),
                            g.get('gameDate', None),
                            g.get('homeTeam', {}).get('id', -1),
                            g.get('awayTeam', {}).get('id', -1),
                            g.get('homeTeam', {}).get('score', -1),
                            g.get('awayTeam', {}).get('score', -1),

                            g.get('gameOutcome', {}).get('lastPeriodType', ''),  # Use get method with default value ''
                            g.get('winningGoalie', {}).get('playerId', -1),  # Use get method with default value -1
                            g.get('winningGoalScorer', {}).get('playerId', -1),  # Use get method with default value -1
                            g.get('seriesStatus', {}).get('round', -1),  # Use get method with default value ''
                        )
                        #if homeTeamId or awayTeamId is not in teams_dict, skip it
                        if game[4] in teams_dict['team_id'] and game[5] in teams_dict['team_id']:
                            games.append(game)
            except json.JSONDecodeError:
                pass

    cursor.executemany("""
    INSERT IGNORE INTO games (game_id, season, game_type, date, home_team_id, away_team_id, home_score, away_score, game_outcome, winning_goalie_id, winning_goal_scorer_id, series_status_round)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, games)
    connection.commit()

def update_seasons_end_standings(season_id):
    #get info for the season for each team

    # Get data from games table

    cursor.execute("""
    SELECT season_id, home_team_id, away_team_id, home_score, away_score, game_outcome, game_type
    FROM games
    WHERE season_id = %s
    """, (season_id,))

    games_data = cursor.fetchall()

    for team_id in teams_dict['team_id']:
        print(team_id)
        seasons_end_standings_data = []

        #print(season_id)

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

        for game in games_data:
            if game[0] == season_id:
                if game[1] == team_id:
                    print(game)
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
                    #print(game)
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
        #print(f"Season_id: {season_id}, Team_id: {team_id}, Wins: {wins}, Losses: {losses}, OT_Losses: {ot_losses}, Points: {points}, Goals_For: {goals_for}, Goals_Against: {goals_against}")
        seasons_end_standings_data.append((season_id, team_id, wins, losses, ot_losses, points, games_played, goals_for, goals_against, preseason_wins, preseason_losses, preseason_ot_losses, preseason_goals_for, preseason_goals_against, preseason_games_played, playoff_wins, playoff_losses, playoff_ot_losses, playoff_goals_for, playoff_goals_against, playoff_games_played))


        print(f"Standings: {seasons_end_standings_data}")
        # Populate seasons_end_standings table
        cursor.executemany("""
        INSERT IGNORE INTO seasons_end_standings (season_id, team_id, wins, losses, ot_losses, points, games_played, goals_for, goals_against, preseason_wins, preseason_losses, preseason_ot_losses, preseason_goals_for, preseason_goals_against, preseason_games_played, playoff_wins, playoff_losses, playoff_ot_losses, playoff_goals_for, playoff_goals_against, playoff_games_played)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, seasons_end_standings_data)

        connection.commit()

def update_events_table(season_id):
    #grab each game_id from the games table

    cursor.execute("""
        SELECT DISTINCT game_id
        FROM games
        WHERE season_id = %s
    """, (season_id,))

    game_ids = cursor.fetchall()

    for id in game_ids:

        url = url_base + str(id[0]) + '/play-by-play'
        #print(url)
        response = requests.get(url)

        if response.text.strip():
            try:
                data = json.loads(response.text)
                #print(data)
                if 'plays' in data:
                    plays_data = []
                    for play in data['plays']:
                        # Extract play data
                        periodDescriptor = play.get('periodDescriptor', {})
                        details = play.get('details', {})
                        #print(play['typeDescKey'])
                        #print(details)
                        play_data = (
                            play.get('eventId', -1),
                            id[0],
                            periodDescriptor.get('number', -1),
                            periodDescriptor.get('periodType', ''),
                            play.get('timeInPeriod', ''),
                            play.get('timeRemaining', ''),
                            play.get('situationCode', -1),
                            play.get('homeTeamDefendingSide', ''),
                            play.get('typeCode', -1),
                            play.get('typeDescKey', ''),
                            play.get('sortOrder', -1),
                            details.get('xCoord', -1),
                            details.get('yCoord', -1),
                            details.get('zoneCode', ''),
                            details.get('shotType', ''),
                            details.get('blockingPlayerId', -1),
                            details.get('shootingPlayerId', -1),
                            details.get('goalieInNetId', -1),
                            details.get('playerId', -1),
                            details.get('eventOwnerTeamId', -1),
                            details.get('awaySOG', -1),
                            details.get('homeSOG', -1),
                            details.get('hittingPlayerId', -1),
                            details.get('hitteePlayerId', -1),
                            details.get('reason', ''),
                            details.get('secondaryReason', ''),
                            details.get('losingPlayerId', -1),
                            details.get('winningPlayerId', -1)
                        )
                        plays_data.append(play_data)


                    # Insert play data into the database
                    cursor.executemany("""
                    INSERT IGNORE INTO events (
                        event_id, game_id, period_number, period_type, time_in_period, time_remaining, situation_code,
                        home_team_defending_side, type_code, type_desc_key, sort_order, x_coord, y_coord, zone_code,
                        shot_type, blocking_player_id, shooting_player_id, goalie_in_net_id, player_id, event_owner_team_id,
                        away_sog, home_sog, hitting_player_id, hittee_player_id, reason, secondary_reason,
                        losing_player_id, winning_player_id
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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

        print(f"Season ID: {season_id}, Team ID: {team_abv}")

        url = f"https://api-web.nhle.com/v1/roster/{team_abv}/{season_id}"
        #print(url)
        response = requests.get(url)

        if response.text.strip():
            try:
                data = json.loads(response.text)
                #print(data)
                roster_data = []
                if 'forwards' in data:
                    for player in data['forwards']:
                        # Extract player data
                        player_data = (
                            player.get('id', -1),
                            team_id,
                            season_id,
                            player.get('firstName', {}).get('default', ''),
                            player.get('lastName', {}).get('default', ''),
                            player.get('sweaterNumber', -1),
                            player.get('positionCode', ''),
                            player.get('shootsCatches', ''),
                            player.get('heightInInches', -1),
                            player.get('weightInPounds', -1),
                            player.get('heightInCentimeters', -1),
                            player.get('weightInKilograms', -1),
                            player.get('birthDate', ''),
                            player.get('birthCity', {}).get('default', ''),
                            player.get('birthCountry', '')
                        )
                        roster_data.append(player_data)

                if 'defensemen' in data:
                    for player in data['defensemen']:
                        # Extract player data
                        player_data = (
                            player.get('id', -1),
                            team_id,
                            season_id,
                            player.get('firstName', {}).get('default', ''),
                            player.get('lastName', {}).get('default', ''),
                            player.get('sweaterNumber', -1),
                            player.get('positionCode', ''),
                            player.get('shootsCatches', ''),
                            player.get('heightInInches', -1),
                            player.get('weightInPounds', -1),
                            player.get('heightInCentimeters', -1),
                            player.get('weightInKilograms', -1),
                            player.get('birthDate', ''),
                            player.get('birthCity', {}).get('default', ''),
                            player.get('birthCountry', '')
                        )
                        roster_data.append(player_data)

                if 'goalies' in data:
                    for player in data['goalies']:
                        # Extract player data
                        player_data = (
                            player.get('id', -1),
                            team_id,
                            season_id,
                            player.get('firstName', {}).get('default', ''),
                            player.get('lastName', {}).get('default', ''),
                            player.get('sweaterNumber', -1),
                            player.get('positionCode', ''),
                            player.get('shootsCatches', ''),
                            player.get('heightInInches', -1),
                            player.get('weightInPounds', -1),
                            player.get('heightInCentimeters', -1),
                            player.get('weightInKilograms', -1),
                            player.get('birthDate', ''),
                            player.get('birthCity', {}).get('default', ''),
                            player.get('birthCountry', '')
                        )
                        roster_data.append(player_data)

                #print(roster_data)

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

                #print("Rows inserted:", cursor.rowcount)

            except json.JSONDecodeError:
                print(f"JSONDecodeError for season ID {season_id} and team ID {team_abv}.")
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
        print(player_id)
        if response.text.strip():
            try:
                data = json.loads(response.text)
                if data['data']:
                    for season in data['data']:
                        # Extract player data

                        player = (
                            season.get('assists', -1),
                            season.get('evGoals', -1),
                            season.get('evPoints', -1),
                            season.get('faceoffWinPct', -1),
                            season.get('gameWinningGoals', -1),
                            season.get('gamesPlayed', -1),
                            season.get('goals', -1),
                            season.get('lastName', ''),
                            season.get('otGoals', -1),
                            season.get('penaltyMinutes', -1),
                            player_id,
                            season.get('plusMinus', -1),
                            season.get('points', -1),
                            season.get('pointsPerGame', -1),
                            season.get('positionCode', ''),
                            season.get('ppGoals', -1),
                            season.get('ppPoints', -1),
                            season.get('seasonId', -1),
                            season.get('shGoals', -1),
                            season.get('shPoints', -1),
                            season.get('shootingPct', -1),
                            season.get('shootsCatches', ''),
                            season.get('shots', -1),
                            season.get('skaterFullName', ''),
                            season.get('teamAbbrevs', ''),
                            season.get('timeOnIcePerGame', -1)

                        )

                        cursor.execute("""
                            INSERT IGNORE INTO players_season (
                                assists, ev_goals, ev_points, faceoff_win_pct, game_winning_goals, games_played, goals, last_name, ot_goals, penalty_minutes, player_id, plus_minus, points, points_per_game, position_code, pp_goals, pp_points, season_id, sh_goals, sh_points, shooting_percentage, shoots_catches, shots, skater_full_name, team_abbreviations, time_on_ice_per_game
                            )  
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, player)
                        connection.commit()

                        #print("Rows inserted:", cursor.rowcount)

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
        print(player[0])
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
        #print("Rows inserted:", cursor.rowcount)    


def main():

    last_season_in_db = get_most_recent_season_from_db()
    current_season = get_current_season()

    print(f"Last season in DB: {last_season_in_db}")
    print(f"Current season: {current_season}")

    last_season_int = int(str(last_season_in_db)[:4])
    current_season_int = int(str(current_season)[:4])

    #do everything for each season
    for year in range(last_season_int, current_season_int + 1):
        season_id = int(str(year) + str(year + 1))

        update_seasons_table(season_id)


        update_games_table(year)

        #seasons_end_standings --------------------------------------------------

        update_seasons_end_standings(season_id)

        #event table --------------------------------------------

        update_events_table(season_id)

        #roster_players table --------------------------------------------------

        update_roster_players_table(season_id)

        
        #players_season table --------------------------------------------------

        update_players_season_table(season_id)

        #players table --------------------------------------------------

        update_players_table(season_id)


if __name__ == '__main__':
    main()