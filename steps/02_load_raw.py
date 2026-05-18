import time
from datetime import datetime, timezone

import pandas as pd
from nba_api.stats.static import teams, players
from nba_api.stats.endpoints import leaguegamefinder, boxscoretraditionalv2, leaguedashplayerstats
from snowflake.snowpark import Session

SEASON = "2015-16"


# Metadata helper function. Adds endpoint, and current time to current table
def add_load_metadata(df: pd.DataFrame, source_endpoint: str) -> pd.DataFrame:
    df = df.copy()
    df["SOURCE_ENDPOINT"] = source_endpoint
    df["EXTRACTED_AT"] = datetime.now(timezone.utc)
    return df


# Write df table to snowflake
def write_raw_table(session: Session, df: pd.DataFrame, table_name: str):
    session.use_schema("RAW_NBA")

    session.write_pandas(
        df=df,
        table_name=table_name,
        auto_create_table=True,
        overwrite=True,
    )


# get teams with `teams.get_teams()`
# add the three metadata values
# write to snowflake
def load_teams(session: Session):
    df = pd.DataFrame(teams.get_teams())
    df = add_load_metadata(df, source_endpoint="nba_api.stats.static.teams")
    write_raw_table(session, df, "TEAMS")


# Gets every player ever. Just like a static directory
def load_players(session: Session):
    df = pd.DataFrame(players.get_players())
    df = add_load_metadata(df, source_endpoint="nba_api.stats.static.players")
    write_raw_table(session, df, "PLAYERS")


# Get every game for a specific season
def load_games(session: Session):
    endpoint = leaguegamefinder.LeagueGameFinder(
        season_nullable=SEASON,
        league_id_nullable="00",
    )

    df = endpoint.get_data_frames()[0]
    df = add_load_metadata(df, "LeagueGameFinder")
    write_raw_table(session, df, "GAMES")


# box score for each game stored in RAW_NBA.GAMES
# gets each game_id from games, then calls boxScoreTradV2 endpoint for each game
# appends that box score to all_player_stats, write all_player_stats to PLAYER_BOX_SCORES table
def load_player_box_scores(session: Session):
    games_df = session.table("RAW_NBA.GAMES").to_pandas()
    game_ids = games_df["GAME_ID"].drop_duplicates().tolist()

    all_player_stats = []

    for game_id in game_ids:
        print(f"Loading box score for game {game_id}")

        endpoint = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id=game_id)
        player_stats = endpoint.get_data_frames()[0]

        player_stats["GAME_ID"] = game_id
        all_player_stats.append(player_stats)

        time.sleep(0.6)

    df = pd.concat(all_player_stats, ignore_index=True)
    df = add_load_metadata(df, "BoxScoreTraditionalV2")
    write_raw_table(session, df, "PLAYER_BOX_SCORES")


# Gets player stats for the defined season.
def load_player_season_stats(session: Session):
    endpoint = leaguedashplayerstats.LeagueDashPlayerStats(
        season=SEASON,
        season_type_all_star="Regular Season",
        per_mode_detailed="PerGame",
        league_id_nullable="00",
    )

    df = endpoint.get_data_frames()[0]
    df = add_load_metadata(df, "LeagueDashPlayerStats")
    write_raw_table(session, df, "PLAYER_SEASON_STATS")


def load_all_raw_tables(session: Session):
    session.use_database("NBA_DB")
    session.use_warehouse("NBA_WH")
    session.use_schema("RAW_NBA")

    load_teams(session)
    load_players(session)
    load_games(session)
    load_player_box_scores(session)
    load_player_season_stats(session)


if __name__ == "__main__":
    with Session.builder.getOrCreate() as session:
        load_all_raw_tables(session)
