from snowflake.snowpark import Session
import snowflake.snowpark.functions as F

# Harmonised view of player season stats (RAW table has 68 columns)
def create_player_season_stats_view(session: Session):
    session.use_schema("HARMONIZED")

    player_stats = session.table("RAW_NBA.PLAYER_SEASON_STATS").select(
        F.col("PLAYER_ID"),
        F.col("PLAYER_NAME"),
        F.col("NICKNAME"),
        F.col("TEAM_ID"),
        F.col("TEAM_ABBREVIATION"),
        F.col("AGE"),
        F.col("GP"),
        F.col("W"),
        F.col("L"),
        F.col("W_PCT"),
        F.col("MIN"),
        F.col("FGM"),
        F.col("FGA"),
        F.col("FG_PCT"),
        F.col("FG3M"),
        F.col("FG3A"),
        F.col("FG3_PCT"),
        F.col("FTM"),
        F.col("FTA"),
        F.col("FT_PCT"),
        F.col("OREB"),
        F.col("DREB"),
        F.col("REB"),
        F.col("AST"),
        F.col("TOV"),
        F.col("STL"),
        F.col("BLK"),
        F.col("BLKA"),
        F.col("PF"),
        F.col("PFD"),
        F.col("PTS"),
        F.col("PLUS_MINUS"),
        F.col("NBA_FANTASY_PTS"),
        F.col("DD2"),
        F.col("TD3"),
        F.col("SOURCE_ENDPOINT"),
        F.col("EXTRACTED_AT"),
    ).distinct()

    player_stats.create_or_replace_view("PLAYER_SEASON_STATS_V")


# Grab offensive-only stats from harmonised view and add to new view
def create_player_season_offense_view(session: Session):
    session.use_schema("HARMONIZED")

    offensive_player_stats = session.table("HARMONIZED.PLAYER_SEASON_STATS_V").select(
        F.col("PLAYER_ID"),
        F.col("PLAYER_NAME"),
        F.col("TEAM_ID"),
        F.col("TEAM_ABBREVIATION"),
        F.col("AGE"),
        F.col("GP"),
        F.col("MIN"),
        F.col("PTS"),
        F.col("REB"),
        F.col("AST"),
        F.col("FGM"),
        F.col("FGA"),
        F.col("FG_PCT"),
        F.col("FG3M"),
        F.col("FG3A"),
        F.col("FG3_PCT"),
        F.col("FTM"),
        F.col("FTA"),
        F.col("FT_PCT"),
        F.col("TOV"),
        F.col("PLUS_MINUS"),
        F.col("W"),
        F.col("L"),
    )

    offensive_player_stats.create_or_replace_view("PLAYER_SEASON_OFFENSE_V")

# Grab defensive-only stats from harmonised view and add to new view
def create_player_season_defense_view(session: Session):
    session.use_schema("HARMONIZED")

    defensive_player_stats = session.table("HARMONIZED.PLAYER_SEASON_STATS_V").select(
        F.col("PLAYER_ID"),
        F.col("PLAYER_NAME"),
        F.col("TEAM_ID"),
        F.col("TEAM_ABBREVIATION"),
        F.col("AGE"),
        F.col("GP"),
        F.col("MIN"),
        F.col("DREB"),
        F.col("OREB"),
        F.col("REB"),
        F.col("STL"),
        F.col("BLK"),
        F.col("BLKA"),
        F.col("PF"),
        F.col("PFD"),
        F.col("PLUS_MINUS"),
    )

    defensive_player_stats.create_or_replace_view("PLAYER_SEASON_DEFENSE_V")

# Create game-level player stats view by joining box scores to games, players, and teams
def create_game_player_stats_view(session: Session):
    session.use_schema("HARMONIZED")

    box_scores = session.table("RAW_NBA.PLAYER_BOX_SCORES").select(
        F.col("GAME_ID"),
        F.col("TEAM_ID"),
        F.col("TEAM_ABBREVIATION"),
        F.col("PLAYER_ID"),
        F.col("PLAYER_NAME"),
        F.col("START_POSITION"),
        F.col("COMMENT"),
        F.col("MIN"),
        F.col("FGM"),
        F.col("FGA"),
        F.col("FG_PCT"),
        F.col("FG3M"),
        F.col("FG3A"),
        F.col("FG3_PCT"),
        F.col("FTM"),
        F.col("FTA"),
        F.col("FT_PCT"),
        F.col("OREB"),
        F.col("DREB"),
        F.col("REB"),
        F.col("AST"),
        F.col("STL"),
        F.col("BLK"),
        F.col("TO").alias("TOV"),
        F.col("PF"),
        F.col("PTS"),
        F.col("PLUS_MINUS"),
    )

    games = session.table("RAW_NBA.GAMES").select(
        F.col("GAME_ID"),
        F.col("TEAM_ID").alias("GAME_TEAM_ID"),
        F.to_date(F.col("GAME_DATE")).alias("GAME_DATE"),
        F.col("MATCHUP"),
        F.col("WL"),
    )

    players = session.table("RAW_NBA.PLAYERS").select(
        F.col("ID").alias("PLAYER_DIM_ID"),
        F.col("FULL_NAME"),
        F.col("FIRST_NAME"),
        F.col("LAST_NAME"),
        F.col("IS_ACTIVE"),
    )

    teams = session.table("RAW_NBA.TEAMS").select(
        F.col("ID").alias("TEAM_DIM_ID"),
        F.col("FULL_NAME").alias("TEAM_FULL_NAME"),
        F.col("ABBREVIATION").alias("TEAM_ABBREVIATION_DIM"),
        F.col("NICKNAME").alias("TEAM_NICKNAME"),
        F.col("CITY").alias("TEAM_CITY"),
        F.col("STATE").alias("TEAM_STATE"),
    )

    final_df = (
        box_scores
        .join(
            games,
            (box_scores["GAME_ID"] == games["GAME_ID"])
            & (box_scores["TEAM_ID"] == games["GAME_TEAM_ID"]),
            rsuffix="_G",
        )
        .join(players, box_scores["PLAYER_ID"] == players["PLAYER_DIM_ID"], rsuffix="_P")
        .join(teams, box_scores["TEAM_ID"] == teams["TEAM_DIM_ID"], rsuffix="_T")
        .select(
            box_scores["GAME_ID"],
            games["GAME_DATE"],
            games["MATCHUP"],
            games["WL"],
            box_scores["TEAM_ID"],
            teams["TEAM_FULL_NAME"],
            teams["TEAM_CITY"],
            teams["TEAM_NICKNAME"],
            box_scores["TEAM_ABBREVIATION"],
            box_scores["PLAYER_ID"],
            players["FULL_NAME"],
            players["FIRST_NAME"],
            players["LAST_NAME"],
            players["IS_ACTIVE"],
            box_scores["PLAYER_NAME"],
            box_scores["START_POSITION"],
            box_scores["COMMENT"],
            box_scores["MIN"],
            box_scores["FGM"],
            box_scores["FGA"],
            box_scores["FG_PCT"],
            box_scores["FG3M"],
            box_scores["FG3A"],
            box_scores["FG3_PCT"],
            box_scores["FTM"],
            box_scores["FTA"],
            box_scores["FT_PCT"],
            box_scores["OREB"],
            box_scores["DREB"],
            box_scores["REB"],
            box_scores["AST"],
            box_scores["STL"],
            box_scores["BLK"],
            box_scores["TOV"],
            box_scores["PF"],
            box_scores["PTS"],
            box_scores["PLUS_MINUS"],
        )
    )

    final_df.create_or_replace_view("GAME_PLAYER_STATS_V")

def create_all_views(session: Session):
    session.use_database("NBA_DB")
    session.use_warehouse("NBA_WH")

    create_player_season_stats_view(session)
    create_player_season_offense_view(session)
    create_player_season_defense_view(session)
    create_game_player_stats_view(session)


if __name__ == "__main__":
    with Session.builder.getOrCreate() as session:
        create_all_views(session)