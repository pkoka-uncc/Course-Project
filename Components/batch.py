#Imports
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
 
#Build Spark Session
def build_spark():
    return (
        SparkSession.builder.appName("NBA_Batch_Analysis").config("spark.sql.shuffle.partitions", "8").getOrCreate()
    )
 
#Load Tables
def load_tables(spark: SparkSession, data_dir:str):
    path = data_dir.rstrip("/")
    games = spark.read.csv(f"{path}/games.csv", header=True, inferSchema=True)
    teams = spark.read.csv(f"{path}/teams.csv", header=True, inferSchema=True)
    ranking = spark.read.csv(f"{path}/ranking.csv", header=True, inferSchema=True)
    return games, teams, ranking                
 
#Query 1: Team performance trend (last 5 seasons)
def q1_team_trend(games, teams):
 
    print("\n" + "=" * 60)
    print("Q1 - Strongest team performance (last 5 seasons)")
 
    #Get the most recent season number and subtract 4 to get a 5-season window
    last5 = games.filter(F.col("SEASON") >= F.lit(games.agg(F.max("SEASON")).collect()[0][0] - 4))
 
    #Each game has a home team and an away team. Split each game into 2 rows, one for home, and one for away. Every team appears once.
    #Used inversion for away team wins since there is no away_team_wins column.
    home = last5.select(
        F.col("SEASON"),                                        
        F.col("HOME_TEAM_ID").alias("TEAM_ID"),
        F.col("HOME_TEAM_WINS").alias("WIN"),
        F.col("PTS_home").alias("PTS_FOR"),
        F.col("PTS_away").alias("PTS_AGAINST"),
    )
 
    away = last5.select(
        F.col("SEASON"),
        F.col("VISITOR_TEAM_ID").alias("TEAM_ID"),
        (1 - F.col("HOME_TEAM_WINS")).alias("WIN"),
        F.col("PTS_away").alias("PTS_FOR"),
        F.col("PTS_home").alias("PTS_AGAINST"),
    )
 
    #Stacks home and away team games into a combined dataframe. 
    all_games = home.union(away)
 
    #Season stats for each team using aggregation
    season_stats = (
        all_games.groupBy("TEAM_ID", "SEASON").agg(
            F.count("*").alias("GP"),
            F.sum("WIN").alias("WINS"), 
            F.mean("PTS_FOR").alias("AVG_PTS_FOR"),
            F.mean("PTS_AGAINST").alias("AVG_PTS_AGAINST"),
        )
        .withColumn("WIN_PCT", F.round(F.col("WINS") / F.col("GP"), 3))    
        .withColumn("AVG_MARGIN", F.round(F.col("AVG_PTS_FOR") - F.col("AVG_PTS_AGAINST"), 2))  
    )
 
    #Average win % and margin across all 5 seasons by team
    team_avg = season_stats.groupBy("TEAM_ID").agg(
        F.round(F.mean("WIN_PCT"), 3).alias("AVG_WIN_PCT"),    
        F.round(F.mean("AVG_MARGIN"), 2).alias("AVG_MARGIN"),   
    )
 
    #Lookup TEAM_ID with city nicknames (ex. Golden State Warriors)
    team_names = teams.select(                                  
        F.col("TEAM_ID"),                                      
        F.concat(F.col("CITY"), F.lit(" "), F.col("NICKNAME")).alias("TEAM_NAME"),
    )
 
    #Joins the team names onto team_avg with TEAM_ID as the key
    #Sorts by descending winning percentage and keeps 3 display columns
    result = (
        team_avg.join(team_names, "TEAM_ID")
        .orderBy(F.desc("AVG_WIN_PCT"))
        .select("TEAM_NAME", "AVG_WIN_PCT", "AVG_MARGIN")
    )
 
    #Print top 15 teams
    result.show(15, truncate=False)
 
    #Query 1: Spark SQL version of the same query. Registers dfs as temp SQL tables so we can query them
    season_stats.createOrReplaceTempView("season_stats")
    team_names.createOrReplaceTempView("team_names")
 
    spark = games.sparkSession
    spark.sql("""
        SELECT t.TEAM_NAME,
               ROUND(AVG(s.WIN_PCT), 3)    AS AVG_WIN_PCT,
               ROUND(AVG(s.AVG_MARGIN), 2) AS AVG_MARGIN,
               COUNT(DISTINCT s.SEASON)    AS SEASONS_PLAYED
        FROM   season_stats s
        JOIN   team_names   t ON s.TEAM_ID = t.TEAM_ID
        GROUP  BY t.TEAM_NAME
        ORDER  BY AVG_WIN_PCT DESC
        LIMIT  10
    """).show(truncate=False)
 
 
#Query 2: 3P% vs wins
#Shows whether teams that shoot better from 3 tend to win more
#Groups games into 3P% buckets and measures home win rate for each game
def q2_three_point_vs_wins(games):
 
    print("\n" + "=" * 60)
    print("Q2 — 3-Point % correlation with wins")
    print("=" * 60)
 
    #Drops rows where any of the columns are null.
    clean = games.dropna(subset=["FG3_PCT_home", "FG3_PCT_away", "HOME_TEAM_WINS"])
 
    #Bucket home 3P% into quartiles
    bucketed = clean.withColumn(
        "FG3_BUCKET",
        F.when(F.col("FG3_PCT_home") < 0.30, "< 30%")
         .when(F.col("FG3_PCT_home") < 0.35, "30-35%")
         .when(F.col("FG3_PCT_home") < 0.40, "35-40%")
         .otherwise(">= 40%")
    )
 
    #For each bucket, count games, average home win rate, average home pts.
    #If there is a higher win rate in 40% bucket then 3P% matters.
    bucketed.groupBy("FG3_BUCKET").agg(
        F.count("*").alias("GAMES"),
        F.round(F.mean("HOME_TEAM_WINS"), 3).alias("HOME_WIN_RATE"),    
        F.round(F.mean("PTS_home"), 1).alias("AVG_PTS"),                
    ).orderBy("FG3_BUCKET").show(truncate=False)
 
    # Season trend: avg 3P% across all games
    # Use Spark to show how there has been a shift in the league towards 3 point shooting after 2015.
    print("Season 3P% trend")
    spark = games.sparkSession
    clean.createOrReplaceTempView("games_clean")
    spark.sql("""
        SELECT SEASON,
               ROUND(AVG(FG3_PCT_home), 3) AS AVG_3PCT_HOME,   
               ROUND(AVG(FG3_PCT_away), 3) AS AVG_3PCT_AWAY    
        FROM   games_clean
        GROUP  BY SEASON
        ORDER  BY SEASON
    """).show(30, truncate=False)
 
#Query 3: point differential
#Measures how much a team wins or loses by
def q3_point_differential(games):
    print("\n" + "=" * 60)
    print("Q3 — Average point differential: home wins vs away wins")
    print("=" * 60)
 
    #Drop games with missing score
    clean = games.dropna(subset=["PTS_home", "PTS_away"])
 
    #Calculates margin which shows absolute difference in score
    diff = clean.withColumn("MARGIN", F.abs(F.col("PTS_home") - F.col("PTS_away")))
 
    #Group by home team wins and shows average winning margin and average points for each side
    diff.groupBy("HOME_TEAM_WINS").agg(
        F.count("*").alias("GAMES"),
        F.round(F.mean("MARGIN"), 2).alias("AVG_MARGIN"),       
        F.round(F.mean("PTS_home"), 1).alias("AVG_PTS_HOME"),   
        F.round(F.mean("PTS_away"), 1).alias("AVG_PTS_AWAY"),   
    ).withColumn(
        "WINNER", F.when(F.col("HOME_TEAM_WINS") == 1, "Home").otherwise("Away")
    ).select("WINNER", "GAMES", "AVG_MARGIN", "AVG_PTS_HOME", "AVG_PTS_AWAY").show()
 
    # Per-season breakdown. CASE WHEN makes sure that the average margin is only for games the home team or away team has won
    spark = games.sparkSession
    diff.createOrReplaceTempView("games_diff")
    spark.sql("""
        SELECT SEASON,
               ROUND(AVG(CASE WHEN HOME_TEAM_WINS = 1 THEN MARGIN END), 2) AS HOME_WIN_MARGIN,
               ROUND(AVG(CASE WHEN HOME_TEAM_WINS = 0 THEN MARGIN END), 2) AS AWAY_WIN_MARGIN
        FROM   games_diff
        GROUP  BY SEASON
        ORDER  BY SEASON
    """).show(30, truncate=False)
 
#Main
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default=".", help="Path to CSV files")
    args = parser.parse_args()
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    games, teams, ranking = load_tables(spark, args.data_dir)
 
    q1_team_trend(games, teams)
    q2_three_point_vs_wins(games)
    q3_point_differential(games)
 
    spark.stop()
 
 
if __name__ == "__main__":
    main()
 


