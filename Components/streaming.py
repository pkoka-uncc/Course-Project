#Imports
import argparse
import json
import os
import random
import time
import pandas as pd
from datetime import datetime
from pathlib import Path
 
#The streaming file simulates real games live and in play-by-play
#Writer
STREAM_DIR = "C:/Users/kokap/Downloads/Final Project/nba_stream"
#Checker
CHECKPOINT = "C:/Users/kokap/Downloads/Final Project/nba_checkpoint"
 
#Produce
def produce(data_dir: str):                                             
 
    Path(STREAM_DIR).mkdir(parents=True, exist_ok=True)
 
    gd = pd.read_csv(f"{data_dir}/games_details.csv", low_memory=False)
    games = pd.read_csv(f"{data_dir}/games.csv")
 
    #Pick one completed game at random
    sample_game_id = random.choice(games["GAME_ID"].tolist())
    game_info = games[games["GAME_ID"] == sample_game_id].iloc[0]
    players = gd[gd["GAME_ID"] == sample_game_id].dropna(subset=["PTS"])  
 
    home_id = int(game_info["HOME_TEAM_ID"])
    away_id = int(game_info["VISITOR_TEAM_ID"])
 
    print(f"Simulating game {sample_game_id}")
    print(f"Home team ID: {home_id} | Away team ID: {away_id}")
    print(f"Players in game: {len(players)}\n")
 
    #Simulate quarters
    quarters = [1, 2, 3, 4]
    event_id = 0
    
    #Simulate a player's quarter worth of stats by applying a scale instead of using a full game.
    for quarter in quarters:
        quarter_players = players.sample(frac=1)                        
        for _, row in quarter_players.iterrows():                        
            scale = random.uniform(0.15, 0.35)
            event = {
                "event_id":    event_id,
                "game_id":     int(sample_game_id),
                "quarter":     quarter,
                "timestamp":   datetime.utcnow().isoformat(),
                "team_id":     int(row["TEAM_ID"]),
                "player_name": str(row["PLAYER_NAME"]),
                "pts":         max(0, round(float(row["PTS"]) * scale)),          
                "ast":         max(0, round(float(row["AST"]) * scale)) if pd.notna(row["AST"]) else 0,  
                "reb":         max(0, round(float(row["REB"]) * scale)) if pd.notna(row["REB"]) else 0,
                "home_team":   home_id,
                "away_team":   away_id,
            }
 
            #Creates a file and Opens the file and writes the event as a JSON microbatch
            fname = f"{STREAM_DIR}/event_{event_id:06d}.json"         
            with open(fname, "w") as f:
                json.dump(event, f)
            #Increment so file gets a unique name
            event_id += 1
            #Prints live update to terminal and waits 0.4 seconds before writing the next event
            print(f"  Q{quarter} | {row['PLAYER_NAME']} +{event['pts']} pts")
            time.sleep(0.4)
 
        #Prints a divider between each quarter
        print(f"\n  ── End of Q{quarter} ──\n")
        time.sleep(1.5)
 
 
#Spark created to read JSON event
def consume():
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F                             
    from pyspark.sql.types import (                                     
        StructType, StructField,
        IntegerType, StringType, LongType
    )
 
    spark = (
        SparkSession.builder
        .appName("NBA_Live_Streaming")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
 
    spark.sparkContext.setLogLevel("WARN")
 
    #Schema match for features of what the author writes
    schema = StructType([
        StructField("event_id",    LongType(),    True),
        StructField("game_id",     LongType(),    True),
        StructField("quarter",     IntegerType(), True),
        StructField("timestamp",   StringType(),  True),
        StructField("team_id",     LongType(),    True),
        StructField("player_name", StringType(),  True),
        StructField("pts",         IntegerType(), True),
        StructField("ast",         IntegerType(), True),
        StructField("reb",         IntegerType(), True),
        StructField("home_team",   LongType(),    True),
        StructField("away_team",   LongType(),    True),
    ])
 
    #Read stream from the directory
    raw = (                                                             
        spark.readStream
        .schema(schema)
        .json(STREAM_DIR)
    )
 
    #Live Score
    scoreboard = raw.groupBy("game_id", "team_id", "quarter").agg(     
        F.sum("pts").alias("TOTAL_PTS"),
        F.sum("ast").alias("TOTAL_AST"),
        F.sum("reb").alias("TOTAL_REB"),
        F.count("event_id").alias("EVENTS"),
    ).orderBy("quarter", "team_id")
 
    #Query for scoreboard by team
    q1 = (
        scoreboard.writeStream
        .outputMode("complete")
        .format("console")
        .option("truncate", False)
        .option("numRows", 30)
        .queryName("scoreboard")
        .start()
    )
 
    #Momentum In Game
    momentum = raw.groupBy("game_id", "quarter").agg(
        F.sum(F.when(F.col("team_id") == F.col("home_team"), F.col("pts"))).alias("HOME_PTS"),
        F.sum(F.when(F.col("team_id") == F.col("away_team"), F.col("pts"))).alias("AWAY_PTS"),
    ).withColumn(
        "MOMENTUM",
        F.when(F.col("HOME_PTS") > F.col("AWAY_PTS"), "Home leading")  
        .when(F.col("AWAY_PTS") > F.col("HOME_PTS"), "Away leading")
        .otherwise("Tied")
    ).orderBy("quarter")
 
    #Query 2 - Momentum Stream                                         
    q2 = (
        momentum.writeStream
        .outputMode("complete")
        .format("console")
        .option("truncate", False)
        .queryName("momentum")
        .start()
    )
 
    #Top Performer by points, assists, rebounds
    top_performers = raw.groupBy("player_name", "team_id").agg(
        F.sum("pts").alias("PTS"),
        F.sum("ast").alias("AST"),
        F.sum("reb").alias("REB"),
    ).orderBy(F.desc("PTS"))
 
    #Query 3 - Top Performers in Game
    q3 = (
        top_performers.writeStream                                      
        .outputMode("complete")                                         
        .format("console")
        .option("truncate", False)
        .option("numRows", 10)
        .queryName("top_performers")
        .start()
    )
 
    #Streams waiting for events
    print(f"\n Streaming from {STREAM_DIR} - Waiting for events...")
    print("Run the producer in another terminal:")
    print(" python component2_streaming --mode produce --data-dir /path/to/csvs\n")
 
    #Await Termination
    spark.streams.awaitAnyTermination()
 
 
#Creates a parser where you can type arguments into the script which it reads
def main():
    parser = argparse.ArgumentParser()
    #Defines two arguments for terminal: Either produce or consume. The producer reads the game data from CSVs and simulates live games into a JSON file in a folder.
    #The consumer watches the same folder and every time a new JSON file appears, picks up and processes it.
    #Updates live scoreboard, momentum tracker, and top performers.
    parser.add_argument("--mode", choices=["produce", "consume"], required=True)  
    parser.add_argument("--data-dir", default=".", help="Path to CSVs (producer only)")
    args = parser.parse_args()
 
    if args.mode == "produce":
        produce(args.data_dir)
    else:
        consume()
 
if __name__ == "__main__":                                              
    main()




    

