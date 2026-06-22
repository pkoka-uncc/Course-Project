# NBA Game Outcome Analysis with Apache Spark
 
**Course:** ITCS 6190 – Cloud Computing for Data Analysis  
**Author:** Prithvi Koka
 
---
 
## Problem Statement
 
Right now, the NBA Finals is happening. People are excited from all across the U.S. to support their favorite teams. What if they were able to predict the games before they even happened? NBA game data creates the perfect opportunity to look at performance trends, simulate real-time games, and build predictive models for game outcomes. This project designs and implements a big data analytics pipeline using Apache Spark by processing, streaming, and modeling NBA statistics — demonstrating use of Structured APIs, Streaming, and MLlib working together on real data.
 
---
 
## Dataset
 
| Field | Details |
|---|---|
| Name | NBA Games Data |
| Source | Nathan Lauga |
| Link | https://www.kaggle.com/datasets/nathanlauga/nba-games |
| Size | All games from 2003 to 2022 (~26,000 games) |
 
**Key Features:**
- Game Stats: Points, Field Goal %, Free Throw %, 3-Point %, Rebounds, Assists, Home Wins
- Team Stats: Wins, Losses, Win Percentage
---
 
## Components
 
### 1. Structured APIs
Uses Spark DataFrames and Spark SQL to perform batch analysis on historical game and team statistics. Includes aggregations, join statements, and trend analytics.
 
### 2. Structured Streaming
Simulates a live NBA game using Spark Structured Streaming. Processes incoming game events in near real time, tracking score and momentum shifts.
 
### 3. MLlib
Trains a binary Logistic Regression classifier to predict wins/losses based on features such as FG%, 3P%, rebounds, assists, and 5-game rolling average points.
 
---
 
## Analytical Questions
 
| Question | Component |
|---|---|
| Which teams have had the strongest performance trend over the last 5 seasons? | Structured APIs (Spark DataFrames + Spark SQL) |
| Can we detect momentum shifts in real time? | Structured Streaming |
| Can we predict the winner of a game based on pregame stats? | MLlib (Logistic Regression) |
 
---
 
## How to Run
git clone https://github.com/pkoka-uncc/Course-Project.git
 
> **Note:** Make sure all directories are changed to where your project is stored. Use `cd` to navigate to the right directory.

> **Note:** Requires Hadoop on Windows. Setup guide: https://gist.github.com/vorpal56/5e2b67b6be3a827b85ac82a63a5b3b2e
 
Make sure  environment variables in Bash:
```bash
$env:HADOOP_HOME = "C:\hadoop"
$env:PATH = "$env:PATH;C:\hadoop\bin"
```
Do bash run.sh in your bash terminal.
 
## Results
 
### Q1 — Strongest Team Performance (Last 5 Seasons)
 
The Milwaukee Bucks, Philadelphia 76ers, and Denver Nuggets are the top three performing teams over the last 5 seasons based on average win percentage and point margin.
 
```
+---------------------+-----------+----------+
|TEAM_NAME            |AVG_WIN_PCT|AVG_MARGIN|
+---------------------+-----------+----------+
|Milwaukee Bucks      |0.663      |5.79      |
|Philadelphia 76ers   |0.622      |3.52      |
|Denver Nuggets       |0.606      |2.35      |
|Boston Celtics       |0.594      |4.41      |
|Utah Jazz            |0.593      |3.96      |
|Los Angeles Clippers |0.573      |2.0       |
|Toronto Raptors      |0.563      |2.65      |
|Miami Heat           |0.561      |1.13      |
|Brooklyn Nets        |0.549      |0.86      |
|Phoenix Suns         |0.538      |1.44      |
|Dallas Mavericks     |0.533      |1.92      |
|Memphis Grizzlies    |0.532      |1.28      |
|Golden State Warriors|0.518      |0.63      |
|Los Angeles Lakers   |0.504      |-0.33     |
|Indiana Pacers       |0.484      |0.03      |
+---------------------+-----------+----------+
```
 
**Does 3-Point % impact wins? Have teams improved their 3PT accuracy over time?**
 
Teams shooting >= 40% from 3 win 76.4% of their games — a clear correlation between 3P% and winning. However, the league-wide 3PT accuracy has remained stable between 33–36.5% across all 20 seasons, meaning teams haven't gotten more accurate — they've just attempted more shots.
 
```
+----------+-----+-------------+-------+
|FG3_BUCKET|GAMES|HOME_WIN_RATE|AVG_PTS|
+----------+-----+-------------+-------+
|< 30%     |7668 |0.404        |95.9   |
|30-35%    |4900 |0.524        |102.3  |
|35-40%    |4671 |0.613        |105.3  |
|>= 40%    |9313 |0.764        |109.3  |
+----------+-----+-------------+-------+
```
 
**What is the average point differential between home and away team wins?**
 
Home teams win by an average of 11.86 points, while away teams win by 10.15 points. This margin has slightly increased each season, reinforcing the home court advantage.
 
```
+------+-----+----------+------------+------------+
|WINNER|GAMES|AVG_MARGIN|AVG_PTS_HOME|AVG_PTS_AWAY|
+------+-----+----------+------------+------------+
|  Home|15645|     11.86|       107.8|        96.0|
|  Away|10907|     10.15|        97.2|       107.3|
+------+-----+----------+------------+------------+
```
 
---
 
### Q2 — Momentum Detection (Structured Streaming)
 
Yes, momentum shifts can be detected in real time. A producer stream simulates live NBA game data quarter by quarter, and a consumer stream processes and displays it. In the game tracked (ID: 20301158), the home team led every quarter.
 
```
+--------+-------+--------+--------+------------+
|game_id |quarter|HOME_PTS|AWAY_PTS|MOMENTUM    |
+--------+-------+--------+--------+------------+
|20301158|1      |23      |22      |Home leading|
|20301158|2      |23      |21      |Home leading|
|20301158|3      |24      |22      |Home leading|
|20301158|4      |20      |19      |Home leading|
+--------+-------+--------+--------+------------+
```
 
Top performers from the simulated game:
 
```
+-----------------+----------+---+---+---+
|player_name      |team_id   |PTS|AST|REB|
+-----------------+----------+---+---+---+
|Amar'e Stoudemire|1610612756|26 |0  |6  |
|Shane Battier    |1610612763|19 |0  |4  |
|Leandro Barbosa  |1610612756|19 |3  |0  |
|Joe Johnson      |1610612756|16 |5  |4  |
|Stromile Swift   |1610612763|14 |0  |11 |
+-----------------+----------+---+---+---+
```
 
---
 
### Q3 — Game Outcome Prediction (MLlib)
 
A Logistic Regression model was trained to predict home team wins using 12 features: FG%, FT%, 3P%, Rebounds, Assists (home and away), and 5-game rolling average points. The model was trained on seasons before 2021 and tested on 2021+.
 
**Evaluation Metrics:**
 
| Metric | Training Set | Test Set |
|---|---|---|
| AUC-ROC | 0.9255 | 0.9361 |
| Accuracy | 84.3% | 85.3% |
| F1 Score | 0.8421 | 0.8531 |
| Precision | 0.8420 | 0.8532 |
| Recall | 0.8426 | 0.8534 |
 
**Confusion Matrix (Test Set):**
 
```
+--------------+----------+-----+
|HOME_TEAM_WINS|prediction|count|
+--------------+----------+-----+
|           0.0|       0.0|  686|
|           0.0|       1.0|  158|
|           1.0|       0.0|  125|
|           1.0|       1.0|  962|
+--------------+----------+-----+
```
 
**Feature Coefficients (ranked by influence):**
 
```
FG_PCT_away            -1.4327
FG_PCT_home            +1.3911
REB_away               -0.6236
REB_home               +0.5919
FG3_PCT_home           +0.5146
FG3_PCT_away           -0.4976
FT_PCT_home            +0.4255
FT_PCT_away            -0.4176
AST_home               +0.4115
AST_away               -0.3822
PTS_away_roll5         -0.2510
PTS_home_roll5         +0.2496
```
 
Field goal percentage (both home and away) is the strongest predictor of game outcomes. Positive coefficients increase the probability of a home team win, negative coefficients decrease it.
 
**Sample Predictions:**
 
```
+----------+------+---------+
| GAME_DATE|ACTUAL|PREDICTED|
+----------+------+---------+
|2021-10-04|   1.0|      1.0|
|2021-10-09|   0.0|      0.0|
|2021-10-23|   1.0|      1.0|
|2021-10-27|   0.0|      1.0|
|2021-10-28|   1.0|      1.0|
|2021-10-30|   1.0|      1.0|
|2021-11-03|   1.0|      1.0|
|2021-11-06|   1.0|      1.0|
|2021-11-08|   1.0|      1.0|
|2021-11-09|   1.0|      1.0|
+----------+------+---------+
```
 


