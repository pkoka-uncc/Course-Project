ITCS 6190 – Cloud Computing for Data Analysis
Team: Prithvi Koka
Course Project:
NBA Game Outcome Analysis with Apache Spark

Problem Statement:
Right now, the NBA Finals is happening. People are excited from all across the U.S. to support their favorite teams. People are stressed out about, but what if they were able to predict the games before they even happened? With NBA game data creates the perfect opportunity to look at performance trends, simulate real-time games and build predictive models for game outcomes. This project will design and implement a big data analytics pipeline using Apache Spark by processing, streaming, and modeling NBA statistics; demonstrating use of structured APIs, streaming, and MLLib working together on real data.

Dataset:
Name: NBA Games Data
Source: By Nathan Lagua
Link: https://www.kaggle.com/datasets/nathanlauga/nba-games
Size: All games from 2004 to 2020 (about 60000 games)
Key Features/Fields:
Game Stats: (Points, Field Goal Percentage, Free Throw Percentage, Rebounds, Assists, Home Wins, Away Wins)
Team Stats: (Wins, Losses, Win Percentage)
Componenets Planned:
1. Structured APIs
Use Spark DataFrames and Spark SQL to perform batch analysis (apply same rules to data) on historical game and team statistics. Will include aggregations, join statements, and trend analytics.

2. Structured Streaming
Simulate a live NBA game using Spark Structured Streaming. Can process incoming game events close to real time, tracking score and momentum shifts.

3. MLLib
Train a binary classification model (Logistic Regression) to predict wins/losses based on features such as home wins, away wins, recent win streak, average pts scored.

Analytical Questions:
Which teams have had the strong performance trend the last 5 years? (API - Spark DataFrames and Spark SQL)

Can we detect any momentum shifts in real time? (Streaming - Hadoop)

Can we predict the winner of a game based on pregame stats? (ML Model - Logistic Regression)

How to run:
Note: Make sure all directories are changed to where your project is stored. Note: Use cd to get to the right directory where the component files are. To run component Structured APIs: python Components/batch.py > "Outputs for Components/Batch_Output.txt"
To run component Structured Streaming (Won't work without Hadoop): Here is a guide on how to set it up through Windows https://gist.github.com/vorpal56/5e2b67b6be3a827b85ac82a63a5b3b2e. Make sure to set your environmental variables through Hadoop in Powershell (For example, PS C:\Users\kokap\Downloads\Final Project> $env:HADOOP_HOME = "C:\hadoop"

$env:PATH = "$env:PATH;C:\hadoop\bin"). First: Run the consume, python Components/streaming.py --mode consume > "Outputs for Components/Streaming_Output.txt" Second: Run the produce, python Components/streaming.py --mode produce --data-dir "C:\Users\kokap\Downloads\Final Project"
To run component MLib: python Components/ml.py > "Outputs for Components/Machine_Learning_Output.txt"

Code is also thoroughly commented with what functions do so that may also help you when it comes to running.

Results:
Q1 Analytical Question (Contained in Batch_Output.txt):
We can answer this by loading team data on to Spark DataFrames and creating tables while simultaneously creating queries through Spark SQL to analyze data.

The Miluwake Bucks (Avg Win Margin: 0.663%, Avg Pts Win Margin: 5.79pts), Philadelphia 76ers (Avg Win Margin: 0.622%, Avg Pts Win Margin: 3.52pts), and Denver Nuggets(Avg Win Margin: 0.606%, Avg Pts Win Margin: 2.35pts) are the teams with the strongest performance trends in the last 5 years.

Q1 - Strongest team performance (last 5 seasons) +---------------------+-----------+----------+ |TEAM_NAME |AVG_WIN_PCT|AVG_MARGIN| +---------------------+-----------+----------+ |Milwaukee Bucks |0.663 |5.79 | |Philadelphia 76ers |0.622 |3.52 | |Denver Nuggets |0.606 |2.35 | |Boston Celtics |0.594 |4.41 | |Utah Jazz |0.593 |3.96 | |Los Angeles Clippers |0.573 |2.0 | |Toronto Raptors |0.563 |2.65 | |Miami Heat |0.561 |1.13 | |Brooklyn Nets |0.549 |0.86 | |Phoenix Suns |0.538 |1.44 | |Dallas Mavericks |0.533 |1.92 | |Memphis Grizzlies |0.532 |1.28 | |Golden State Warriors|0.518 |0.63 | |Los Angeles Lakers |0.504 |-0.33 | |Indiana Pacers |0.484 |0.03 | +---------------------+-----------+----------+

Another analytical question I created: Does 3 point made % impact wins and have NBA teams been more accurate in their 3PT shots over time? +----------+-----+-------------+-------+ |FG3_BUCKET|GAMES|HOME_WIN_RATE|AVG_PTS| +----------+-----+-------------+-------+ |30-35% |4900 |0.524 |102.3 | |35-40% |4671 |0.613 |105.3 | |< 30% |7668 |0.404 |95.9 | |>= 40% |9313 |0.764 |109.3 | +----------+-----+-------------+-------+

Season 3P% trend +------+-------------+-------------+ |SEASON|AVG_3PCT_HOME|AVG_3PCT_AWAY| +------+-------------+-------------+ |2003 |0.343 |0.33 | |2004 |0.355 |0.347 | |2005 |0.35 |0.356 | |2006 |0.353 |0.348 | |2007 |0.36 |0.353 | |2008 |0.364 |0.358 | |2009 |0.35 |0.35 | |2010 |0.356 |0.349 | |2011 |0.345 |0.338 | |2012 |0.363 |0.344 | |2013 |0.356 |0.355 | |2014 |0.35 |0.343 | |2015 |0.355 |0.346 | |2016 |0.365 |0.346 | |2017 |0.361 |0.359 | |2018 |0.357 |0.351 | |2019 |0.36 |0.353 | |2020 |0.366 |0.362 | |2021 |0.355 |0.35 | |2022 |0.356 |0.347 | +------+-------------+-------------+

Yes, indeed they do... the NBA teams that have shot 3s at a higher percentage are also more likely to win the game as evidenced by the >= 40% having a higher win percentage than the other 3 Point Percentage ranges. The NBA teams have actually had a stable 3PT Made% hovering around 33% - 36.5% showing evidence that NBA players haven't gotten better at 3pt shots. Home teams have a small, but slightly better percentage at making 3PT shots typically.

Another analytical question: What is the average point differential between home teams and away teams and what is the average point differental for home team wins compared to away team wins for each season? +------+-----+----------+------------+------------+ |WINNER|GAMES|AVG_MARGIN|AVG_PTS_HOME|AVG_PTS_AWAY| +------+-----+----------+------------+------------+ | Home|15645| 11.86| 107.8| 96.0| | Away|10907| 10.15| 97.2| 107.3| +------+-----+----------+------------+------------+

+------+---------------+---------------+ |SEASON|HOME_WIN_MARGIN|AWAY_WIN_MARGIN| +------+---------------+---------------+ |2003 |11.43 |8.73 | |2004 |11.25 |9.08 | |2005 |11.2 |8.95 | |2006 |11.41 |9.39 | |2007 |12.53 |10.1 | |2008 |11.67 |10.04 | |2009 |11.62 |10.26 | |2010 |11.44 |9.38 | |2011 |11.76 |9.92 | |2012 |11.63 |10.03 | |2013 |11.66 |9.9 | |2014 |11.54 |10.28 | |2015 |11.98 |10.27 | |2016 |12.55 |9.96 | |2017 |11.69 |10.82 | |2018 |12.33 |10.95 | |2019 |12.45 |10.52 | |2020 |12.23 |12.16 | |2021 |13.08 |11.75 | |2022 |12.08 |9.87 | +------+---------------+---------------+

As we can see, home teams win by around 11.86 points while for away teams win by around 10.15 points. There is a trend of the point differential seems to be slightly increasing each season for both home team wins and away team wins.

Q2 Analytical Question (Contained in Streaming_Output.txt):
Yes, we can detect momentum shifts when it comes to teams in real time. We do this by taking data from a historical game and streaming it as output using queries. We create a producer stream that creates the Live NBA Data and a consumer stream that streams the data to us in real time. In the game we tracked, the home team had the momentum by outscoring the away team each quarter (Batch 0). If the opposite happens where the away team scores more points, the away team will have the momentum for the quarter. (Batch 2 - Quarter 4, Note: Accidentally had 2 producer streams so thats why it shows quarter 4 twice)

Batch: 0
+--------+-------+--------+--------+------------+ |game_id |quarter|HOME_PTS|AWAY_PTS|MOMENTUM | +--------+-------+--------+--------+------------+ |20301158|1 |23 |22 |Home leading| |20301158|2 |23 |21 |Home leading| |20301158|3 |24 |22 |Home leading| |20301158|4 |20 |19 |Home leading| +--------+-------+--------+--------+------------+

Batch: 2
+--------+-------+--------+--------+------------+ |game_id |quarter|HOME_PTS|AWAY_PTS|MOMENTUM | +--------+-------+--------+--------+------------+ |20301158|1 |23 |22 |Home leading| |20301158|2 |23 |21 |Home leading| |20301158|3 |24 |22 |Home leading| |11000021|4 |7 |8 |Away leading| |20301158|4 |20 |19 |Home leading| +--------+-------+--------+--------+------------+

Scoreboard: +-----------------+----------+---+---+---+ |player_name |team_id |PTS|AST|REB| +-----------------+----------+---+---+---+ |Amar'e Stoudemire|1610612756|26 |0 |6 | |Shane Battier |1610612763|19 |0 |4 | |Leandro Barbosa |1610612756|19 |3 |0 | |Joe Johnson |1610612756|16 |5 |4 | |Stromile Swift |1610612763|14 |0 |11 | |James Posey |1610612763|13 |1 |5 | |Casey Jacobsen |1610612756|12 |0 |4 | |Jason Williams |1610612763|10 |6 |0 | |Shawn Marion |1610612756|9 |0 |9 | |Bo Outlaw |1610612763|8 |0 |5 | +-----------------+----------+---+---+---+

Simulating game 20301158 Home team ID: 1610612756 | Away team ID: 1610612763 Players in game: 19 Q1 | Mike Miller +1 pts Q1 | Amar'e Stoudemire +5 pts Q1 | Lorenzen Wright +1 pts Q1 | Casey Jacobsen +3 pts Q1 | Antonio McDyess +1 pts Q1 | Stromile Swift +4 pts Q1 | James Posey +3 pts Q1 | Earl Watson +1 pts Q1 | Jason Williams +2 pts Q1 | Bo Outlaw +2 pts Q1 | Shawn Marion +2 pts Q1 | Zarko Cabarkapa +0 pts Q1 | Shane Battier +6 pts Q1 | Jake Tsakalidis +1 pts Q1 | Joe Johnson +5 pts Q1 | Leandro Barbosa +6 pts Q1 | Donnell Harvey +0 pts Q1 | Maciej Lampe +1 pts Q1 | Theron Smith +1 pts

── End of Q1 ── Q2 | Maciej Lampe +1 pts Q2 | Shawn Marion +3 pts Q2 | Theron Smith +1 pts Q2 | Leandro Barbosa +4 pts Q2 | Mike Miller +1 pts Q2 | Amar'e Stoudemire +8 pts Q2 | Jake Tsakalidis +0 pts Q2 | James Posey +4 pts Q2 | Zarko Cabarkapa +0 pts Q2 | Shane Battier +4 pts Q2 | Donnell Harvey +0 pts Q2 | Stromile Swift +3 pts Q2 | Joe Johnson +4 pts Q2 | Jason Williams +3 pts Q2 | Earl Watson +1 pts Q2 | Lorenzen Wright +1 pts Q2 | Casey Jacobsen +3 pts Q2 | Bo Outlaw +3 pts Q2 | Antonio McDyess +0 pts

── End of Q2 ── Q3 | Zarko Cabarkapa +0 pts Q3 | Shane Battier +6 pts Q3 | Jason Williams +2 pts Q3 | Amar'e Stoudemire +8 pts Q3 | Stromile Swift +3 pts Q3 | Donnell Harvey +0 pts Q3 | Bo Outlaw +2 pts Q3 | Lorenzen Wright +1 pts Q3 | James Posey +3 pts Q3 | Casey Jacobsen +3 pts Q3 | Joe Johnson +4 pts Q3 | Theron Smith +1 pts Q3 | Antonio McDyess +1 pts Q3 | Earl Watson +1 pts Q3 | Maciej Lampe +1 pts Q3 | Shawn Marion +2 pts Q3 | Leandro Barbosa +5 pts Q3 | Mike Miller +2 pts Q3 | Jake Tsakalidis +1 pts

── End of Q3 ── Q4 | Stromile Swift +4 pts Q4 | Joe Johnson +3 pts Q4 | Jason Williams +3 pts Q4 | Shane Battier +3 pts Q4 | Amar'e Stoudemire +5 pts Q4 | Theron Smith +1 pts Q4 | Jake Tsakalidis +1 pts Q4 | Casey Jacobsen +3 pts Q4 | Maciej Lampe +1 pts Q4 | Zarko Cabarkapa +1 pts Q4 | Lorenzen Wright +1 pts Q4 | James Posey +3 pts Q4 | Leandro Barbosa +4 pts Q4 | Shawn Marion +2 pts Q4 | Bo Outlaw +1 pts Q4 | Antonio McDyess +1 pts Q4 | Earl Watson +1 pts Q4 | Mike Miller +1 pts Q4 | Donnell Harvey +0 pts

── End of Q4 ──

Q3 Analytical Question (Contained in Machine_Learning_Output.txt):
Yes, we can predict wins and losses using a logistic regression model that takes in feature data and uses training and testing by evaluating 5 factors. We can validate our logistic regression model using confusion matrix, feature importance, and cross validation. In the end, we were able to successfully predict winners and losers for the home team each game.

AUC-ROC - measures how well the model separates wins from losses Accuracy - measures the percentage of games predicted correctly F1 - measures the balance between precision and recall. Will tell if classes are imbalanced Precision - compares games predicated as home wins to actual number of home wins Recall - compares home wins to number of wins the model predicted correctly

We had a home team win count of 15618 and a home team loss count of 10839.

For our training set, AUC-ROC : 0.9255402193829424 Accuracy : 0.8425549227013832 F1 score : 0.8421441666326115 Precision : 0.8420200688441632 Recall : 0.8425549227013833

For our testing set, AUC-ROC : 0.9361159676835643 Accuracy : 0.8534438114966338 F1 score : 0.8530783624338772 Precision : 0.8532206338421899 Recall : 0.8534438114966338

Confusion Matrix +--------------+----------+-----+ |HOME_TEAM_WINS|prediction|count| +--------------+----------+-----+ | 0.0| 0.0| 686| | 0.0| 1.0| 158| | 1.0| 0.0| 125| | 1.0| 1.0| 962| +--------------+----------+-----+

Feature coefficients: FG_PCT_away -1.4327499458375432 FG_PCT_home +1.391138116999025 REB_away -0.6235835668166401 REB_home +0.591921406948027 FG3_PCT_home +0.5146439383760156 FG3_PCT_away -0.49757424894579855 FT_PCT_home +0.42545245929818093 FT_PCT_away -0.4175793189923284 AST_home +0.41150426440617505 AST_away -0.3821948681437488 PTS_away_roll5 -0.2509696472885987 PTS_home_roll5 +0.24958520405914403

Sample Predictions +----------+------+---------+ | GAME_DATE|ACTUAL|PREDICTED| +----------+------+---------+ |2021-10-04| 1.0| 1.0| |2021-10-09| 0.0| 0.0| |2021-10-23| 1.0| 1.0| |2021-10-27| 0.0| 1.0| |2021-10-28| 1.0| 1.0| |2021-10-30| 1.0| 1.0| |2021-11-03| 1.0| 1.0| |2021-11-06| 1.0| 1.0| |2021-11-08| 1.0| 1.0| |2021-11-09| 1.0| 1.0| +----------+------+---------+ only showing top 10 rows