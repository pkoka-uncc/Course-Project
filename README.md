# ITCS 6190 – Cloud Computing for Data Analysis

## Team: Prithvi Koka 

## Course Project: 
NBA Game Outcome Analysis with Apache Spark

## Problem Statement: 
Right now, the NBA Finals is happening. People are excited from all across the U.S. to support their favorite teams. People are stressed out about, but what if they were able to predict the games before they even happened? With NBA game data creates the perfect opportunity to look at performance trends, simulate real-time games and build predictive models for game outcomes. This project will design and implement a big data analytics pipeline using Apache Spark by processing, streaming, and modeling NBA statistics; demonstrating use of structured APIs, streaming, and MLLib working together on real data.

## Dataset: 
- **Name:** NBA Games Data 
- **Source:** By Nathan Lagua
- **Link:** https://www.kaggle.com/datasets/nathanlauga/nba-games 
- **Size:** All games from 2004 to 2020 (about 60000 games)

## Key Features/Fields: 
- **Game Stats:** (Points, Field Goal Percentage, Free Throw Percentage, Rebounds, Assists, Home Wins, Away Wins)
- **Team Stats:** (Wins, Losses, Win Percentage)

## Componenets Planned: 

### 1. Structured APIs 
Use Spark DataFrames and Spark SQL to perform batch analysis (apply same rules to data) on historical game and team statistics. Will include aggregations, join statements, and trend analytics.

### 2. Structured Streaming 
Simulate a live NBA game using Spark Structured Streaming. Can process incoming game events close to real time, tracking score and momentum shifts. 

### 3. MLLib 
Train a binary classification model (Logistic Regression) to predict wins/losses based on features such as home wins, away wins, recent win streak, average pts scored.

### Analytical Questions: 

Which teams have had the strong performance trend the last 5 years? (API - Spark DataFrames and Spark SQL)

Can we detect any momentum shifts in real time? (Streaming - Hadoop)

Can we predict the winner of a game based on pregame stats? (ML Model - Logistic Regression)

### How to run: 
Note: Make sure all directories are changed to where your project is stored.
To run component Structured APIs:  python batch.py > Batch_Output.txt   
To run component Structured Streaming (Won't work without Hadoop): 
Here is a guide on how to set it up through Windows https://gist.github.com/vorpal56/5e2b67b6be3a827b85ac82a63a5b3b2e.
Make sure to set your environmental variables through Hadoop in Powershell (For example, PS C:\Users\kokap\Downloads\Final Project> $env:HADOOP_HOME = "C:\hadoop"
>> $env:PATH = "$env:PATH;C:\hadoop\bin"). 
First: Run the consume, python streaming.py --mode consume > Streaming_Output.txt
Second: Run the produce, python streaming.py --mode produce --data-dir "C:\Users\kokap\Downloads\Final Project"  
To run component MLib: python ml.py > Machine_Learning_Output.txt

Code is also commented with what functions do so that may also help you when it comes to running.

## Results: 

### Q1 Analytical Question (Contained in Batch_Output.txt):
The Miluwake Bucks (Avg Win Margin: 0.663%, Avg Pts Win Margin: 5.79pts), Philadelphia 76ers (Avg Win Margin: 0.622%, Avg Pts Win Margin: 3.52pts), and Denver Nuggets(Avg Win Margin: 0.606%, Avg Pts Win Margin: 2.35pts) are the teams with the strongest performance trends in the last 5 years. 

Another analytical question I created: Does 3 point % impact wins?
Yes, indeed they do... if there were 3

### Q2 Analytical Question (Contained in Streaming_Output.txt):
Yes we can detect momentum shifts when it comes to teams in real time. In the game we tracked, the home team had the momentum by outscoring the away team each quarter. If the opposite happens, the away team will have the momentum for the quarter (for example in Batch 2).

### Q3 Analytical Question (Contained in Machine_Learning_Output.txt):


