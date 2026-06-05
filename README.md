# ITCS 6190 – Cloud Computing for Data Analysis

## Team: Prithvi Koka 

## Course Project: NBA Game Outcome Analysis with Apache Spark

## Problem Statement: NBA game data creates an opportunity to look at performance trends, simulate real-time games and build predictive models for game outcomes. This project will design and implement a big data analytics pipeline using Apache Spark by processing, streaming, and modeling NBA statistics; demonstrating use of structured APIs, streaming, and MLLib working together on real data.

## Dataset: 
**Name:** NBA Games Data 
**Source:** By Nathan Lagua Link: https://www.kaggle.com/datasets/nathanlauga/nba-games 
**Size:** All games from 2004 to 2020 (about 60000 games)

## Key Features/Fields: 
**Game Stats:** (Points, Field Goal Percentage, Free Throw Percentage, Rebounds, Assists, Home Wins, Away Wins)
**Team Stats:** (Wins, Losses, Win Percentage)

## Componenets Planned: 

### 1. Structured APIs - Use Spark DataFrames and Spark SQL to perform batch analysis (apply same rules to data) on historical game and team statistics. Will include aggregations, join statements, and trend analytics.

### 2. Structured Streaming - Simulate a live NBA game using Spark Structured Streaming. Can process incoming game events close to real time, tracking score and momentum shifts. 

### 3. MLLib - Train a binary classification model (Linear regression) to predict wins/losses based on features such as home wins, away wins, recent win streak, average pts score/

### Analytical Questions: 

Which teams have had the strong performance trend the last 5 years? (API)

Can we detect any momentum shifts in real time? (Streaming)

Can we predict the winner of a game based on pregame stats? (ML Model)

