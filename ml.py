#Imports
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.ml import Pipeline
from pyspark.ml.functions import vector_to_array
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    MulticlassClassificationEvaluator,
)
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
 
#Feature Columns
FEATURE_COLS = [
    "FG_PCT_home", "FT_PCT_home", "FG3_PCT_home", "AST_home", "REB_home",
    "FG_PCT_away", "FT_PCT_away", "FG3_PCT_away", "AST_away", "REB_away",
    "PTS_home_roll5", "PTS_away_roll5",
]
 
#The column we are trying to predict (1 is home team win, 0 is away team win)
LABEL_COL = "HOME_TEAM_WINS"
 
#Games before this season we train, otherwise we use for test
TRAIN_YEAR = 2021
 
#Spark Session for NBA Win Prediction
def build_spark():
    return(
        SparkSession.builder
        .appName("NBA_Win_Prediction")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
 
def add_rolling_features(games):
 
    #Games variable with games date as the range for each team
    games = games.withColumn("GAME_DATE", F.to_date("GAME_DATE_EST"))
 
    #Home Team Rolling Points (Avg points scored over the last 5 games)
    w_home = (
        Window.partitionBy("HOME_TEAM_ID")
        .orderBy("GAME_DATE")
        .rowsBetween(-5,-1)
    )
 
    #Away Team Rolling Points (Avg points scored over the last 5 games)
    w_away = (
        Window.partitionBy("VISITOR_TEAM_ID")
        .orderBy("GAME_DATE")
        .rowsBetween(-5,-1)
    )
 
    #Makes a games variable that has 5 game rolling average.
    #We use the rolling average (momentum) because it is a better indicator of how a team will perform than a single game.
    games = (
        games
        .withColumn("PTS_home_roll5", F.round(F.avg("PTS_home").over(w_home), 2))
        .withColumn("PTS_away_roll5", F.round(F.avg("PTS_away").over(w_away), 2))
    )
 
    #Return games variable
    return games
 
#Drop null on features and labelCols. We cast label to a double
def prepare_data(games):
    all_cols = FEATURE_COLS + [LABEL_COL, "SEASON", "GAME_DATE"]
    df = games.select(all_cols).dropna()
    df = df.withColumn(LABEL_COL, F.col(LABEL_COL).cast("double"))
    return df
 
#Build a pipeline
#Vector Assembler combines all 12 features into one vector.
def build_pipeline():
    assembler = VectorAssembler(
        inputCols = FEATURE_COLS,
        outputCol = "features_raw",
    )
 
    #Create a standard scaler and normalizes features.
    scaler = StandardScaler(
        inputCol = "features_raw",
        outputCol = "features",
        withMean = True,
        withStd = True
    )
 
    #Logistic Regression is the classifier
    lr = LogisticRegression(
        featuresCol = "features",
        labelCol = LABEL_COL,
        maxIter = 100,
    )
 
    #Pipeline consists of assembler, scaler, lr
    return Pipeline(stages=[assembler, scaler, lr])
 
#Print Metrics
def print_metrics(predictions, label: str = ""):
    if label:
        print(f"\n-- {label} --")
 
    #AUC-ROC measures how well the model separates wins from losses.
    auc = BinaryClassificationEvaluator(
        labelCol = LABEL_COL, metricName = "areaUnderROC"
    ).evaluate(predictions)
 
    #Accuracy measures the percentage of games predicted correctly
    acc = MulticlassClassificationEvaluator(
        labelCol = LABEL_COL, metricName = "accuracy"
    ).evaluate(predictions)
 
    #F1 measures the balance between precision and recall. Will tell if classes are imbalanced.
    f1 = MulticlassClassificationEvaluator(
        labelCol = LABEL_COL, metricName = "f1"
    ).evaluate(predictions)
 
    #Precision compares games predicated as home wins to actual number of home wins
    prec = MulticlassClassificationEvaluator(
        labelCol = LABEL_COL, metricName = "weightedPrecision"
    ).evaluate(predictions)
 
    #Recall compares home wins to number the model predicted correctly.
    rec = MulticlassClassificationEvaluator(
        labelCol = LABEL_COL, metricName = "weightedRecall"
    ).evaluate(predictions)
 
    #AUC-ROC
    print(f" AUC-ROC : {auc}")
    #Accuracy
    print(f" Accuracy : {acc}")
    #F1
    print(f" F1 score : {f1}")           
    #Prec
    print(f" Precision : {prec}")
    #Recall
    print(f" Recall : {rec}")
    return auc, acc
 
#Confusion Matrix which shows table of actual vs predicted labels so you can see where model is wrong.
def confusion_matrix(predictions):
    print("Confusion Matrix")
    cm = (
        predictions
        .groupBy(LABEL_COL, "prediction")
        .count()
        .orderBy(LABEL_COL, "prediction")
    )
    cm.show()
 
#Feature Importance prints each feature's coefficient from LR model.
#If it has larger abs value then that stat has more influence on prediction
def feature_importance(model):
    lr_model = model.stages[-1]
    print("Feature coefficients")
    for name, coef in sorted(
        zip(FEATURE_COLS, lr_model.coefficients.toArray()),
        key=lambda x: abs(x[1]),
        reverse=True,
    ):
        sign = "+" if coef > 0 else "-"                         
        print(f"{name:<22} {sign}{abs(coef)}")                
 
#Cross Validation tries different combinations of hyperparameters to find the best model.
#regParam controls strength of regularization which prevents overfitting
#elasticNetParam controls mix of L1 (Lasso) and L2(Ridge) regularization.
#NumFolds has data split into 3 ways.
#Learned these all from a previous class.
def cross_validate(pipeline, train_df):
    print("Cross Validation")
    lr = pipeline.getStages()[-1]
    param_grid = (
        ParamGridBuilder()
        .addGrid(lr.regParam, [0.01, 0.1, 0.5])
        .addGrid(lr.elasticNetParam,[0.0, 0.5])
        .build()
    )
 
    evaluator = BinaryClassificationEvaluator(
        labelCol = LABEL_COL, metricName = "areaUnderROC"
    )
 
    cv = CrossValidator(
        estimator = pipeline,
        estimatorParamMaps = param_grid,
        evaluator = evaluator,
        numFolds = 3,
        parallelism = 2
    )
 
    cv_model = cv.fit(train_df)
    print(f"Best AUC (CV): {max(cv_model.avgMetrics)}")
    return cv_model.bestModel
 
#Main
#Trains the model, evaluates the model, and saves it into storage.
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default = ".", help = "PATH_TO_CSV files")
    parser.add_argument("--cross-val", action = "store_true", help = "Run 3-fold cross validation")
    args = parser.parse_args()
 
    spark = build_spark()
 
    spark.sparkContext.setLogLevel("WARN")
 
    #Load and engineer
    print("Loading data...")
    games = spark.read.csv(
        f"{args.data_dir}/games.csv", header = True, inferSchema = True
    )
    print(f"Raw games: {games.count():,} rows")
 
    print("Engineering rolling features")
    games = add_rolling_features(games)
    df = prepare_data(games)
    print(f"After feature prep (nulls dropped): {df.count():,} rows")
 
    #Class Balance
    print("Label Distribution")
    df.groupBy(LABEL_COL).count().orderBy(LABEL_COL).show()
 
    #Chronological Splitting
    train_df = df.filter(F.col("SEASON") < TRAIN_YEAR)
    test_df = df.filter(F.col("SEASON") >= TRAIN_YEAR)
 
    #Training
    pipeline = build_pipeline()
 
    if args.cross_val:
        model = cross_validate(pipeline, train_df)
 
    else:
        print("Training Logistic Regression")
        model = pipeline.fit(train_df)
 
    #Evaluate
    train_preds = model.transform(train_df)
    test_preds = model.transform(test_df)
 
    print("Model Evaluation")
    print_metrics(train_preds, "Training_set")
    print_metrics(test_preds, "Testing_set")
 
    confusion_matrix(test_preds)
    feature_importance(model)
 
    #Predicting samples
    print("Sample Predictions")
    test_preds.select(
        "GAME_DATE",
        F.col(LABEL_COL).alias("ACTUAL"),
        F.col("prediction").alias("PREDICTED"),
        
    ).show(10)
 
    spark.stop()
 
 
if __name__ == "__main__":
    main()





