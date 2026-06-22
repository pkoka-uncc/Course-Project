export HADOOP_HOME="C:/hadoop"
export PATH="$PATH:C:/hadoop/bin"

echo "Running Component 1 - Structured APIs"
python Components/batch.py --data-dir "." > "Outputs for Components/Batch_Output.txt"

echo "Running Component 2 - Streaming"

# Clean up any leftover stream files from previous runs
rm -f nba_stream/*.json

python Components/streaming.py --mode consume > "Outputs for Components/Streaming_Output.txt" &
CONSUMER_PID=$!

# Wait longer for Spark to fully initialize on Windows
sleep 30

python Components/streaming.py --mode produce --data-dir "."

# Give consumer time to process the last batch before killing
sleep 10

kill $CONSUMER_PID 2>/dev/null

echo "Running Component 3 - MLlib"
python Components/ml.py --data-dir "." > "Outputs for Components/Machine_Learning_Output.txt"

echo "All components complete."