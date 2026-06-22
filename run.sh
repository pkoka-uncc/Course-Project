echo "Running Component 1 - Structured APIs..."
python Components/batch.py --data-dir "." > "Outputs for Components/Batch_Output.txt"

echo "Running Component 2 - Streaming..."
python Components/streaming.py --mode consume > "Outputs for Components/Streaming_Output.txt" &
CONSUMER_PID=$!
sleep 10
python Components/streaming.py --mode produce --data-dir "."
kill $CONSUMER_PID

echo "Running Component 3 - MLlib..."
python Components/ml.py --data-dir "." > "Outputs for Components/Machine_Learning_Output.txt"

echo "All components complete."