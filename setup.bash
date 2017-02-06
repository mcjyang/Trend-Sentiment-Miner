python getData.py &
spark-submit streaming.py > out.txt 2> err.txt &
python server.py