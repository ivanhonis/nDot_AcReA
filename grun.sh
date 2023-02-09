echo --------------------------------------
echo nDot - Background runner!
echo --------------------------------------

ps aux
for KILLPID in `ps ax | grep 'python3' | awk ' { print $1;}'`; do
  echo kill pid $KILLPID;
  kill -9 $KILLPID;
done

echo --------------------------------------
echo Run: grid_acrea.py
echo --------------------------------------

nohup python3 grid_acrea.py > output.log &

