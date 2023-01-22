echo --------------------------------------
echo nDot - Kill all Python process!
echo --------------------------------------

ps aux
for KILLPID in `ps ax | grep 'python3' | awk ' { print $1;}'`; do
  echo kill pid $KILLPID;
  kill -9 $KILLPID;
done

