
if [ "$1" = "up" ]; then
    java -cp .:./lib/* MyDedup upload 16 128 256 64 $2 $3
elif [ "$1" = "do" ]; then
    time java -cp .:./lib/* MyDedup download $2 $3 $4
    diff $2 $3
elif [ "$1" = "de" ]; then
    time java -cp .:./lib/* MyDedup delete $2 $3
fi
