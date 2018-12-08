
if [ "$1" = "upload" ]; then
    time java -cp .:./lib/* MyDedup upload 10240 15000 20480 128 $2 local
elif [ "$1" = "download" ]; then
    time java -cp .:./lib/* MyDedup download $2 $3 local
elif [ "$1" = "delete" ]; then
    time java -cp .:./lib/* MyDedup delete $2 local
fi
