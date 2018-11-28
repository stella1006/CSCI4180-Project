
if [ "$1" = "upload" ]; then
    java -cp .:./lib/* MyDedup upload 5 13 10 10 $2 local
elif [ "$1" = "download" ]; then
    java -cp .:./lib/* MyDedup download $2 $3 local
elif [ "$1" = "delete" ]; then
    java -cp .:./lib/* MyDedup delete $2 local
fi
