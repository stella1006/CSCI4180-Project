#
time java -cp .:./lib/* MyDedup upload 8 128 256 64 test/hello.txt $1
time java -cp .:./lib/* MyDedup upload 1024 2048 4096 64 test/IntegratedPros.txt $1
time java -cp .:./lib/* MyDedup upload 1024 2048 4096 64 test/aaa.bin $1
time java -cp .:./lib/* MyDedup upload 8 128 256 64 test/hellotest.txt $1
time java -cp .:./lib/* MyDedup upload 8 128 256 64 test/test.txt $1
time java -cp .:./lib/* MyDedup upload 8 128 256 64 test/test_1.txt $1


# ./run.sh up test/hello.txt $1
# ./run.sh up test/test_zero.txt $1
# ./run.sh up test/hellotest.txt $1
# ./run.sh up test/test.txt $1
# ./run.sh up test/test_1.txt $1
