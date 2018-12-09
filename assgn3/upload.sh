#
time java -cp .:./lib/* MyDedup upload 8 128 256 64 test/hello.txt azure
time java -cp .:./lib/* MyDedup upload 1024 2048 4096 64 test/IntegratedPros.txt azure
time java -cp .:./lib/* MyDedup upload 1024 2048 4096 64 test/aaa.bin azure
time java -cp .:./lib/* MyDedup upload 8 128 256 64 test/hellotest.txt azure
time java -cp .:./lib/* MyDedup upload 8 128 256 64 test/test.txt azure
time java -cp .:./lib/* MyDedup upload 8 128 256 64 test/test_1.txt azure


# ./run.sh up test/hello.txt
# ./run.sh up test/test_zero.txt
# ./run.sh up test/hellotest.txt
# ./run.sh up test/test.txt
# ./run.sh up test/test_1.txt
