#
time java -cp .:./lib/* MyDedup upload 8 128 256 64 test/hello.txt $1
time java -cp .:./lib/* MyDedup upload 1024 2048 4096 64 test/galaxy_20K.bin $1
time java -cp .:./lib/* MyDedup upload 1024 2048 4096 64 test/model.txt $1
time java -cp .:./lib/* MyDedup upload 8 128 256 64 test/hellotest.txt $1
time java -cp .:./lib/* MyDedup upload 8 128 256 64 test/test.txt $1
time java -cp .:./lib/* MyDedup upload 8 128 256 64 test/test_1.txt $1


# time java -cp .:./lib/* MyDedup upload 8 128 256 64  test/hello.txt $1
# time java -cp .:./lib/* MyDedup upload 8 128 256 64  test/test_zero.txt $1
# time java -cp .:./lib/* MyDedup upload 8 128 256 64  test/hellotest.txt $1
# time java -cp .:./lib/* MyDedup upload 8 128 256 64  test/test.txt $1
# time java -cp .:./lib/* MyDedup upload 8 128 256 64  test/test_1.txt $1
