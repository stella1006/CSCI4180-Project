./run.sh download test_zero.txt con_test_zero.txt
diff test_zero.txt con_test_zero.txt

./run.sh download test_1.txt con_test_1.txt
diff test_1.txt con_test_1.txt

./run.sh download hello.txt con_hello.txt
diff hello.txt con_hello.txt

./run.sh download hellotest.txt con_hellotest.txt
diff hellotest.txt con_hellotest.txt

./run.sh download test.txt con_test.txt
diff test.txt con_test.txt
