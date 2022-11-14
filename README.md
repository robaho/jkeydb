DEPRECATED. Use [robaho/jleveldb](https://github.com/robaho/jleveldb) instead.

Java version of [keydb](https://github.com/robaho/keydb) - an ultra fast key/value database.

It lacks most of the test cases of the Go version, but is binary file compatible. It was primarily created to test the performance difference between Go and Java.

Performance numbers using the GraalVM on OSX 11.1 2015 iMac 4ghz i7

```
insert time 10000000 records = 15443ms, usec per op 1.5443
close time 4954ms
scan time 1934ms, usec per op 0.1934
scan time 50% 81ms, usec per op 0.162
random access time 6.264us per get
close with merge 1 time 0ms
scan time 2077ms, usec per op 0.2077
scan time 50% 67ms, usec per op 0.134
random access time 6.083us per get
```
