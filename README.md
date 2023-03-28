# Hadoop-NLP


```
$ java index.java < stopwords.txt
$ rm -r output
$ hadoop jar temp.jar DF ./../Wikipedia-articles/test/ ./output/test
$ hadoop jar temp.jar DF ./../Wikipedia-articles/full/ ./output/full
$ java top.java < ./../output/full/part-r-00000
$ hadoop jar temp_2.jar TFIDF ./../Wikipedia-articles/test/ ./output/secondtest/ ./../TarFiles/output/secondtest/part-r-00000  ./output/secondsectest
```
