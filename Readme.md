
# SQL
```shell
CREATE TABLE UserScores (name STRING, score INT)
WITH (
'connector' = 'http-restful',
'path' = '/flink/table1',
'port' = '8080',
'format' = 'http-restful-json'
-- TODO
-- 'mode' = 'server'
);
CREATE TABLE print_table  
(id STRING,score INT)
WITH ('connector' = 'print','sink.parallelism'='1','standard-error'='true') ;

insert into print_table select name,score from UserScores;
```

```shell
POST
DELETE
PUT
```


```shell
CREATE TABLE UserScores (name STRING, score INT)
WITH (
'connector' = 'http-restful',
'remote-url' = 'http://localhost:8088/some/re',
#'method' = 'post',-- ''
'format' = 'http-restful-json'
-- 'mode' = 'client'
-- 'http-restful.headers.Cookie' = '_ga=GA1.2.2136019336.1646989403'
-- 'http-restful.headers.Referer' = 'https://www.baidu.com'
);

```