# pg-gendata

testdata テーブルに意味のないデータを追加する

```
DSN="user=postgres password=postgres host=127.0.0.1 port=5432 dbname=postgres sslmode=disable" \
./pg-gendata --records=50000 [--init]
```
