# dualconn

1. `gurl :8080/query q=='select * from kv'`
2. `gurl :8080/info`
3. `gurl :8080/enable target=="127.0.0.1:3301" disable==1`

```sh
$ gurl :8080/query q=='select * from kv'
{
  "cost": "2.86166ms",
  "rows": [
    {
      "k": "'k1'",
      "v": "'Shieldfuschia'"
    }
  ]
}

$ gurl :8080/query q=='select * from kv'
{
  "cost": "2.241875ms",
  "rows": [
    {
      "k": "'k1'",
      "v": "'Lancerchestnut'"
    }
  ]
}

$ gurl :8080/info
{
  "timeout": 3000000000,
  "targets": [
    {
      "addr": "127.0.0.1:3301",
      "dialTime": "2024-03-28T12:04:03.832635+08:00",
      "conns": {
        "2eIilVzuc4wkqmdTX9tAmXJDuRu": {
          "readN": 263,
          "writeN": 224,
          "readLast": "2024-03-28T12:04:03.835553+08:00",
          "writeLast": "2024-03-28T12:04:03.834964+08:00",
          "closed": false
        }
      }
    },
    {
      "addr": "127.0.0.1:3302"
    }
  ],
  "protagonistHalo": true
}
```
