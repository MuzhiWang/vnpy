# Tornado Server API
### /stock_data

###### POST request body
```
{
  "symbol": "BTC-USD",
  "interval": "1m",
  "exchange": "COINBASE",
  "start_ts": 1565222400,
  "end_ts": 1565308800
}
```
####### Response
```
{
  "res": [
    {
      "ts": 1565247660.0,
      "H": 12020,
      "L": 12008.52,
      "O": 12007.36,
      "C": 12007.36,
      "V": 8.52312197
    }
  }
}
```
