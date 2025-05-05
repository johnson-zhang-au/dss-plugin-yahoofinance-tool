# Yahoo Finance Plugin for Dataiku DSS

This plugin provides access to Yahoo Finance market data directly within Dataiku DSS. It allows agents and workflows to retrieve financial information such as stock quotes, historical prices, options chains, market indices, financial statements, and news.

## Features

- **Real-time Stock Quotes**: Retrieve current market prices, changes, and trading volume
- **Historical Data**: Access historical price data with adjustable time periods and intervals
- **Options Chains**: Get information on available options, including strikes, premiums, and expiration dates
- **Company Information**: Access fundamental company data like sector, industry, and company descriptions
- **Market Indices**: Monitor major market indices like S&P 500, Dow Jones, and NASDAQ
- **Financial Statements**: Retrieve income statements, balance sheets, and cash flow statements
- **News and Sentiment**: Get the latest news related to stocks or the general market

## Installation

1. Download the plugin
2. In your Dataiku DSS instance, go to Administration > Plugins > Upload plugin
3. Upload the plugin ZIP file
4. Restart the instance if prompted

## Configuration

The plugin supports the following configuration options:

- **Cache Expiry** (in minutes): Controls how long to cache results to reduce API calls and avoid rate limiting
- **Logging Level**: Sets the verbosity of logs (DEBUG, INFO, WARNING, ERROR, CRITICAL)

## Important Note

This plugin uses the open-source `yfinance` Python package, which provides **unofficial** access to Yahoo Finance data through web scraping. It is not an official API service from Yahoo and does not require or support API keys. 

* yfinance GitHub repository: https://github.com/ranaroussi/yfinance
* Documentation: https://pypi.org/project/yfinance/

Please note that using this data may be subject to Yahoo Finance's terms of service, usage limits, and rate limiting. Yahoo Finance does not offer an official API for public use, and this package is a community-maintained solution.

## Agent Tool Functions

The plugin provides the following functions via the Yahoo Finance agent tool:

### 1. Get Stock Quote (`quote`)

Retrieves the current stock quote including price, change, volume, and other key metrics.

**Sample Usage:**
```json
{
  "action": "quote",
  "ticker": "AAPL"
}
```

### 2. Get Historical Price Data (`history`)

Fetches historical price data for a stock with various time periods and intervals.

**Sample Usage:**
```json
{
  "action": "history",
  "ticker": "MSFT",
  "period": "6mo",
  "interval": "1d"
}
```

### 3. Get Enhanced Historical Data (`stock_history`)

Similar to `history` but provides enhanced outputs including price change calculations and formatted tables.

**Sample Usage:**
```json
{
  "action": "stock_history",
  "symbol": "TSLA",
  "period": "1y",
  "interval": "1wk"
}
```

### 4. Get Options Chain Data (`options`)

Retrieves options chain data for calls, puts, or both.

**Sample Usage:**
```json
{
  "action": "options",
  "ticker": "AMZN",
  "optionType": "call",
  "expirationDate": "2023-12-15"
}
```

### 5. Get Company Information (`info`)

Retrieves fundamental company information and profile.

**Sample Usage:**
```json
{
  "action": "info",
  "ticker": "NVDA"
}
```

### 6. Get Market Indices (`market_indices`)

Retrieves current data for major market indices.

**Sample Usage:**
```json
{
  "action": "market_indices",
  "indices": ["^GSPC", "^DJI", "^IXIC"]
}
```

### 7. Get Financial Statements (`company_financials`)

Retrieves various financial statements for a company.

**Sample Usage:**
```json
{
  "action": "company_financials",
  "symbol": "GOOG",
  "statement": "income",
  "period": "annual"
}
```

### 8. Get Stock News (`stock_news`)

Retrieves recent news articles related to a stock or the general market.

**Sample Usage:**
```json
{
  "action": "stock_news",
  "symbol": "META",
  "count": 5
}
```

## Input Parameter Details

| Parameter | Type | Description | Example Values |
|-----------|------|-------------|----------------|
| `action` | string | The action to perform | "quote", "history", "options", "info", "market_indices", "stock_history", "company_financials", "stock_news" |
| `ticker` / `symbol` | string | The stock ticker symbol | "AAPL", "MSFT", "GOOG" |
| `period` | string | Time period for historical data | "1d", "5d", "1mo", "3mo", "6mo", "1y", "2y", "5y", "10y", "ytd", "max" |
| `interval` | string | Data interval for historical data | "1m", "2m", "5m", "15m", "30m", "60m", "90m", "1h", "1d", "5d", "1wk", "1mo", "3mo" |
| `optionType` | string | Option type for options data | "call", "put" |
| `expirationDate` | string | Expiration date for options | "YYYY-MM-DD" format |
| `indices` | array | List of index symbols | ["^GSPC", "^DJI", "^IXIC"] |
| `statement` | string | Financial statement type | "income", "balance", "cash", "all" |
| `count` | integer | Number of news items to retrieve | 1-10 |

## Example Response Formats

### Stock Quote Response

```json
{
  "output": {
    "symbol": "AAPL",
    "price": 178.72,
    "change": 1.23,
    "changePercent": 0.69,
    "previousClose": 177.49,
    "open": 177.52,
    "dayHigh": 179.43,
    "dayLow": 177.13,
    "volume": 59482910,
    "marketCap": 2765432000000,
    "timestamp": "2023-11-09T15:30:45.123456"
  }
}
```

### Market Indices Response

```json
{
  "output": {
    "indices": [
      {
        "symbol": "^GSPC",
        "name": "S&P 500",
        "price": 4508.24,
        "change": -23.17,
        "changePercent": -0.51,
        "previousClose": 4531.41,
        "open": 4529.53,
        "dayHigh": 4538.26,
        "dayLow": 4496.41,
        "timestamp": "2023-11-09T15:30:45.123456"
      },
      // More indices...
    ],
    "formatted_output": "S&P 500\nPrice: 4,508.24\nChange: -23.17 (-0.51%)\nPrevious Close: 4,531.41\nDay Range: 4,496.41 - 4,538.26\n\nDow Jones Industrial Average\n..."
  }
}
```

## Error Handling

The plugin provides detailed error messages and logging. Common errors include:

- Invalid ticker symbols
- Network issues accessing Yahoo Finance
- Invalid parameter combinations
- Rate limiting from Yahoo Finance (the plugin includes caching to minimize this)

Error responses include an explanation and logs provide additional context.

## Dependencies

This plugin relies on the following Python libraries:

- yfinance >= 0.2.12
- pandas >= 1.3.0 
- requests >= 2.25.0
- numpy >= 1.20.0

## Limitations

- Yahoo Finance may impose rate limits which may affect high-frequency usage
- Data may be delayed (typically 15-20 minutes for market data)
- Extended historical data may not be available for all securities
- News content provides summaries only, not full article text
- The yfinance package is an unofficial tool and not affiliated with Yahoo Finance
- Data availability and format can change without notice

## License

This plugin is released under the Apache Software License.

## Support

For issues, feature requests, or contributions, please contact the plugin author or open an issue on the repository. 