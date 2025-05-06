# Yahoo Finance Tool

This tool provides access to financial data from Yahoo Finance, including stock quotes, historical data, options data, company information, and market news.

## Features

- Get current stock quotes
- Retrieve historical price data with analysis
- Access options chain data
- Get company information and financial statements
- Fetch market indices data
- Retrieve latest stock news
- Get Fear & Greed Index
- Create visualizations for various data types

## Installation

1. Install the required packages:
```bash
pip install yfinance pandas numpy matplotlib
```

2. Configure the tool in your Dataiku instance.

## Usage

### Stock Quote
```python
{
    "action": "quote",
    "ticker": "AAPL"
}
```

### Historical Data
```python
{
    "action": "stock_history",
    "ticker": "AAPL",
    "period": "1mo",
    "interval": "1d"
}
```

### Options Data
```python
{
    "action": "options",
    "ticker": "AAPL",
    "optionType": "call",
    "expirationDate": "2024-04-19"
}
```

### Company Information
```python
{
    "action": "info",
    "ticker": "AAPL"
}
```

### Market Indices
```python
{
    "action": "market_indices",
    "indices": ["^GSPC", "^DJI", "^IXIC"]
}
```

### Company Financials
```python
{
    "action": "company_financials",
    "ticker": "AAPL",
    "statement": "income",
    "period": "annual"
}
```

### Stock News
```python
{
    "action": "stock_news",
    "ticker": "AAPL",
    "count": 5
}
```

### Fear & Greed Index
```python
{
    "action": "fear_greed"
}
```

The Fear & Greed Index is a market sentiment indicator that ranges from 0 to 100:
- 0-20: Extreme Fear
- 21-40: Fear
- 41-60: Neutral
- 61-80: Greed
- 81-100: Extreme Greed

### Visualization
```python
{
    "action": "visualize",
    "dataType": "stock_history",  # Options: stock_history, market_indices, financials, fear_greed
    "chartType": "line",         # Options: line, area, candlestick, bar, scatter
    "ticker": "AAPL",
    "period": "1mo",
    "metrics": ["close", "volume"]
}
```

The visualization feature supports different types of charts for various data:

1. Stock History:
   - Line/Area/Candlestick charts for price data
   - Volume bars on secondary axis
   - Multiple metrics (open, high, low, close, volume)

2. Market Indices:
   - Bar/Line charts comparing different indices
   - Current values for selected indices

3. Company Financials:
   - Bar/Line charts for financial metrics
   - Multiple metrics (e.g., Revenue, Net Income)
   - Annual/Quarterly data

4. Fear & Greed Index:
   - Line chart with sentiment bands
   - Color-coded zones for different sentiment levels
   - Current score marker

The visualization output includes:
- Base64-encoded PNG image of the chart
- Underlying data used to create the chart

## Notes

- All timestamps are in UTC
- Historical data is limited by Yahoo Finance's data availability
- Options data is only available for US stocks
- News articles are limited to 10 per request
- Visualizations are generated as PNG images with a resolution of 1200x800 pixels

## Error Handling

The tool includes comprehensive error handling for:
- Invalid ticker symbols
- Unavailable data
- API connection issues
- Invalid parameters

All errors are logged with appropriate context for debugging. 