# This file is the implementation of custom agent tool my-yahoofinance-tool
from dataiku.llm.agent_tools import BaseAgentTool
import yfinance as yf
import pandas as pd
import numpy as np
import json
import time
from datetime import datetime, timedelta
from utils.logging import logger

class CustomAgentTool(BaseAgentTool):
    def set_config(self, config, plugin_config):
        self.config = config
        self.cache = {}
        self.cache_timestamps = {}
        self.cache_expiry = config.get("cache_expiry", 5) * 60  # Convert to seconds
        
        # Set up logging
        self.setup_logging()
        
    def setup_logging(self):
        """
        Sets up the logging level using the logger.
        """
        # Get the logging level from the configuration, default to INFO
        logging_level = self.config.get("logging_level", "INFO")

        try:
            # Set the logging level dynamically
            logger.set_level(logging_level)
            logger.info(f"Yahoo Finance Tool: Logging initialized with level: {logging_level}")
        except ValueError as e:
            # Handle invalid logging levels
            logger.error(f"Yahoo Finance Tool: Invalid logging level '{logging_level}': {str(e)}")
            raise

    def get_descriptor(self, tool):
        logger.debug("Generating descriptor for the Yahoo Finance tool.")
        return {
            "description": "Get financial data from Yahoo Finance. You can retrieve stock quotes, historical data, options data, company information, and news for a given ticker symbol.",
            "inputSchema": {
                "$id": "https://example.com/agents/tools/yahoofinance/input",
                "title": "Input for the Yahoo Finance tool",
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "description": "The action to perform. Options: quote (get current stock price), stock_history (get historical price data with analysis and formatting), options (get options chain data), info (get company information), market_indices (get market index data), company_financials (get financial statements), stock_news (get latest news)",
                        "enum": ["quote", "stock_history", "options", "info", "market_indices", "company_financials", "stock_news"]
                    },
                    "ticker": {
                        "type": "string",
                        "description": "The ticker symbol to query (e.g., AAPL for Apple Inc.)"
                    },
                    "period": {
                        "type": "string",
                        "description": "Period for historical data: 1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max",
                        "enum": ["1d", "5d", "1mo", "3mo", "6mo", "1y", "2y", "5y", "10y", "ytd", "max"]
                    },
                    "interval": {
                        "type": "string",
                        "description": "Interval for historical data: 1m, 2m, 5m, 15m, 30m, 60m, 90m, 1h, 1d, 5d, 1wk, 1mo, 3mo",
                        "enum": ["1m", "2m", "5m", "15m", "30m", "60m", "90m", "1h", "1d", "5d", "1wk", "1mo", "3mo"]
                    },
                    "optionType": {
                        "type": "string",
                        "description": "Option type: call or put",
                        "enum": ["call", "put"]
                    },
                    "expirationDate": {
                        "type": "string",
                        "description": "Expiration date for options in YYYY-MM-DD format"
                    },
                    "indices": {
                        "type": "array",
                        "items": {
                            "type": "string"
                        },
                        "description": "List of market indices to fetch (e.g., ['^GSPC', '^DJI', '^IXIC'] for S&P 500, Dow Jones, and NASDAQ)"
                    },
                    "symbol": {
                        "type": "string",
                        "description": "Stock ticker symbol (identical to 'ticker', provided for compatibility)"
                    },
                    "statement": {
                        "type": "string",
                        "description": "Financial statement type to retrieve",
                        "enum": ["income", "balance", "cash", "all"]
                    },
                    "count": {
                        "type": "integer",
                        "description": "Number of items to retrieve (e.g., news articles)",
                        "minimum": 1,
                        "maximum": 10
                    }
                },
                "required": ["action"]
            }
        }

    def invoke(self, input, trace):
        args = input["input"]
        action = args["action"]
        
        logger.info(f"Invoking action: {action}")
        logger.debug(f"Input arguments: {args}")
        
        cache_key = json.dumps(args, sort_keys=True)
        current_time = time.time()
        
        # Check if result is in cache and not expired
        if cache_key in self.cache and current_time - self.cache_timestamps.get(cache_key, 0) < self.cache_expiry:
            logger.debug(f"Using cached data for action: {action}")
            return self.cache[cache_key]
        
        # Log the request
        logger.info(f"Fetching data for action: {action}")
        
        try:
            # Normalize ticker/symbol inputs
            symbol = args.get("symbol", args.get("ticker"))
            
            # Handle the different actions
            if action == "quote":
                if not symbol:
                    raise ValueError("Missing required parameter: symbol or ticker")
                logger.debug(f"Processing quote request for {symbol}")
                result = self._get_stock_quote(symbol)
            elif action == "stock_history":
                if not symbol:
                    raise ValueError("Missing required parameter: symbol or ticker")
                period = args.get("period", "1mo")
                interval = args.get("interval", "1d")
                
                logger.debug(f"Processing stock_history request for {symbol} with period {period} and interval {interval}")
                result = self._get_stock_history(symbol, period, interval)
            elif action == "options":
                if not symbol:
                    raise ValueError("Missing required parameter: symbol or ticker")
                option_type = args.get("optionType")
                expiration_date = args.get("expirationDate")
                logger.debug(f"Processing options request for {symbol} with type {option_type} and expiration {expiration_date}")
                result = self._get_stock_options(symbol, option_type, expiration_date)
            elif action == "info":
                if not symbol:
                    raise ValueError("Missing required parameter: symbol or ticker")
                logger.debug(f"Processing info request for {symbol}")
                result = self._get_company_info(symbol)
            elif action == "market_indices":
                indices = args.get("indices", ["^GSPC", "^DJI", "^IXIC"])  # Default: S&P 500, Dow Jones, NASDAQ
                logger.debug(f"Processing market indices request for {indices}")
                result = self._get_market_indices(indices)
            elif action == "company_financials":
                if not symbol:
                    raise ValueError("Missing required parameter: symbol or ticker")
                statement = args.get("statement", "income")
                period = args.get("period", "annual")
                logger.debug(f"Processing company financials request for {symbol}, statement type: {statement}, period: {period}")
                result = self._get_company_financials(symbol, statement, period)
            elif action == "stock_news":
                # Symbol is optional for general market news
                count = min(args.get("count", 5), 10)  # Default 5, max 10
                logger.debug(f"Processing stock news request for {symbol if symbol else 'general market'}, count: {count}")
                result = self._get_stock_news(symbol, count)
            else:
                error_msg = f"Invalid action: {action}"
                logger.error(error_msg)
                return {
                    "error": error_msg,
                    "sources": [{
                        "toolCallDescription": f"Error: Invalid action {action}"
                    }]
                }
                
            # Store in cache
            logger.debug(f"Caching results for action: {action} (cache expires in {self.cache_expiry/60} minutes)")
            self.cache[cache_key] = result
            self.cache_timestamps[cache_key] = current_time
            
            return result
            
        except Exception as e:
            error_msg = f"Error fetching data for action {action}: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return {
                "error": f"Failed to fetch data for action {action}: {str(e)}",
                "sources": [{
                    "toolCallDescription": f"Error fetching data for action {action}"
                }]
            }
    
    def _get_stock_quote(self, symbol):
        """Get current stock quote"""
        logger.debug(f"Getting quote for {symbol}")
        stock = yf.Ticker(symbol)
        
        # Get the last price
        try:
            quote = stock.info
            logger.debug(f"Retrieved raw quote data for {symbol}")
            
            # Extract the most relevant quote information
            relevant_data = {
                "symbol": symbol,
                "price": quote.get("currentPrice", quote.get("regularMarketPrice")),
                "change": quote.get("regularMarketChange"),
                "changePercent": quote.get("regularMarketChangePercent"),
                "previousClose": quote.get("regularMarketPreviousClose"),
                "open": quote.get("regularMarketOpen"),
                "dayHigh": quote.get("regularMarketDayHigh"),
                "dayLow": quote.get("regularMarketDayLow"),
                "volume": quote.get("regularMarketVolume"),
                "marketCap": quote.get("marketCap"),
                "timestamp": datetime.now().isoformat()
            }
            
            logger.info(f"Successfully retrieved quote for {symbol}")
            
            return {
                "output": relevant_data,
                "sources": [{
                    "toolCallDescription": f"Retrieved current quote for {symbol}"
                }]
            }
        except Exception as e:
            logger.error(f"Error getting quote for {symbol}: {str(e)}", exc_info=True)
            raise Exception(f"Error getting quote for {symbol}: {str(e)}")
    
    def _get_stock_history(self, symbol, period="1mo", interval="1d"):
        """
        Get historical price data
        
        Args:
            symbol (str): Stock ticker symbol
            period (str): Time period (1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max)
            interval (str): Data interval (1m, 2m, 5m, 15m, 30m, 60m, 90m, 1h, 1d, 5d, 1wk, 1mo, 3mo)
            
        Returns:
            Historical stock data for the specified symbol and time range
        """
        logger.debug(f"Getting history for {symbol} with period {period} and interval {interval}")
        stock = yf.Ticker(symbol)
        
        try:
            # Get stock info for company name and currency
            info = stock.info
            currency = info.get("currency", "USD")
            company_name = info.get("shortName", symbol)
            
            # Get historical data
            hist = stock.history(period=period, interval=interval)
            logger.debug(f"Retrieved {len(hist)} historical data points for {symbol}")
            
            if hist.empty:
                return {
                    "output": {
                        "symbol": symbol,
                        "name": company_name,
                        "message": "No historical data available for this period and interval"
                    },
                    "sources": [{
                        "toolCallDescription": f"No historical data available for {symbol} with period {period} and interval {interval}"
                    }]
                }
            
            # Convert to a serializable format
            hist_dict = []
            for index, row in hist.iterrows():
                hist_dict.append({
                    "date": index.strftime('%Y-%m-%d %H:%M:%S'),
                    "open": row.get("Open"),
                    "high": row.get("High"),
                    "low": row.get("Low"),
                    "close": row.get("Close"),
                    "volume": row.get("Volume", 0)
                })
            
            # Standard output
            output = {
                "symbol": symbol,
                "period": period,
                "interval": interval,
                "data": hist_dict
            }
            
            # Add enhanced information if requested
            if not hist.empty:
                # Calculate price change over the period
                first_close = hist_dict[0]["close"] if hist_dict else None
                last_close = hist_dict[-1]["close"] if hist_dict else None
                price_change = None
                price_change_pct = None
                
                if first_close is not None and last_close is not None:
                    price_change = last_close - first_close
                    if first_close > 0:
                        price_change_pct = (price_change / first_close) * 100
                
                # Create time range description
                start_date = hist.index[0].strftime('%m/%d/%Y') if not hist.empty else "N/A"
                end_date = hist.index[-1].strftime('%m/%d/%Y') if not hist.empty else "N/A"
                time_range = f"{start_date} to {end_date}"
                
                # Create a formatted table for human-readable display
                formatted_output = f"Historical data for {company_name} ({symbol}) ({period}, {interval} intervals)\n"
                formatted_output += f"Currency: {currency}\n"
                formatted_output += f"Trading Period: {time_range}\n\n"
                
                # Table header
                formatted_output += f"{'Date':<11} | {'Open':<9} | {'High':<9} | {'Low':<9} | {'Close':<9} | {'Volume':<12}\n"
                formatted_output += f"{'-' * 11}|{'-' * 11}|{'-' * 11}|{'-' * 11}|{'-' * 11}|{'-' * 14}\n"
                
                # Table rows (limit to 7 rows for readability)
                display_rows = min(len(hist_dict), 7)
                sample_indices = []
                
                if display_rows <= 7:
                    sample_indices = range(display_rows)
                else:
                    # Sample evenly distributed rows for longer histories
                    step = len(hist_dict) // 7
                    for i in range(0, len(hist_dict), step):
                        if len(sample_indices) < 7:
                            sample_indices.append(i)
                
                for i in sample_indices:
                    if i < len(hist_dict):
                        row = hist_dict[i]
                        date = row["date"].split()[0]  # Just the date part
                        open_price = f"${row['open']:.2f}" if row["open"] else "N/A"
                        high = f"${row['high']:.2f}" if row["high"] else "N/A"
                        low = f"${row['low']:.2f}" if row["low"] else "N/A"
                        close = f"${row['close']:.2f}" if row["close"] else "N/A"
                        volume = f"{row['volume']:,}" if row["volume"] else "N/A"
                        
                        formatted_output += f"{date:<10} | {open_price:<9} | {high:<9} | {low:<9} | {close:<9} | {volume:<12}\n"
                
                # Add price change information
                if price_change is not None and price_change_pct is not None:
                    change_sign = "+" if price_change > 0 else ""
                    formatted_output += f"\nPrice Change: {change_sign}${price_change:.2f} ({change_sign}{price_change_pct:.2f}%)"
                
                # Add enhanced data to output
                output.update({
                    "name": company_name,
                    "currency": currency,
                    "time_range": time_range,
                    "price_change": price_change,
                    "price_change_percent": price_change_pct,
                    "formatted_output": formatted_output
                })
            
            logger.info(f"Successfully retrieved history for {symbol}")
            
            return {
                "output": output,
                "sources": [{
                    "toolCallDescription": f"Retrieved historical data for {symbol} with period {period} and interval {interval}"
                }]
            }
        except Exception as e:
            logger.error(f"Error getting history for {symbol}: {str(e)}", exc_info=True)
            raise Exception(f"Error getting history for {symbol}: {str(e)}")
    
    def _get_stock_options(self, symbol, option_type=None, expiration_date=None):
        """Get options chain data"""
        logger.debug(f"Getting options for {symbol} with type {option_type} and expiration {expiration_date}")
        stock = yf.Ticker(symbol)
        
        try:
            # Get available expiration dates if none provided
            expirations = stock.options
            logger.debug(f"Available expirations for {symbol}: {expirations}")
            
            if not expirations:
                logger.info(f"No options data available for {symbol}")
                return {
                    "output": {
                        "symbol": symbol,
                        "message": "No options data available for this ticker"
                    },
                    "sources": [{
                        "toolCallDescription": f"No options available for {symbol}"
                    }]
                }
            
            # Use first available expiration if none specified
            if not expiration_date:
                expiration_date = expirations[0]
                logger.debug(f"Using first available expiration date: {expiration_date}")
            elif expiration_date not in expirations:
                logger.warning(f"Requested expiration date {expiration_date} not available for {symbol}")
                return {
                    "output": {
                        "symbol": symbol,
                        "availableExpirations": expirations,
                        "message": f"Expiration date {expiration_date} not available. Please choose from available dates."
                    },
                    "sources": [{
                        "toolCallDescription": f"Invalid expiration date for {symbol}"
                    }]
                }
            
            # Get options data
            logger.debug(f"Fetching option chain for {symbol} with expiration {expiration_date}")
            options = stock.option_chain(expiration_date)
            
            # Filter by option type if specified
            if option_type == "call":
                options_data = options.calls
                option_type_str = "Calls"
                logger.debug(f"Filtering for call options only ({len(options_data)} contracts)")
            elif option_type == "put":
                options_data = options.puts
                option_type_str = "Puts"
                logger.debug(f"Filtering for put options only ({len(options_data)} contracts)")
            else:
                # Return both if not specified
                calls_data = self._process_options_data(options.calls)
                puts_data = self._process_options_data(options.puts)
                logger.debug(f"Returning both call ({len(calls_data)}) and put ({len(puts_data)}) options")
                
                return {
                    "output": {
                        "symbol": symbol,
                        "expirationDate": expiration_date,
                        "calls": calls_data[:10],  # Limit to 10 strikes
                        "puts": puts_data[:10]     # Limit to 10 strikes
                    },
                    "sources": [{
                        "toolCallDescription": f"Retrieved options chain for {symbol} expiring {expiration_date}"
                    }]
                }
            
            # Process and return the filtered data
            options_data = self._process_options_data(options_data)
            logger.info(f"Successfully retrieved {option_type_str} options for {symbol}")
            
            return {
                "output": {
                    "symbol": symbol,
                    "expirationDate": expiration_date,
                    "optionType": option_type_str,
                    "data": options_data[:10]  # Limit to 10 strikes
                },
                "sources": [{
                    "toolCallDescription": f"Retrieved {option_type_str} options for {symbol} expiring {expiration_date}"
                }]
            }
        except Exception as e:
            logger.error(f"Error getting options for {symbol}: {str(e)}", exc_info=True)
            raise Exception(f"Error getting options for {symbol}: {str(e)}")
    
    def _process_options_data(self, df):
        """Helper to process options dataframe into serializable format"""
        if df.empty:
            logger.debug("Options dataframe is empty")
            return []
            
        options_list = []
        for _, row in df.iterrows():
            options_list.append({
                "strike": row.get("strike"),
                "lastPrice": row.get("lastPrice"),
                "bid": row.get("bid"),
                "ask": row.get("ask"),
                "change": row.get("change"),
                "percentChange": row.get("percentChange"),
                "volume": row.get("volume"),
                "openInterest": row.get("openInterest"),
                "impliedVolatility": row.get("impliedVolatility")
            })
        
        logger.debug(f"Processed {len(options_list)} option contracts")
        return options_list
    
    def _get_company_info(self, symbol):
        """Get company information"""
        logger.debug(f"Getting company info for {symbol}")
        stock = yf.Ticker(symbol)
        
        try:
            info = stock.info
            logger.debug(f"Retrieved raw company info for {symbol}")
            
            # Extract the most relevant company information
            relevant_info = {
                "symbol": symbol,
                "name": info.get("shortName"),
                "industry": info.get("industry"),
                "sector": info.get("sector"),
                "country": info.get("country"),
                "website": info.get("website"),
                "market": info.get("market"),
                "currency": info.get("currency"),
                "exchange": info.get("exchange"),
                "marketCap": info.get("marketCap"),
                "employees": info.get("fullTimeEmployees"),
                "description": info.get("longBusinessSummary")
            }
            
            logger.info(f"Successfully retrieved company info for {symbol}")
            
            return {
                "output": relevant_info,
                "sources": [{
                    "toolCallDescription": f"Retrieved company information for {symbol}"
                }]
            }
        except Exception as e:
            logger.error(f"Error getting company info for {symbol}: {str(e)}", exc_info=True)
            raise Exception(f"Error getting company info for {symbol}: {str(e)}")

    def _get_market_indices(self, indices=None):
        """
        Gets current market data from Yahoo Finance for major indices.
        
        Args:
            indices (list): List of index symbols to fetch (default: ["^GSPC", "^DJI", "^IXIC"])
            
        Returns:
            Current market data for the specified indices
        """
        logger.debug(f"Getting market indices data for {indices}")
        
        # Default indices if none provided: S&P 500, Dow Jones, NASDAQ
        if not indices:
            indices = ["^GSPC", "^DJI", "^IXIC"]
        
        indices_data = []
        
        try:
            for index_symbol in indices:
                index = yf.Ticker(index_symbol)
                quote = index.info
                
                # Map index symbols to names
                index_names = {
                    "^GSPC": "S&P 500",
                    "^DJI": "Dow Jones Industrial Average",
                    "^IXIC": "NASDAQ Composite",
                    "^RUT": "Russell 2000",
                    "^VIX": "CBOE Volatility Index",
                    "^FTSE": "FTSE 100",
                    "^N225": "Nikkei 225",
                    "^HSI": "Hang Seng Index"
                }
                
                name = index_names.get(index_symbol, quote.get("shortName", index_symbol))
                
                # Extract relevant data
                index_data = {
                    "symbol": index_symbol,
                    "name": name,
                    "price": quote.get("regularMarketPrice"),
                    "change": quote.get("regularMarketChange"),
                    "changePercent": quote.get("regularMarketChangePercent"),
                    "previousClose": quote.get("regularMarketPreviousClose"),
                    "open": quote.get("regularMarketOpen"),
                    "dayHigh": quote.get("regularMarketDayHigh"),
                    "dayLow": quote.get("regularMarketDayLow"),
                    "timestamp": datetime.now().isoformat()
                }
                
                indices_data.append(index_data)
                logger.debug(f"Retrieved data for index {index_symbol} ({name})")
            
            # Create a formatted output string for human-readable display
            formatted_output = ""
            for index in indices_data:
                name = index["name"]
                price = f"{index['price']:,.2f}" if index['price'] else "N/A"
                change = f"{index['change']:,.2f}" if index['change'] else "N/A"
                change_pct = f"{index['changePercent']:.2f}%" if index['changePercent'] else "N/A"
                prev_close = f"{index['previousClose']:,.2f}" if index['previousClose'] else "N/A"
                day_low = f"{index['dayLow']:,.2f}" if index['dayLow'] else "N/A"
                day_high = f"{index['dayHigh']:,.2f}" if index['dayHigh'] else "N/A"
                
                change_sign = "+" if index['change'] and index['change'] > 0 else ""
                
                formatted_output += f"{name}\n"
                formatted_output += f"Price: {price}\n"
                formatted_output += f"Change: {change_sign}{change} ({change_sign}{change_pct})\n"
                formatted_output += f"Previous Close: {prev_close}\n"
                formatted_output += f"Day Range: {day_low} - {day_high}\n\n"
            
            logger.info(f"Successfully retrieved data for {len(indices_data)} market indices")
            
            return {
                "output": {
                    "indices": indices_data,
                    "formatted_output": formatted_output.strip()
                },
                "sources": [{
                    "toolCallDescription": f"Retrieved current market data for {len(indices_data)} indices"
                }]
            }
            
        except Exception as e:
            logger.error(f"Error getting market indices data: {str(e)}", exc_info=True)
            raise Exception(f"Error getting market indices data: {str(e)}")
    
    def _get_company_financials(self, symbol, statement_type="income", period="annual"):
        """
        Gets financial statement data for a company from Yahoo Finance.
        
        Args:
            symbol (str): Stock ticker symbol
            statement_type (str): Financial statement type ("income", "balance", "cash", or "all")
            period (str): Time period ("annual" or "quarterly")
            
        Returns:
            Financial statement data for the specified company
        """
        logger.debug(f"Getting company financials for {symbol}, statement type: {statement_type}, period: {period}")
        stock = yf.Ticker(symbol)
        
        try:
            # Get stock info for company name and currency
            info = stock.info
            currency = info.get("currency", "USD")
            company_name = info.get("shortName", symbol)
            
            # Validate statement type
            valid_types = ["income", "balance", "cash", "all"]
            if statement_type not in valid_types:
                raise ValueError(f"Invalid statement type: {statement_type}. Must be one of {valid_types}")
            
            # Validate period
            valid_periods = ["annual", "quarterly"]
            if period not in valid_periods:
                raise ValueError(f"Invalid period: {period}. Must be one of {valid_periods}")
            
            financials = {}
            formatted_output = f"Financial Statements for {company_name} ({symbol})\nCurrency: {currency}\n\n"
            
            # Get the requested financial statements
            if statement_type == "income" or statement_type == "all":
                if period == "annual":
                    income_stmt = stock.income_stmt
                    period_label = "Annual"
                else:
                    income_stmt = stock.quarterly_income_stmt
                    period_label = "Quarterly"
                
                # Process income statement
                if not income_stmt.empty:
                    income_data = self._process_financial_statement(income_stmt)
                    financials["income_statement"] = income_data
                    
                    # Format income statement for human-readable output
                    formatted_output += f"=== {period_label} Income Statement ===\n"
                    for item, values in income_data.items():
                        formatted_output += f"{item}:\n"
                        for date, value in values.items():
                            if value is not None:
                                value_str = f"{value:,.0f}" if abs(value) >= 1 else f"{value:.2f}"
                                formatted_output += f"  {date}: {value_str}\n"
                        formatted_output += "\n"
            
            if statement_type == "balance" or statement_type == "all":
                if period == "annual":
                    balance_sheet = stock.balance_sheet
                    period_label = "Annual"
                else:
                    balance_sheet = stock.quarterly_balance_sheet
                    period_label = "Quarterly"
                
                # Process balance sheet
                if not balance_sheet.empty:
                    balance_data = self._process_financial_statement(balance_sheet)
                    financials["balance_sheet"] = balance_data
                    
                    # Format balance sheet for human-readable output
                    formatted_output += f"=== {period_label} Balance Sheet ===\n"
                    for item, values in balance_data.items():
                        formatted_output += f"{item}:\n"
                        for date, value in values.items():
                            if value is not None:
                                value_str = f"{value:,.0f}" if abs(value) >= 1 else f"{value:.2f}"
                                formatted_output += f"  {date}: {value_str}\n"
                        formatted_output += "\n"
            
            if statement_type == "cash" or statement_type == "all":
                if period == "annual":
                    cash_flow = stock.cashflow
                    period_label = "Annual"
                else:
                    cash_flow = stock.quarterly_cashflow
                    period_label = "Quarterly"
                
                # Process cash flow statement
                if not cash_flow.empty:
                    cash_data = self._process_financial_statement(cash_flow)
                    financials["cash_flow"] = cash_data
                    
                    # Format cash flow statement for human-readable output
                    formatted_output += f"=== {period_label} Cash Flow Statement ===\n"
                    for item, values in cash_data.items():
                        formatted_output += f"{item}:\n"
                        for date, value in values.items():
                            if value is not None:
                                value_str = f"{value:,.0f}" if abs(value) >= 1 else f"{value:.2f}"
                                formatted_output += f"  {date}: {value_str}\n"
                        formatted_output += "\n"
            
            if not financials:
                return {
                    "output": {
                        "symbol": symbol,
                        "name": company_name,
                        "message": "No financial statement data available"
                    },
                    "sources": [{
                        "toolCallDescription": f"No financial statement data available for {symbol}"
                    }]
                }
            
            logger.info(f"Successfully retrieved {statement_type} financial statements for {symbol}")
            
            return {
                "output": {
                    "symbol": symbol,
                    "name": company_name,
                    "currency": currency,
                    "statement_type": statement_type,
                    "period": period,
                    "financials": financials,
                    "formatted_output": formatted_output
                },
                "sources": [{
                    "toolCallDescription": f"Retrieved {statement_type} financial statements for {symbol} ({period})"
                }]
            }
            
        except Exception as e:
            logger.error(f"Error getting company financials for {symbol}: {str(e)}", exc_info=True)
            raise Exception(f"Error getting company financials for {symbol}: {str(e)}")
    
    def _process_financial_statement(self, statement_df):
        """Helper method to process financial statement DataFrames into dictionaries"""
        result = {}
        
        try:
            # Convert the DataFrame to a more serializable format
            for index, row in statement_df.iterrows():
                item_name = index
                item_values = {}
                
                for column in statement_df.columns:
                    date_str = column.strftime('%Y-%m-%d')
                    value = row[column]
                    # Convert numpy values to Python native types
                    if isinstance(value, (np.integer, np.floating)):
                        value = float(value)
                    elif pd.isna(value):
                        value = None
                    
                    item_values[date_str] = value
                
                result[item_name] = item_values
                
            return result
        except Exception as e:
            logger.error(f"Error processing financial statement: {str(e)}", exc_info=True)
            return {}
    
    def _get_stock_news(self, symbol=None, count=5):
        """
        Gets recent news articles and sentiment analysis for a stock.
        
        Args:
            symbol (str, optional): Stock ticker symbol (omit for general market news)
            count (int): Number of news items to retrieve (max 10)
            
        Returns:
            Recent news articles related to the specified stock or general market
        """
        logger.debug(f"Getting stock news for {symbol if symbol else 'general market'}, count: {count}")
        
        try:
            if symbol:
                # Get news for specific ticker
                stock = yf.Ticker(symbol)
                news = stock.news
                if news:
                    logger.debug(f"Retrieved {len(news)} news items for {symbol}")
                company_name = stock.info.get("shortName", symbol)
                news_source = f"{company_name} ({symbol})"
            else:
                # Get general market news
                market = yf.Ticker("^GSPC")  # S&P 500 as a proxy for market news
                news = market.news
                if news:
                    logger.debug(f"Retrieved {len(news)} general market news items")
                news_source = "General Market News"
            
            # Limit to requested count
            count = min(count, 10)  # Cap at 10
            if news and len(news) > count:
                news = news[:count]
            
            if not news:
                return {
                    "output": {
                        "source": news_source,
                        "message": "No news articles available"
                    },
                    "sources": [{
                        "toolCallDescription": f"No news articles available for {news_source}"
                    }]
                }
            
            # Process news items
            processed_news = []
            for item in news:
                # Extract the publisher
                publisher = item.get("publisher", "Unknown Source")
                
                # Format the timestamp
                timestamp = item.get("providerPublishTime", 0)
                if timestamp:
                    date = datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")
                else:
                    date = "Unknown Date"
                
                # Process the news item
                news_item = {
                    "title": item.get("title", "No Title"),
                    "publisher": publisher,
                    "link": item.get("link", ""),
                    "publish_date": date,
                    "type": item.get("type", ""),
                    "related_tickers": item.get("relatedTickers", []),
                    "summary": item.get("summary", "No summary available")
                }
                
                processed_news.append(news_item)
            
            # Create formatted news output
            formatted_output = f"=== Latest News for {news_source} ===\n\n"
            
            for i, item in enumerate(processed_news, 1):
                formatted_output += f"{i}. {item['title']}\n"
                formatted_output += f"   Source: {item['publisher']} | Date: {item['publish_date']}\n"
                
                if item['summary']:
                    # Truncate long summaries
                    summary = item['summary']
                    if len(summary) > 300:
                        summary = summary[:297] + "..."
                    formatted_output += f"   Summary: {summary}\n"
                
                if item['related_tickers']:
                    formatted_output += f"   Related Tickers: {', '.join(item['related_tickers'])}\n"
                
                formatted_output += f"   Link: {item['link']}\n\n"
            
            logger.info(f"Successfully retrieved {len(processed_news)} news articles for {news_source}")
            
            return {
                "output": {
                    "source": news_source,
                    "count": len(processed_news),
                    "news": processed_news,
                    "formatted_output": formatted_output
                },
                "sources": [{
                    "toolCallDescription": f"Retrieved {len(processed_news)} news articles for {news_source}"
                }]
            }
            
        except Exception as e:
            error_msg = f"Error getting news for {symbol if symbol else 'general market'}: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise Exception(error_msg)
