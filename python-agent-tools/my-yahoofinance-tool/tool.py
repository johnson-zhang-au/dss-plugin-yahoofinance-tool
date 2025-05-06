# This file is the implementation of custom agent tool my-yahoofinance-tool
from dataiku.llm.agent_tools import BaseAgentTool
import yfinance as yf
import pandas as pd
import numpy as np
import json
import time
from datetime import datetime, timedelta
from utils.logging import logger
import requests
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import os
from matplotlib.figure import Figure
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
import dataiku
from dataiku.core.intercom import backend_json_call
import io

class CustomAgentTool(BaseAgentTool):
    def set_config(self, config, plugin_config):
        self.config = config
        self.cache = {}
        self.cache_timestamps = {}
        self.cache_expiry = config.get("cache_expiry", 5) * 60  # Convert to seconds
        
        # Get the managed folder from config
        self.charts_folder = dataiku.Folder(config.get("upload_folder"))
        self.public_url_prefix = config.get("public_url_prefix", "")
        
        # Set up logging
        self.setup_logging()
        
    def _setup_charts_folder(self, config):
        """
        Sets up the managed folder for storing charts.
        
        Args:
            config (dict): Tool configuration
            
        Returns:
            dataiku.Folder: Dataiku folder object for charts
        """
        folder_name = config.get("charts_folder_name", "yahoo_finance_charts")
        folder_type = config.get("charts_folder_type", "azure_blob")
        
        try:
            # Check if folder exists
            try:
                folder = dataiku.Folder(folder_name)
                logger.info(f"Using existing managed folder: {folder_name}")
                return folder
            except:
                # Create new folder if it doesn't exist
                logger.info(f"Creating new managed folder: {folder_name}")
                
                # Create folder configuration
                folder_config = {
                    "type": folder_type,
                    "params": {
                        "container": config.get("azure_container", "charts"),
                        "path": config.get("azure_path", "/"),
                        "connection": config.get("azure_connection", ""),
                        "publicAccess": True  # Ensure public access for image URLs
                    }
                }
                
                # Create the folder
                folder = dataiku.Folder.create(folder_name, folder_config)
                logger.info(f"Successfully created managed folder: {folder_name}")
                return folder
                
        except Exception as e:
            error_msg = f"Error setting up charts folder: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise Exception(error_msg)

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
                        "description": "The action to perform. Options: quote (get current stock price), stock_history (get historical price data with analysis and formatting), options (get options chain data), info (get company information), market_indices (get market index data), company_financials (get financial statements), stock_news (get latest news), fear_greed (get Fear & Greed Index), visualize (create charts for various data types)",
                        "enum": ["quote", "stock_history", "options", "info", "market_indices", "company_financials", "stock_news", "fear_greed", "visualize"]
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
                    },
                    "chartType": {
                        "type": "string",
                        "description": "Type of chart to create for visualization",
                        "enum": ["line", "area", "candlestick", "bar", "scatter"]
                    },
                    "dataType": {
                        "type": "string",
                        "description": "Type of data to visualize",
                        "enum": ["stock_history", "market_indices", "financials", "fear_greed"]
                    },
                    "metrics": {
                        "type": "array",
                        "items": {
                            "type": "string"
                        },
                        "description": "Specific metrics to include in the chart (e.g., ['close', 'volume'] for stock history)"
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
            elif action == "fear_greed":
                logger.debug("Processing Fear & Greed Index request")
                result = self._get_fear_greed_index()
            elif action == "visualize":
                data_type = args.get("dataType")
                chart_type = args.get("chartType", "line")
                metrics = args.get("metrics", [])
                
                if not data_type:
                    raise ValueError("Missing required parameter: dataType")
                
                logger.debug(f"Processing visualization request for {data_type} with chart type {chart_type}")
                result = self._create_visualization(data_type, chart_type, metrics, args)
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
                
                # Add enhanced data to output
                output.update({
                    "name": company_name,
                    "currency": currency,
                    "time_range": time_range,
                    "price_change": price_change,
                    "price_change_percent": price_change_pct
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
            
            logger.info(f"Successfully retrieved data for {len(indices_data)} market indices")
            
            return {
                "output": {
                    "indices": indices_data
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
            
            # Get the requested financial statements
            if statement_type == "income" or statement_type == "all":
                if period == "annual":
                    income_stmt = stock.income_stmt
                else:
                    income_stmt = stock.quarterly_income_stmt
                
                # Process income statement
                if not income_stmt.empty:
                    income_data = self._process_financial_statement(income_stmt)
                    financials["income_statement"] = income_data
            
            if statement_type == "balance" or statement_type == "all":
                if period == "annual":
                    balance_sheet = stock.balance_sheet
                else:
                    balance_sheet = stock.quarterly_balance_sheet
                
                # Process balance sheet
                if not balance_sheet.empty:
                    balance_data = self._process_financial_statement(balance_sheet)
                    financials["balance_sheet"] = balance_data
            
            if statement_type == "cash" or statement_type == "all":
                if period == "annual":
                    cash_flow = stock.cashflow
                else:
                    cash_flow = stock.quarterly_cashflow
                
                # Process cash flow statement
                if not cash_flow.empty:
                    cash_data = self._process_financial_statement(cash_flow)
                    financials["cash_flow"] = cash_data
            
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
                    "financials": financials
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
            
            logger.info(f"Successfully retrieved {len(processed_news)} news articles for {news_source}")
            
            return {
                "output": {
                    "source": news_source,
                    "count": len(processed_news),
                    "news": processed_news
                },
                "sources": [{
                    "toolCallDescription": f"Retrieved {len(processed_news)} news articles for {news_source}"
                }]
            }
            
        except Exception as e:
            error_msg = f"Error getting news for {symbol if symbol else 'general market'}: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise Exception(error_msg)

    def _get_fear_greed_index(self):
        """
        Gets the current Fear & Greed Index value from CNN Money.
        
        Returns:
            Current Fear & Greed Index data including score and rating
        """
        logger.debug("Getting Fear & Greed Index data")
        
        try:
            # CNN Money's Fear & Greed Index API endpoint
            url = "https://api.alternative.me/fng/"
            params = {
                "limit": 1,
                "format": "json"
            }
            
            response = requests.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            
            if not data.get("data"):
                raise ValueError("No Fear & Greed Index data available")
            
            # Get the most recent data point
            current_data = data["data"][0]
            
            # Get score and rating from the API response
            score = int(current_data["value"])
            rating = current_data["value_classification"]
            
            # Format timestamp
            timestamp = datetime.fromtimestamp(int(current_data["timestamp"])).strftime("%Y-%m-%d %H:%M:%S")
            
            logger.info(f"Successfully retrieved Fear & Greed Index: {score} ({rating})")
            
            return {
                "output": {
                    "score": score,
                    "rating": rating,
                    "timestamp": timestamp
                },
                "sources": [{
                    "toolCallDescription": "Retrieved current Fear & Greed Index from Alternative.me"
                }]
            }
        except Exception as e:
            error_msg = f"Error getting Fear & Greed Index: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise Exception(error_msg)

    def _create_visualization(self, data_type, chart_type, metrics, args):
        """
        Creates a visualization for the specified data type and chart type.
        
        Args:
            data_type (str): Type of data to visualize
            chart_type (str): Type of chart to create
            metrics (list): Specific metrics to include
            args (dict): Additional arguments for data retrieval
            
        Returns:
            Visualization image URL and data
        """
        logger.debug(f"Creating visualization for {data_type} with chart type {chart_type}")
        
        try:
            # Create figure with specific size and DPI
            fig = Figure(figsize=(12, 8), dpi=100)
            canvas = FigureCanvas(fig)
            
            # Get the data based on data type
            if data_type == "stock_history":
                if not args.get("symbol") and not args.get("ticker"):
                    raise ValueError("Missing required parameter: symbol or ticker")
                symbol = args.get("symbol", args.get("ticker"))
                period = args.get("period", "1mo")
                interval = args.get("interval", "1d")
                data = self._get_stock_history(symbol, period, interval)
                
                # Default metrics if none specified
                if not metrics:
                    metrics = ["close", "volume"]
                
                # Create subplots
                ax1 = fig.add_subplot(111)
                ax2 = ax1.twinx() if "volume" in metrics else None
                
                # Convert dates to datetime objects
                dates = [datetime.strptime(point["date"], "%Y-%m-%d %H:%M:%S") for point in data["output"]["data"]]
                
                # Plot price data
                for metric in metrics:
                    if metric in ["open", "high", "low", "close"]:
                        values = [point[metric] for point in data["output"]["data"]]
                        if chart_type == "candlestick":
                            # For candlestick, we need OHLC data
                            if metric == "close":
                                ax1.plot(dates, values, label=metric.capitalize(), linewidth=2)
                        else:
                            ax1.plot(dates, values, label=metric.capitalize(), linewidth=2)
                
                # Plot volume if requested
                if "volume" in metrics and ax2:
                    volume = [point["volume"] for point in data["output"]["data"]]
                    ax2.bar(dates, volume, alpha=0.3, color='gray', label='Volume')
                    ax2.set_ylabel('Volume')
                
                # Format the plot
                ax1.set_title(f"{symbol} Stock Price History")
                ax1.set_xlabel('Date')
                ax1.set_ylabel('Price')
                ax1.grid(True, alpha=0.3)
                ax1.legend(loc='upper left')
                
                # Format x-axis dates
                ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
                fig.autofmt_xdate()
                
                if ax2:
                    ax2.legend(loc='upper right')
                
                # Generate filename
                filename = f"{symbol}_{period}_{chart_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
                
            elif data_type == "market_indices":
                indices = args.get("indices", ["^GSPC", "^DJI", "^IXIC"])
                data = self._get_market_indices(indices)
                
                # Create bar chart
                ax = fig.add_subplot(111)
                names = [index["name"] for index in data["output"]["indices"]]
                values = [index["price"] for index in data["output"]["indices"]]
                
                if chart_type == "bar":
                    ax.bar(names, values)
                else:
                    ax.plot(names, values, marker='o')
                
                ax.set_title("Market Indices Comparison")
                ax.set_xlabel("Index")
                ax.set_ylabel("Value")
                ax.grid(True, alpha=0.3)
                plt.xticks(rotation=45)
                
                # Generate filename
                filename = f"market_indices_{chart_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
                
            elif data_type == "financials":
                if not args.get("symbol") and not args.get("ticker"):
                    raise ValueError("Missing required parameter: symbol or ticker")
                symbol = args.get("symbol", args.get("ticker"))
                statement = args.get("statement", "income")
                period = args.get("period", "annual")
                data = self._get_company_financials(symbol, statement, period)
                
                # Default metrics if none specified
                if not metrics:
                    metrics = ["Total Revenue", "Net Income"]
                
                ax = fig.add_subplot(111)
                
                # Plot each metric
                for metric in metrics:
                    if metric in data["output"]["financials"].get("income_statement", {}):
                        values = data["output"]["financials"]["income_statement"][metric]
                        dates = list(values.keys())
                        values = list(values.values())
                        
                        if chart_type == "bar":
                            ax.bar(dates, values, label=metric)
                        else:
                            ax.plot(dates, values, marker='o', label=metric)
                
                ax.set_title(f"{data['output']['name']} Financial Metrics")
                ax.set_xlabel("Date")
                ax.set_ylabel("Value")
                ax.grid(True, alpha=0.3)
                ax.legend()
                plt.xticks(rotation=45)
                
                # Generate filename
                filename = f"{symbol}_financials_{statement}_{chart_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
                
            elif data_type == "fear_greed":
                data = self._get_fear_greed_index()
                
                ax = fig.add_subplot(111)
                
                # Plot the score
                score = data["output"]["score"]
                timestamp = datetime.strptime(data["output"]["timestamp"], "%Y-%m-%d %H:%M:%S")
                
                # Add colored bands
                colors = ['red', 'orange', 'yellow', 'lightgreen', 'green']
                labels = ['Extreme Fear', 'Fear', 'Neutral', 'Greed', 'Extreme Greed']
                ranges = [(0, 20), (21, 40), (41, 60), (61, 80), (81, 100)]
                
                for (start, end), color, label in zip(ranges, colors, labels):
                    ax.axhspan(start, end, color=color, alpha=0.2, label=label)
                
                # Plot the score
                ax.plot([timestamp], [score], 'bo', markersize=10)
                ax.axhline(y=score, color='blue', linestyle='--', alpha=0.3)
                
                ax.set_title("Fear & Greed Index")
                ax.set_xlabel("Date")
                ax.set_ylabel("Score")
                ax.set_ylim(0, 100)
                ax.grid(True, alpha=0.3)
                ax.legend()
                
                # Format x-axis
                ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
                fig.autofmt_xdate()
                
                # Generate filename
                filename = f"fear_greed_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
            
            else:
                raise ValueError(f"Unsupported data type for visualization: {data_type}")
            
            # Adjust layout and save to buffer
            fig.tight_layout()
            buf = io.BytesIO()
            fig.savefig(buf, format='png', bbox_inches='tight')
            buf.seek(0)
            
            # Upload to managed folder
            try:
                self.charts_folder.upload_stream(filename, buf)
                logger.info(f"Successfully uploaded chart to managed folder: {filename}")
                
                # Build the public URL using the prefix and filename
                url = f"{self.public_url_prefix.rstrip('/')}/{filename}"
                
                return {
                    "output": {
                        "image_url": url,
                        "data": data["output"]
                    },
                    "sources": [{
                        "toolCallDescription": f"Created {chart_type} chart for {data_type} data"
                    }]
                }
            except Exception as e:
                error_msg = f"Error uploading chart to managed folder: {str(e)}"
                logger.error(error_msg, exc_info=True)
                raise Exception(error_msg)
            
        except Exception as e:
            error_msg = f"Error creating visualization for {data_type}: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise Exception(error_msg)
        finally:
            plt.close(fig)
