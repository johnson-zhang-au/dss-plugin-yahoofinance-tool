"""
Unit tests for the Yahoo Finance Tool
"""
import sys
import os
import unittest
from unittest.mock import patch, MagicMock
import json
import pandas as pd
import numpy as np
from datetime import datetime

# Add the parent directory to the path so we can import our module
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import our custom agent tool
from python_agent_tools.my_yahoofinance_tool.tool import CustomAgentTool


class TestYahooFinanceTool(unittest.TestCase):
    """Test cases for the Yahoo Finance Tool"""
    
    def setUp(self):
        """Set up the test environment"""
        self.tool = CustomAgentTool()
        
        # Set up configuration
        self.tool.set_config({
            "cache_expiry": 5,
            "logging_level": "INFO"
        }, {})
        
        # Sample data for mocking
        self.sample_quote = {
            "currentPrice": 150.25,
            "regularMarketPrice": 150.25,
            "regularMarketChange": 2.75,
            "regularMarketChangePercent": 1.86,
            "regularMarketPreviousClose": 147.50,
            "regularMarketOpen": 148.30,
            "regularMarketDayHigh": 151.20,
            "regularMarketDayLow": 147.80,
            "regularMarketVolume": 65432100,
            "marketCap": 2500000000000,
            "shortName": "Apple Inc."
        }
        
        # Mock historical data
        dates = pd.date_range(start='2023-01-01', end='2023-01-10')
        self.sample_history = pd.DataFrame({
            'Open': np.random.uniform(145, 155, len(dates)),
            'High': np.random.uniform(150, 160, len(dates)),
            'Low': np.random.uniform(140, 150, len(dates)),
            'Close': np.random.uniform(145, 155, len(dates)),
            'Volume': np.random.randint(50000000, 100000000, len(dates))
        }, index=dates)
        
        # Mock options data
        self.sample_options_calls = pd.DataFrame({
            'strike': [140, 145, 150, 155, 160],
            'lastPrice': [10.5, 7.2, 4.8, 2.5, 1.2],
            'bid': [10.4, 7.1, 4.7, 2.4, 1.1],
            'ask': [10.6, 7.3, 4.9, 2.6, 1.3],
            'change': [0.5, 0.3, 0.1, -0.2, -0.3],
            'percentChange': [5.0, 4.2, 2.1, -7.4, -20.0],
            'volume': [1200, 1500, 2200, 1800, 900],
            'openInterest': [5000, 6200, 7800, 4500, 2300],
            'impliedVolatility': [0.25, 0.23, 0.22, 0.24, 0.28]
        })
        
        self.sample_options_puts = pd.DataFrame({
            'strike': [140, 145, 150, 155, 160],
            'lastPrice': [1.1, 2.3, 4.2, 7.5, 11.2],
            'bid': [1.0, 2.2, 4.1, 7.4, 11.1],
            'ask': [1.2, 2.4, 4.3, 7.6, 11.3],
            'change': [-0.2, -0.1, 0.2, 0.4, 0.6],
            'percentChange': [-15.4, -4.2, 5.0, 5.6, 5.7],
            'volume': [800, 1100, 1900, 1400, 700],
            'openInterest': [3800, 4500, 6200, 3800, 1900],
            'impliedVolatility': [0.26, 0.24, 0.22, 0.23, 0.27]
        })
        
        self.sample_company_info = {
            "shortName": "Apple Inc.",
            "industry": "Consumer Electronics",
            "sector": "Technology",
            "country": "United States",
            "website": "https://www.apple.com",
            "market": "us_market",
            "currency": "USD",
            "exchange": "NASDAQ",
            "marketCap": 2500000000000,
            "fullTimeEmployees": 154000,
            "longBusinessSummary": "Apple Inc. designs, manufactures, and markets smartphones, personal computers, tablets, wearables, and accessories worldwide."
        }
        
        self.mock_expirations = ['2023-01-20', '2023-02-17', '2023-03-17']
        
    @patch('yfinance.Ticker')
    def test_get_stock_quote(self, mock_ticker):
        """Test getting a stock quote"""
        # Configure the mock
        mock_instance = MagicMock()
        mock_instance.info = self.sample_quote
        mock_ticker.return_value = mock_instance
        
        # Call our method
        result = self.tool._get_stock_quote("AAPL")
        
        # Verify the result
        self.assertEqual(result['output']['symbol'], "AAPL")
        self.assertEqual(result['output']['price'], 150.25)
        self.assertEqual(result['output']['change'], 2.75)
        self.assertEqual(result['output']['changePercent'], 1.86)
        self.assertEqual(result['output']['marketCap'], 2500000000000)
        
    @patch('yfinance.Ticker')
    def test_get_stock_history(self, mock_ticker):
        """Test getting historical stock data"""
        # Configure the mock
        mock_instance = MagicMock()
        mock_instance.history.return_value = self.sample_history
        mock_instance.info = self.sample_company_info
        mock_ticker.return_value = mock_instance
        
        # Call our method
        result = self.tool._get_stock_history("AAPL", period="1mo", interval="1d", enhanced_format=True)
        
        # Verify the result
        self.assertEqual(result['output']['symbol'], "AAPL")
        self.assertEqual(result['output']['period'], "1mo")
        self.assertEqual(result['output']['interval'], "1d")
        self.assertEqual(len(result['output']['data']), 10)  # 10 days in our sample
        self.assertIn('formatted_output', result['output'])
        
    @patch('yfinance.Ticker')
    def test_get_stock_options(self, mock_ticker):
        """Test getting options data"""
        # Configure the mock
        mock_instance = MagicMock()
        mock_instance.options = self.mock_expirations
        
        option_chain = MagicMock()
        option_chain.calls = self.sample_options_calls
        option_chain.puts = self.sample_options_puts
        mock_instance.option_chain.return_value = option_chain
        
        mock_ticker.return_value = mock_instance
        
        # Test getting call options
        call_result = self.tool._get_stock_options("AAPL", option_type="call")
        self.assertEqual(call_result['output']['symbol'], "AAPL")
        self.assertEqual(call_result['output']['optionType'], "Calls")
        self.assertEqual(len(call_result['output']['data']), 5)  # 5 strikes in our sample
        
        # Test getting put options
        put_result = self.tool._get_stock_options("AAPL", option_type="put")
        self.assertEqual(put_result['output']['symbol'], "AAPL")
        self.assertEqual(put_result['output']['optionType'], "Puts")
        self.assertEqual(len(put_result['output']['data']), 5)  # 5 strikes in our sample
        
        # Test getting both option types
        both_result = self.tool._get_stock_options("AAPL")
        self.assertEqual(both_result['output']['symbol'], "AAPL")
        self.assertIn('calls', both_result['output'])
        self.assertIn('puts', both_result['output'])
        
    @patch('yfinance.Ticker')
    def test_get_company_info(self, mock_ticker):
        """Test getting company information"""
        # Configure the mock
        mock_instance = MagicMock()
        mock_instance.info = self.sample_company_info
        mock_ticker.return_value = mock_instance
        
        # Call our method
        result = self.tool._get_company_info("AAPL")
        
        # Verify the result
        self.assertEqual(result['output']['symbol'], "AAPL")
        self.assertEqual(result['output']['name'], "Apple Inc.")
        self.assertEqual(result['output']['industry'], "Consumer Electronics")
        self.assertEqual(result['output']['sector'], "Technology")
        self.assertEqual(result['output']['currency'], "USD")
        self.assertIn('description', result['output'])
        
    @patch('yfinance.Ticker')
    def test_invoke_with_caching(self, mock_ticker):
        """Test the invoke method with caching"""
        # Configure the mock
        mock_instance = MagicMock()
        mock_instance.info = self.sample_quote
        mock_ticker.return_value = mock_instance
        
        # Input for the invoke method
        input_data = {
            "input": {
                "action": "quote",
                "ticker": "AAPL"
            }
        }
        
        # Call the method twice
        first_result = self.tool.invoke(input_data, MagicMock())
        
        # Second call should use cached data
        mock_ticker.reset_mock()  # Reset the mock to verify it's not called again
        second_result = self.tool.invoke(input_data, MagicMock())
        
        # Verify the ticker was not initialized again (cached result used)
        mock_ticker.assert_not_called()
        
        # Results should be the same
        self.assertEqual(first_result, second_result)


if __name__ == '__main__':
    unittest.main() 