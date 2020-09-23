import numpy as np
from scipy.optimize import minimize

sp100_stocks = ['AAPL','MSFT','AMZN','FB','BRKB','GOOGL','GOOG','JPM','JNJ','V','PG','XOM','UNH','BAC','MA','T','DIS','INTC','HD','VZ','MRK','PFE','CVX','KO','CMCSA','CSCO','PEP','WFC','C','BA','ADBE','WMT','CRM','MCD','MDT','BMY','ABT','NVDA','NFLX','AMGN','PM','PYPL','TMO','COST','ABBV','ACN','HON','NKE','UNP','UTX','NEE','IBM','TXN','AVGO','LLY','ORCL','LIN','SBUX','AMT','LMT','GE','MMM','DHR','QCOM','CVS','MO','LOW','FIS','AXP','BKNG','UPS','GILD','CHTR','CAT','MDLZ','GS','USB','CI','ANTM','BDX','TJX','ADP','TFC','CME','SPGI','COP','INTU','ISRG','CB','SO','D','FISV','PNC','DUK','SYK','ZTS','MS','RTN','AGN','BLK']

def MonthDiff(d1, d2):
    return (d1.year - d2.year) * 12 + d1.month - d2.month

def Return(values):
    return (values[-1] - values[0]) / values[0]
    
def Volatility(values):
    values = np.array(values)
    returns = (values[1:] - values[:-1]) / values[:-1]
    return np.std(returns)  

# Custom fee model
class CustomFeeModel(FeeModel):
    def GetOrderFee(self, parameters):
        fee = parameters.Security.Price * parameters.Order.AbsoluteQuantity * 0.00005
        return OrderFee(CashAmount(fee, "USD"))

# Quandl free data
class QuandlFutures(PythonQuandl):
    def __init__(self):
        self.ValueColumnName = "settle"

# Quandl short interest data.
class QuandlFINRA_ShortVolume(PythonQuandl):
    def __init__(self):
        self.ValueColumnName = 'SHORTVOLUME'    # also 'TOTALVOLUME' is accesible

# Quantpedia data
# NOTE: IMPORTANT: Data order must be ascending (datewise)
class QuantpediaFutures(PythonData):
    def GetSource(self, config, date, isLiveMode):
        return SubscriptionDataSource("data.quantpedia.com/backtesting_data/futures/{0}.csv".format(config.Symbol.Value), SubscriptionTransportMedium.RemoteFile, FileFormat.Csv)

    def Reader(self, config, line, date, isLiveMode):
        data = QuantpediaFutures()
        data.Symbol = config.Symbol
        
        if not line[0].isdigit(): return None
        split = line.split(';')
        
        data.Time = datetime.strptime(split[0], "%d.%m.%Y") + timedelta(days=1)
        data['settle'] = float(split[1])
        data.Value = float(split[1])

        return data
        
# NOTE: Manager for new trades. It's represented by certain count of equally weighted brackets for long and short positions.
# If there's a place for new trade, it will be managed for time of holding period.
class TradeManager():
    def __init__(self, algorithm, long_size, short_size, holding_period):
        self.algorithm = algorithm  # algorithm to execute orders in.
        
        self.long_size = long_size
        self.short_size = short_size
        self.weight = 1 / (self.long_size + self.short_size)
        
        self.long_len = 0
        self.short_len = 0
    
        # Arrays of ManagedSymbols
        self.symbols = []
        
        self.holding_period = holding_period    # Days of holding.
    
    # Add stock symbol object
    def Add(self, symbol, long_flag):
        # Open new long trade.
        managed_symbol = ManagedSymbol(symbol, self.holding_period, long_flag)
        
        if long_flag:
            # If there's a place for it.
            if self.long_len < self.long_size:
                self.symbols.append(managed_symbol)
                self.algorithm.SetHoldings(symbol, self.weight)
                self.long_len += 1
        # Open new short trade.
        else:
            # If there's a place for it.
            if self.long_len < self.short_size:
                self.symbols.append(managed_symbol)
                self.algorithm.SetHoldings(symbol, - self.weight)
                self.short_len += 1
    
    # Decrement holding period and liquidate symbols.
    def TryLiquidate(self):
        symbols_to_delete = []
        for managed_symbol in self.symbols:
            managed_symbol.days_to_liquidate -= 1
            
            # Liquidate.
            if managed_symbol.days_to_liquidate == 0:
                symbols_to_delete.append(managed_symbol)
                self.algorithm.Liquidate(managed_symbol.symbol)
                
                if managed_symbol.long_flag: self.long_len -= 1
                else: self.short_len -= 1

        # Remove symbols from management.
        for managed_symbol in symbols_to_delete:
            self.symbols.remove(managed_symbol)
    
    def LiquidateTicker(self, ticker):
        symbol_to_delete = None
        for managed_symbol in self.symbols:
            if managed_symbol.symbol.Value == ticker:
                self.algorithm.Liquidate(managed_symbol.symbol)
                symbol_to_delete = managed_symbol
                if managed_symbol.long_flag: self.long_len -= 1
                else: self.short_len -= 1
                
                break
        
        if symbol_to_delete: self.symbols.remove(symbol_to_delete)
        else: self.algorithm.Debug("Ticker is not held in portfolio!")
    
class ManagedSymbol():
    def __init__(self, symbol, days_to_liquidate, long_flag):
        self.symbol = symbol
        self.days_to_liquidate = days_to_liquidate
        self.long_flag = long_flag
        
class PortfolioOptimization(object):
    def __init__(self, df_return, risk_free_rate, num_assets):
        self.daily_return = df_return
        self.risk_free_rate = risk_free_rate
        self.n = num_assets # numbers of risk assets in portfolio
        self.target_vol = 0.05

    def annual_port_return(self, weights):
        # calculate the annual return of portfolio
        return np.sum(self.daily_return.mean() * weights) * 252

    def annual_port_vol(self, weights):
        # calculate the annual volatility of portfolio
        return np.sqrt(np.dot(weights.T, np.dot(self.daily_return.cov() * 252, weights)))

    def min_func(self, weights):
        # method 1: maximize sharp ratio
        return - self.annual_port_return(weights) / self.annual_port_vol(weights)
        
        # method 2: maximize the return with target volatility
        #return - self.annual_port_return(weights) / self.target_vol

    def opt_portfolio(self):
        # maximize the sharpe ratio to find the optimal weights
        cons = ({'type': 'eq', 'fun': lambda x: np.sum(x) - 1})
        bnds = tuple((0, 1) for x in range(2)) + tuple((0, 0.25) for x in range(self.n - 2))
        opt = minimize(self.min_func,                               # object function
                       np.array(self.n * [1. / self.n]),            # initial value
                       method='SLSQP',                              # optimization method
                       bounds=bnds,                                 # bounds for variables 
                       constraints=cons)                            # constraint conditions
                      
        opt_weights = opt['x']
 
        return opt_weights

import k_data
from collections import deque

class ESGFactorInvestingStrategy(QCAlgorithm):

    def Initialize(self):
        #self.SetStartDate(2014, 6, 1)
        self.SetStartDate(2009, 6, 1)
        self.SetEndDate(2019, 12, 31)
        self.SetCash(100000)
        

        # Decile weighting.
        # True - Value weighted
        # False - Equally weighted
        self.value_weighting = True
        

        self.symbol = 'SPY'
        self.AddEquity(self.symbol, Resolution.Daily)
        
        self.esg_data = self.AddData(ESGData, 'ESG', Resolution.Daily)
        
        # All tickers from ESG database.
        self.tickers = []
        
        self.ticker_deciles = {}
        
        self.holding_period = 12
        self.managed_queue = deque(maxlen = self.holding_period + 1)
        
        self.selection_flag = False
        self.rebalance_flag = False
        self.UniverseSettings.Resolution = Resolution.Daily
        self.AddUniverse(self.CoarseSelectionFunction, self.FineSelectionFunction)
        
        self.Schedule.On(self.DateRules.MonthStart(self.symbol), self.TimeRules.AfterMarketOpen(self.symbol), self.Selection)
    
    def OnSecuritiesChanged(self, changes):
        for security in changes.AddedSecurities:
            security.SetFeeModel(k_data.CustomFeeModel(self))
    
    def CoarseSelectionFunction(self, coarse):
        if not self.selection_flag:
            return Universe.Unchanged
        
        self.selection_flag = False
        
        selected = [x.Symbol for x in coarse if (x.Symbol.Value).lower() in self.tickers]

        return selected
    
    def FineSelectionFunction(self, fine):
        fine = [x for x in fine if x.EarningReports.BasicAverageShares.ThreeMonths > 0 and x.EarningReports.BasicEPS.TwelveMonths > 0 and x.ValuationRatios.PERatio > 0]

        self.rebalance_flag = True
        
        # Store symbol/market cap pair.
        long = [[x.Symbol, (x.EarningReports.BasicAverageShares.ThreeMonths * (x.EarningReports.BasicEPS.TwelveMonths * x.ValuationRatios.PERatio))]
                            for x in fine if (x.Symbol.Value in self.ticker_deciles) and                                                       \
                                                (len(self.ticker_deciles[x.Symbol.Value]) == self.ticker_deciles[x.Symbol.Value].maxlen) and    \
                                                (self.ticker_deciles[x.Symbol.Value][0] != 0) and                                               \
                                                (self.ticker_deciles[x.Symbol.Value][0] >= 0.8) and                                             \
                                                not self.IsInvested(x.Symbol)]
        
        short = [[x.Symbol, (x.EarningReports.BasicAverageShares.ThreeMonths * (x.EarningReports.BasicEPS.TwelveMonths * x.ValuationRatios.PERatio))]
                            for x in fine if (x.Symbol.Value in self.ticker_deciles) and                                                      \
                                                (len(self.ticker_deciles[x.Symbol.Value]) == self.ticker_deciles[x.Symbol.Value].maxlen) and    \
                                                (self.ticker_deciles[x.Symbol.Value][0] != 0) and                                               \
                                                (self.ticker_deciles[x.Symbol.Value][0] <= 0.2) and                                             \
                                                not self.IsInvested(x.Symbol)]

        if len(long + short) == 0: 
            # Store empty item.
            self.managed_queue.append(RebalanceQueueItem([], []))
            return []
        
        self.managed_queue.append(RebalanceQueueItem(long, short))

        return [x[0] for x in long + short]

    def OnData(self, data):
        if not self.rebalance_flag:
            return
        self.rebalance_flag = False

        # Trade execution.
        if len(self.managed_queue) == 0: return

        # Liquidate first items if queue is full.
        if len(self.managed_queue) == self.managed_queue.maxlen:
            item_to_liquidate = self.managed_queue.popleft()
            for symbol, market_cap in item_to_liquidate.long_symbols + item_to_liquidate.short_symbols:
                self.Liquidate(symbol)
        
        curr_stock_set = self.managed_queue[-1]
        if curr_stock_set.count == 0: return
        
        # Open new trades.
        if self.value_weighting:
            weight = 1 / (self.holding_period * 2)
            
            total_market_cap_long = sum([x[1] for x in curr_stock_set.long_symbols])
            for symbol, market_cap in curr_stock_set.long_symbols:
                self.SetHoldings(symbol, weight * (market_cap / total_market_cap_long))
                
            total_market_cap_short = sum([x[1] for x in curr_stock_set.short_symbols])
            for symbol, market_cap in curr_stock_set.short_symbols:
                self.SetHoldings(symbol, -weight * (market_cap / total_market_cap_short))
        else:
            weight = 1 / (self.holding_period * curr_stock_set.count)
            
            # Equally weighted.
            for symbol, market_cap in curr_stock_set.long_symbols:
                self.SetHoldings(symbol, weight)
            for symbol, market_cap in curr_stock_set.short_symbols:
                self.SetHoldings(symbol, -weight)

    def Selection(self):
        # Store universe tickers.
        if len(self.tickers) == 0:
            self.tickers = [x.Key for x in self.esg_data.GetLastData().GetStorageDictionary()]

        self.selection_flag = True
        
        # Store history for every ticker.
        for ticker in self.tickers:
            ticker_u = ticker.upper()
            if ticker_u not in self.ticker_deciles:
                self.ticker_deciles[ticker_u] = deque(maxlen = 2)
                
            decile = self.esg_data.GetLastData()[ticker]
            self.ticker_deciles[ticker_u].append(decile)
            
    def IsInvested(self, symbol):
        return self.Securities.ContainsKey(symbol) and self.Portfolio[symbol].Invested

class RebalanceQueueItem():
    def __init__(self, long_symbols, short_symbols):
        self.long_symbols = long_symbols
        self.short_symbols = short_symbols
        self.count = len(long_symbols + short_symbols)
        
# ESG data.
class ESGData(PythonData):
    def __init__(self):
        self.tickers = []
    
    def GetSource(self, config, date, isLiveMode):
        return SubscriptionDataSource("data.quantpedia.com/backtesting_data/economic/esg_deciles_data.csv", SubscriptionTransportMedium.RemoteFile, FileFormat.Csv)
    
    def Reader(self, config, line, date, isLiveMode):
        data = ESGData()
        data.Symbol = config.Symbol
        
        if not line[0].isdigit():
            self.tickers = [x for x in line.split(';')][1:]
            return None
            
        split = line.split(';')
        
        data.Time = datetime.strptime(split[0], "%Y-%m-%d") + timedelta(days=1)
        
        index = 1
        for ticker in self.tickers:
            data[ticker] = float(split[index])
            index += 1
            
        data.Value = float(split[1])
        return data



#Sources:
#1.https://www.unpri.org/
#2.MELAS, NAGY, NISHIKAWA, LEE, GIESE: Foundations of ESG Investing – Part 1: How ESG Affects Equity Valuation, Risk and Performance
#3.https://www.unpri.org/listed-equity/esg-integration-in-quantitative-strategies/13.article
#4.Daniel, K., M. Grinblatt, S. Titman, and R. Wermers (1997). Measuring mutual fund performance with characteristic-based benchmarks. Journal of Finance 52 (3), 1035– 1058.
#5.Dorfleitner, Gregor and Utz, Sebastian and Wimmer, Maximilian: Where and When Does It Pay to Be Good? A Global Long-Term Analysis of ESG Investing
