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
