
#
# As we have previously mentioned, the choice of the database of ESG scores can alter results. This paper uses for the assessments of 
# environment, social, and governance performance of single firms database provided by Asset4. Scores are updated every year, therefore
# to obtain monthly ESG data, the scores remain unchanged until the next assessment.
# The investment universe consists of stocks of the North America region (Canada and the United States) that have ESG scores available.
# Stocks with a price of less than one USD are excluded. Paper examines the returns as abnormal returns according to the methodology of 
# Daniel et al. (1997). Such methodology controls for risk factors such as size, book-to-market ratio, and momentum. The idea is to match
# a stock along with the mentioned factors to a benchmark portfolio that contains stocks with similar characteristics. Therefore, for the
# North America region, we have 4×4 benchmark portfolios. The abnormal return is calculated as the return of stock minus the return of 
# stock´s matching benchmark portfolio return (equation 1, page 13).
# Finally, each month stocks are ranked according to their E, S and G scores. Long top 20% stocks of each score and short the bottom 20% 
# stocks of each score. Therefore, we have one complex strategy that consists of three individual strategies (for representative purposes, 
# the paper examines each strategy individually). The strategy is equally-weighted: both stocks in the quintiles and individual strategies.
# The strategy is rebalanced yearly.

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



Sources:
# https://quantpedia.com/strategies/esg-factor-investing-strategy/
