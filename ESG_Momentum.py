# STRATEGY IDEA EXPLORATION - ESG FACTOR MOMENTUM 
# The idea is simple; firms that have improved the ESG the most are expected to outperform. 
# According to the paper (Zoltán Nagy, Altaf Kassam, Linda-Eling Lee: CAN ESG ADD ALPHA?, 2016), it is expected that 
# the market could react to a change in rating in a relatively short period. 
# However, the advantages of a better-rated ESG portfolio are expected to be apparent only in the long term, 
# for example, because of increased cash flows, etc. Therefore, the strategy is to overweight, 
# relative to the MSCI World Index, companies that increased their ESG ratings most during the recent 
# past and underweight those with decreased ESG ratings. Where the increases and decreases are based on a 12-month ESG momentum. 
# The idea we are looking at here is for exploration of concept rather than the risk model and constraints at this stage.

# The investment universe consists of stocks in the MSCI World Index.  MSCI ESG Ratings are applied as the ESG database.
# The strategy is based around MSCI designated risk model- GEM3S. This is a limitation to the testing. 
# The ESG Momentum strategy is built by overweighting, relative to the MSCI World Index, companies that increased their
# ESG ratings most during the recent past and underweight those with decreased ESG ratings, where the increases and decreases
# are based on a 12-month ESG momentum. The paper uses the Barra Global Equity Model (GEM3) for portfolio construction with arbitrary
# constraints for exploration of concept at this stage. Portfolio construction optimisation would be added into testing should
# the idea yield positive indications from this early stage testing.

# monthly rebalancing

import k_data
from collections import deque
class ESGFactorMomentumStrategy(QCAlgorithm):
    def Initialize(self):
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
        self.tickers = []
        self.holding_period = 3
        self.managed_queue = deque(maxlen = self.holding_period + 1)

        # Monthly ESG decile data.
        self.esg = {}
        self.period = 14
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
        
        # Momentum/market cap pair.
        momentum_market_cap = {}
       
        # Momentum calc.
        for stock in fine:
            symbol = stock.Symbol
            ticker = symbol.Value
            # ESG data for 14 months is ready.
            if ticker in self.esg and len(self.esg[ticker]) == self.esg[ticker].maxlen:
                esg_data = [x for x in self.esg[ticker]]               
                esg_decile_2_months_ago = esg_data[-3]
                esg_decile_14_months_ago = esg_data[0]
                
                if esg_decile_14_months_ago != 0 and esg_decile_2_months_ago != 0:
                    # Momentum as difference.
                    # momentum = esg_decile_2_months_ago - esg_decile_14_months_ago
                    
                    # Momentum as ratio.
                    momentum = (esg_decile_2_months_ago / esg_decile_14_months_ago) - 1
                    
                    market_cap = stock.EarningReports.BasicAverageShares.ThreeMonths * stock.EarningReports.BasicEPS.TwelveMonths * stock.ValuationRatios.PERatio
                    
                    # Store momentum/market cap pair.
                    momentum_market_cap[symbol] = [momentum, market_cap]
                
        if len(momentum_market_cap) == 0: return []
        
        # Momentum sorting.
        sorted_by_momentum = sorted(momentum_market_cap.items(), key = lambda x: x[1][0], reverse = True)
        decile = int(len(sorted_by_momentum) / 10)
        long = [x for x in sorted_by_momentum[:decile]]
        short = [x for x in sorted_by_momentum[-decile:]]
        
        if len(long + short) == 0: 
            # Store empty item.
            self.managed_queue.append(RebalanceQueueItem([], []))
            return []
        
        self.managed_queue.append(RebalanceQueueItem(long, short))
        self.rebalance_flag = True
        
        return [x[0] for x in long + short]
     
    def IsInvested(self, symbol):
        return self.Securities.ContainsKey(symbol) and self.Portfolio[symbol].Invested       
    
    def OnData(self, data):
        if not self.rebalance_flag:
            return
        self.rebalance_flag = False

        # Trade execution.
        if len(self.managed_queue) == 0: return

        # Liquidate first items if queue is full.
        if len(self.managed_queue) == self.managed_queue.maxlen:
            item_to_liquidate = self.managed_queue.popleft()
            for symbol, momentum_market_cap in item_to_liquidate.long_symbols + item_to_liquidate.short_symbols:
                self.Liquidate(symbol)
        
        curr_stock_set = self.managed_queue[-1]
        if curr_stock_set.count == 0: return
        
        # Open new trades.
        if self.value_weighting:
            weight = 1 / (self.holding_period * 2)
            
            total_market_cap_long = sum([x[1][1] for x in curr_stock_set.long_symbols])
            for symbol, momentum_market_cap in curr_stock_set.long_symbols:
                self.SetHoldings(symbol, weight * (momentum_market_cap[1] / total_market_cap_long))
                
            total_market_cap_short = sum([x[1][1] for x in curr_stock_set.short_symbols])
            for symbol, momentum_market_cap in curr_stock_set.short_symbols:
                self.SetHoldings(symbol, -weight * (momentum_market_cap[1] / total_market_cap_short))
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
        
        # Store history for every ticker.
        for ticker in self.tickers:
            ticker_u = ticker.upper()
            if ticker_u not in self.esg:
                self.esg[ticker_u] = deque(maxlen = self.period)
                
            decile = self.esg_data.GetLastData()[ticker]
            self.esg[ticker_u].append(decile)
        
        self.selection_flag = True

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

# References:
# https://quantpedia.com/strategies/esg-factor-momentum-strategy/
# Zoltán Nagy, Altaf Kassam, Linda-Eling Lee: CAN ESG ADD ALPHA?
# https://www.semanticscholar.org/paper/Can-ESG-Add-Alpha-An-Analysis-of-ESG-Tilt-and-Nagy-Kassam/64f77da4f8ce5906a73ffe4e9eec7c49c0960acc


