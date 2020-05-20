# The strategy is based upon the idea that it is easier to exploit a negative correlation by switching between
# two assets than by traditional asset mixing. It is a simple strategy in this current form - we are testing proof of concept.
# We want to test the simplest form initially to establish if we can build further on the idea going forward
# Paired-switching refers to investing in one of a pair of negatively correlated equities/ETFs/Funds and
# periodic switching of the position on the basis of either the relative performance of the two equities/ETFs/
# Funds over a period immediately prior to the switching or some other criterion. It is based upon the idea
# that if the returns of two assets are negatively correlated, the overlapping of the periods during which
# the assets individually yield returns greater than their mean values will be infrequent. Consequently,
# if the criterion for switching is even minimally accurate in its ability to identify the boundaries of
# such periods, there is a possibility of improving the performance of the portfolio consisting of the two
# assets over the portfolio wherein the two assets are statically weighted on the basis of traditional
# methods such as, for example, variance minimisation. 


from datetime import datetime,timedelta
import pandas as pd
import numpy as np
class PairedSwitching(QCAlgorithm):
    
    def Initialize(self):
        self.SetStartDate(2005,3,15)
        self.SetEndDate(2020,3,11)
        self.SetCash(100000)
        #we select two etfs that are negatively correlated; equity and bond etfs
        self.first = self.AddEquity("SPY",Resolution.Minute)
        self.second = self.AddEquity("AGG",Resolution.Minute)
        self.months = -1
        #monthly scheduled event but rebalancing will run on quarterly basis
        self.Schedule.On(self.DateRules.MonthStart("SPY"), self.TimeRules.AfterMarketOpen("SPY", 1), self.Rebalance)

    def Rebalance(self):
        self.months +=1
        if(self.months%3==0):
            #retrieves prices from 90 days ago
            history_call = self.History(self.Securities.Keys,timedelta(days=90))
            if not history_call.empty:
                first_bars = history_call.loc[self.first.Symbol.Value]
                last_p1 = first_bars["close"].iloc[0]
                second_bars = history_call.loc[self.second.Symbol.Value]
                last_p2 = second_bars["close"].iloc[0]
                # calculates performance of funds over the prior quarter
                first_performance = (float(self.Securities[self.first.Symbol].Price) - float(last_p1))/(float(self.Securities[self.first.Symbol].Price))
                second_performance = (float(self.Securities[self.second.Symbol].Price) - float(last_p2))/(float(self.Securities[self.second.Symbol].Price))
                #buys the fund that has the higher return during the period
                if(first_performance > second_performance):
                    if(self.Securities[self.second.Symbol].Invested==True):
                        self.Liquidate(self.second.Symbol)
                    self.SetHoldings(self.first.Symbol,1)
                else:
                    if(self.Securities[self.first.Symbol].Invested==True):
                        self.Liquidate(self.first.Symbol)
                    self.SetHoldings(self.second.Symbol,1)

    def OnData(self, data):
        pass

# References:
# Maewal, Bock: Paired-Switching for Tactical Portfolio Allocation,
# http://papers.ssrn.com/sol3/papers.cfm?abstract_id=1917044
# https://quantpedia.com/strategies/paired-switching/
# https://www.investopedia.com/terms/s/sectorrotation.asp

