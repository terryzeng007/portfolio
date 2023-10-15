import pandas as pd
from pypfopt import EfficientFrontier
from pypfopt import risk_models
from pypfopt import expected_returns

# Read in price data
df = pd.read_csv("C:/Users/Administrator/Desktop/20231017-开会/ETF_Optimization_update/price_update.csv", parse_dates=True, index_col="date")

S = risk_models.sample_cov(df)
viewdict = {"GLD.P": 0.20, "EWH.P": -0.30, "EWK.P": 0, "EWZ.P": -0.2, "IVV.P": 0.151321}
bl = BlackLittermanModel(S, pi="equal", absolute_views=viewdict, omega="default")
rets = bl.bl_returns()

ef = EfficientFrontier(rets, S)
ef.max_sharpe()