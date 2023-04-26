# biml
**Bi**nance **M**achine **L**earning crypto trading bot.
## Quick start
1. Copy default config **biml/cfg/app-defaults.yaml** to your new config  **biml/cfg/app.yaml**
2. To use virtual Binance account, create your api key here https://testnet.binance.vision/
3. Or if you want to trade on your real Binance account, create your api key here https://www.binance.com/en/support/faq/360002502072
4. Edit config file **biml/cfg/app.yaml**, set **biml.connector.key** and **biml.connector.secret** to your api key. 
5. Run **docker-compose up simplekeras** and the bot will start trading. SimpleKerasStrategy predicts future price movement to buy or sell. Periodical learning on last data happens several times a minute.
6. **biml.strategy** python package contains all the strategies implemented. 
7. Run **jupyter notebook** command and open **analytics/PredictLowHighStrategyEDA.ipynb** to explore the bot's trades. Edit **strategy** variable in the notebook to explore another strategy.


