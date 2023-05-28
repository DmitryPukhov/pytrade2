# pytrade2
**Bi**nance **M**achine **L**earning - crypto trading AI bot.
## Quick start
1. Copy default config **pytrade2/cfg/app-defaults.yaml** to your new config  **pytrade2/cfg/app.yaml**
2. To use virtual Binance account, create your api key here https://testnet.binance.vision/
3. Or if you want to trade on your real Binance account, create your api key here https://www.binance.com/en/support/faq/360002502072
4. Edit config file **pytrade2/cfg/app.yaml**, set **pytrade2.connector.key** and **pytrade2.connector.secret** to your api key. 
5. Run **docker-compose up simplekeras** and the bot will start trading. SimpleKerasStrategy predicts future price movement to buy or sell. Periodical learning on last data happens several times a minute.
6. **pytrade2.strategy** python package contains all the strategies implemented. 
7. Run **jupyter notebook** command and open **analytics/PredictLowHighStrategyEDA.ipynb** to explore the bot's trades. Edit **strategy** variable in the notebook to explore another strategy.


