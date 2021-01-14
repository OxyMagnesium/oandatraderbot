import datetime as dt
import logging
import threading
from time import sleep

from oandapyV20 import API
from oandapyV20.contrib.requests import (
    MarketOrderRequest,
    StopLossDetails,
    TakeProfitDetails,
)
from oandapyV20.endpoints.accounts import AccountSummary
from oandapyV20.endpoints.instruments import InstrumentsCandles
from oandapyV20.endpoints.orders import OrderCreate
from oandapyV20.endpoints.positions import PositionClose
from oandapyV20.endpoints.pricing import PricingStream
from oandapyV20.exceptions import V20Error

logging.basicConfig(
    filename = 'log.txt',
    filemode = 'w',
    format = '%(asctime)s:%(name)s:(%(levelname)s): %(message)s',
    datefmt = '%d-%b-%y %H:%M:%S',
    level = logging.INFO,
)

POS_LONG = 1
POS_SHORT = -1
DESIRED_MARGIN = 0.1
WINDOW_SIZE = dt.timedelta(minutes = 15)

OPENING_TIMES = {
    'EUR_USD': dt.time(hour = 8, minute = 0),
    'GBP_USD': dt.time(hour = 8, minute = 0),
    'AUD_USD': dt.time(hour = 8, minute = 0),
}

CLOSING_TIMES = {
    'EUR_USD': dt.time(hour = 17, minute = 0),
    'GBP_USD': dt.time(hour = 17, minute = 0),
    'AUD_USD': dt.time(hour = 17, minute = 0),
}

TIMEZONES = {
    'EUR_USD': dt.timezone(dt.timedelta(hours = 0), name = 'GMT'),
    'GBP_USD': dt.timezone(dt.timedelta(hours = 0), name = 'GMT'),
    'AUD_USD': dt.timezone(-dt.timedelta(hours = 0), name = 'GMT'),
}

TRADER_START_FUNDS = {
    'EUR_USD': 1000,
    'GBP_USD': 1000,
    'AUD_USD': 1000,
}

class ORBTrader:
    def __init__(self, account_id: str, api: API, instrument: str):
        self.account_id = account_id
        self.api = api
        self.instrument = instrument
        self.opening_time = OPENING_TIMES[instrument]
        self.closing_time = CLOSING_TIMES[instrument]
        self.timezone = TIMEZONES[instrument]
        self.trading = False
        self.pos = None
        self.high = None
        self.low = None
        self.bid = None
        self.ask = None
        self.funds = TRADER_START_FUNDS[instrument]
        logging.info(f'Initialized trader for {instrument}')

    def _get_opening_range(self, from_time: dt.datetime):
        # Assemble parameters and send request
        params = {
            'granularity': f'M{WINDOW_SIZE.seconds//60}',
            'from': from_time.timestamp(),
            'count': 1,
        }
        endpoint = InstrumentsCandles(self.instrument, params)

        # Get the required candle and ensure it is complete
        complete = False
        while not complete:
            sleep(5)
            try:
                logging.info(f'Attempting to get candle for {self.instrument}'
                             f' with opening time {from_time}')
                response = self.api.request(endpoint)
            except V20Error as e:
                logging.error(f'Failed to get candle {self.instrument}: {e}')
                logging.info(f'Aborting candle read for {self.instrument}')
                return
            except OSError as e:
                logging.error(f'Failed to get candle {self.instrument}: {e}')
                logging.info(f'Retrying candle read for {self.instrument}')
            else:
                logging.debug(f'Response: {response}')
                if response['candles']:
                    complete = response['candles'][0]['complete']
                    self.high = float(response['candles'][0]['mid']['h'])
                    self.low = float(response['candles'][0]['mid']['l'])
                else:
                    logging.error(f'Received no candles for {self.instrument}')
                    logging.info(f'Retrying candle read for {self.instrument}')

    def _open_position(self, pos_type: int, funds_alloc: float = 1):
        # Buy as many units as possible using a funds_alloc fraction of the
        # total funds assigned to this trader with a stop loss at the range low
        price = self.ask if pos_type == POS_LONG else self.bid
        stop_loss = self.low if pos_type == POS_LONG else self.high
        take_profit = price*(1 + pos_type*DESIRED_MARGIN)
        order = MarketOrderRequest(
            instrument = self.instrument,
            units = pos_type*funds_alloc*self.funds/price,
            stopLossOnFill = StopLossDetails(stop_loss).data,
            takeProfitOnFill = TakeProfitDetails(take_profit).data,
        )

        # Assemble the request and send it
        logging.info(f'Placing order for {self.instrument}')
        endpoint = OrderCreate(self.account_id, order.data)
        try:
            response = self.api.request(endpoint)
        except (V20Error, OSError) as e:
            logging.error(f'{self.instrument} order failed (aborting): {e}')
        else:
            logging.debug(f'Response: {response}')
            logging.info(f'Order for {self.instrument} placed successfully')
            self.pos = pos_type

    def _close_position(self):
        # Close all positions for this instrument if any are open
        if not self.pos:
            logging.info(f'No position for {self.instrument} to close')
            return

        endpoint = PositionClose(
            self.account_id,
            self.instrument,
            {f'{"long" if self.pos == POS_LONG else "short"}Units': 'ALL'},
        )

        try:
            response = self.api.request(endpoint)
        except V20Error as e:
            logging.error(f'Failed close positions for {self.instrument}: {e}')
        else:
            logging.debug(f'Response: {response}')
            logging.info(f'Positions for {self.instrument} closed')
            self.pos = None

    def tick(self, price: dict):
        # If we're looking to open trades, store prices from the stream
        if self.trading:
            self.bid = price['bid']
            self.ask = price['ask']
            if self.ask > self.high:
                # Upper boundary breakout occurred; take a long position
                self.trading = False
                thread = threading.Thread(
                    target = self._open_position,
                    args = (POS_LONG, ),
                )
                thread.start()
            if self.bid < self.low:
                # Lower boundary breakout occurred; take a short position
                self.trading = False
                thread = threading.Thread(
                    target = self._open_position,
                    args = (POS_SHORT, ),
                )
                thread.start()

    def run(self):
        while True:
            # Calculate the next market opening and closing times
            now = dt.datetime.now(tz = self.timezone)
            opening = dt.datetime.combine(
                now.date(),
                self.opening_time,
                self.timezone,
            )
            closing = dt.datetime.combine(
                now.date(),
                self.closing_time,
                self.timezone
            )
            if now > closing:
                closing += dt.timedelta(hours = 24)
            if now > opening:
                opening += dt.timedelta(hours = 24)

            if self.trading == False:
                # If we are not trading right now, wait until the market opens
                # before getting the day's opening range and starting to trade
                sleep_time = (opening - now + WINDOW_SIZE).total_seconds()
                logging.info(f'{self.instrument} waiting until {opening}'
                             f' ({sleep_time/60} minutes) to start trading')
                sleep(sleep_time)
                self._get_opening_range(opening)
                self.trading = True
                logging.info(f'{self.instrument} is starting')

            else: # self.trading == True
                # If we are trading right now, wait until the market is about
                # to close before closing positions and stopping to trade
                sleep_time = (closing - now - WINDOW_SIZE).total_seconds()
                logging.info(f'{self.instrument} waiting until {closing}'
                             f' ({sleep_time/60} minutes) to stop trading')
                sleep(sleep_time)
                self.trading = False
                self._close_position()
                logging.info(f'{self.instrument} is stopping')


class TraderPool:
    def __init__(self, account_id: str, token: str, instruments: list):
        self.account_id = account_id
        self.api = API(token)
        self.instruments = instruments
        self.traders = {
            instrument: ORBTrader(self.account_id, self.api, instrument)
            for instrument in instruments
        }
        logging.info('Initialized trader pool')

    def _monitor_prices(self):
        # Set up price stream
        endpoint = PricingStream(
            self.account_id,
            {'instruments': ','.join(self.instruments)},
        )

        logging.info('Starting price stream')

        while True:
            try:
                for response in self.api.request(endpoint):
                    # Store any price updates and inform the appropriate trader
                    if response['type'] == 'PRICE':
                        # NOTE: This method of getting the max/min bid/ask may
                        # be unnecessary as they appear to be ordered such that
                        # the appropriate value is first. However, this is not
                        # documented anywhere, so doing it this way is more 
                        # futureproof at the cost of some performance.
                        highest_bid = max(
                            response['bids'], 
                            key = lambda x: float(x['price'])
                        )
                        lowest_ask = min(
                            response['asks'],
                            key = lambda x: float(x['price'])
                        )
                        # Update the appropriate trader
                        self.traders[response['instrument']].tick({
                            'bid': float(highest_bid['price']),
                            'ask': float(lowest_ask['price']),
                        })
            except (V20Error, OSError) as e:
                logging.error(e)

    def run(self):
        # Get account balance and other details
        endpoint = AccountSummary(self.account_id)

        try:
            response = self.api.request(endpoint)
        except (V20Error, OSError) as e:
            logging.error(f'Failed to get account balance; aborting: {e}')
        else:
            logging.debug(f'Response: {response}')
            total_balance = response['account']['balance']
            logging.info(f'Current account balance: {total_balance}')

        # Create and start threads for each trader
        for instrument in self.traders:
            thread = threading.Thread(
                target = self.traders[instrument].run,
                name = instrument,
            )
            thread.start()

        # Start stream for watching price changes
        thread = threading.Thread(
            target = self._monitor_prices,
            name = 'stream',
        )
        thread.start()


if __name__ == '__main__':
    with open('account.txt', 'r') as f:
        account_id = f.read().strip()
    with open('token.txt', 'r') as f:
        token = f.read().strip()
    instruments = ['EUR_USD', 'GBP_USD', 'AUD_USD']
    TraderPool(account_id, token, instruments).run()
