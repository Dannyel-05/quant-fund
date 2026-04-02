"""
Abstract broker interface + paper (simulated) implementation.
"""
import logging
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import pandas as pd
import requests

logger = logging.getLogger(__name__)


class BrokerInterface(ABC):
    @abstractmethod
    def place_order(
        self,
        ticker: str,
        quantity: float,
        direction: str,
        fill_price: float,
        order_type: str = "market",
    ) -> Dict:
        ...

    @abstractmethod
    def get_positions(self) -> Dict[str, float]:
        ...

    @abstractmethod
    def get_account_value(self, current_prices: Dict[str, float] = None) -> float:
        ...

    @abstractmethod
    def get_cash(self) -> float:
        ...


class PaperBroker(BrokerInterface):
    def __init__(self, initial_capital: float, config: dict):
        self.config = config
        self.initial_capital = initial_capital
        self.cash = initial_capital
        self.positions: Dict[str, float] = {}      # ticker -> net shares
        self.avg_prices: Dict[str, float] = {}     # ticker -> avg entry price
        self._log: List[Dict] = []

    # ------------------------------------------------------------------

    def place_order(
        self,
        ticker: str,
        quantity: float,
        direction: str,
        fill_price: float,
        order_type: str = "market",
    ) -> Dict:
        market = "uk" if ticker.endswith(".L") else "us"
        costs = self.config["costs"][market]
        slip = fill_price * costs["slippage_pct"]
        commission = costs.get(
            "commission_per_trade", costs.get("commission_per_trade_gbp", 0)
        )

        if direction == "buy":
            exec_price = fill_price + slip
            stamp = exec_price * quantity * costs.get("stamp_duty_pct", 0)
            total_cost = exec_price * quantity + commission + stamp
            if total_cost > self.cash:
                return {"status": "rejected", "reason": "insufficient_funds"}
            self.cash -= total_cost
            self._update_position(ticker, quantity, exec_price)

        elif direction == "sell":
            exec_price = fill_price - slip
            if self.positions.get(ticker, 0) < quantity:
                return {"status": "rejected", "reason": "insufficient_position"}
            self.cash += exec_price * quantity - commission
            self._update_position(ticker, -quantity, exec_price)

        elif direction == "short":
            exec_price = fill_price - slip
            self.cash += exec_price * quantity - commission
            self._update_position(ticker, -quantity, exec_price)

        elif direction == "cover":
            exec_price = fill_price + slip
            total_cost = exec_price * quantity + commission
            if total_cost > self.cash:
                return {"status": "rejected", "reason": "insufficient_funds"}
            self.cash -= total_cost
            self._update_position(ticker, quantity, exec_price)

        else:
            return {"status": "rejected", "reason": f"unknown direction: {direction}"}

        record = {
            "ticker": ticker,
            "direction": direction,
            "quantity": quantity,
            "fill_price": fill_price,
            "exec_price": exec_price,
            "timestamp": datetime.now().isoformat(),
            "status": "filled",
        }
        self._log.append(record)
        logger.info("FILL %s %s %.0f @ %.4f", direction.upper(), ticker, quantity, exec_price)
        return record

    def get_positions(self) -> Dict[str, float]:
        return {t: v for t, v in self.positions.items() if v != 0}

    def get_account_value(self, current_prices: Dict[str, float] = None) -> float:
        if not current_prices:
            return self.cash
        pos_value = sum(
            shares * current_prices.get(t, self.avg_prices.get(t, 0))
            for t, shares in self.positions.items()
        )
        return self.cash + pos_value

    def get_cash(self) -> float:
        return self.cash

    def get_pnl(self, current_prices: Dict[str, float]) -> float:
        return self.get_account_value(current_prices) - self.initial_capital

    def get_trade_log(self) -> pd.DataFrame:
        return pd.DataFrame(self._log)

    # ------------------------------------------------------------------

    def _update_position(self, ticker: str, delta: float, price: float) -> None:
        old = self.positions.get(ticker, 0)
        new = old + delta
        if abs(new) < 1e-6:
            self.positions.pop(ticker, None)
            self.avg_prices.pop(ticker, None)
        else:
            # FIFO average cost
            if old == 0 or (old > 0) != (new > 0):
                self.avg_prices[ticker] = price
            else:
                self.avg_prices[ticker] = (
                    self.avg_prices.get(ticker, price) * abs(old) + price * abs(delta)
                ) / abs(new)
            self.positions[ticker] = new


# ---------------------------------------------------------------------------
# Alpaca Paper Broker — direct HTTP implementation
# ---------------------------------------------------------------------------


class AlpacaPaperBroker(BrokerInterface):
    """
    Alpaca paper trading broker using direct HTTP requests.
    Reads api_key from config['api_keys']['alpaca_api_key'].
    Falls back gracefully to simulation mode if credentials absent.
    """

    def __init__(self, config: dict):
        self.config = config
        api_key = config.get('api_keys', {}).get('alpaca_api_key', '')
        secret_key = config.get('api_keys', {}).get('alpaca_secret_key', '')
        alpaca_cfg = config.get('alpaca', {})
        self.base_url = alpaca_cfg.get('base_url', 'https://paper-api.alpaca.markets').rstrip('/')
        self.data_url = alpaca_cfg.get('data_url', 'https://data.alpaca.markets').rstrip('/')

        self.enabled = bool(api_key) and 'PASTE' not in api_key
        self.headers = {
            'APCA-API-KEY-ID': api_key,
            'APCA-API-SECRET-KEY': secret_key,
        }
        self._connected = self.is_connected() if self.enabled else False
        if self._connected:
            print('AlpacaPaperBroker: CONNECTED')
        else:
            print('AlpacaPaperBroker: SIMULATION MODE')

    # ------------------------------------------------------------------
    # Connection helpers
    # ------------------------------------------------------------------

    def is_connected(self) -> bool:
        """GET /v2/account — returns True if status 200."""
        try:
            r = requests.get(f'{self.base_url}/v2/account',
                             headers=self.headers, timeout=10)
            return r.status_code == 200
        except Exception as e:
            logger.debug('AlpacaPaperBroker.is_connected: %s', e)
            return False

    # ------------------------------------------------------------------
    # Account & positions
    # ------------------------------------------------------------------

    def get_account(self) -> dict:
        """GET /v2/account."""
        if not self._connected:
            return {}
        try:
            r = requests.get(f'{self.base_url}/v2/account',
                             headers=self.headers, timeout=10)
            if r.status_code == 200:
                return r.json()
        except Exception as e:
            logger.error('AlpacaPaperBroker.get_account: %s', e)
        return {}

    def get_positions(self) -> list:
        """GET /v2/positions."""
        if not self._connected:
            return []
        try:
            r = requests.get(f'{self.base_url}/v2/positions',
                             headers=self.headers, timeout=10)
            if r.status_code == 200:
                return r.json()
        except Exception as e:
            logger.error('AlpacaPaperBroker.get_positions: %s', e)
        return []

    def get_account_value(self, current_prices: Dict[str, float] = None) -> float:
        """Return total portfolio value (satisfies BrokerInterface abstract method)."""
        acc = self.get_account()
        return float(acc.get('portfolio_value', 0.0)) if acc else 0.0

    def get_cash(self) -> float:
        """Return available cash (satisfies BrokerInterface abstract method)."""
        acc = self.get_account()
        return float(acc.get('cash', 0.0)) if acc else 0.0

    # ------------------------------------------------------------------
    # Order management
    # ------------------------------------------------------------------

    def place_order(self, ticker: str, qty: float, side: str,
                    order_type: str = 'market',
                    limit_price: Optional[float] = None,
                    direction: str = '',
                    fill_price: float = 0.0) -> dict:
        """
        Submit an order to Alpaca paper trading.
        Supports BrokerInterface signature (direction/fill_price) as well
        as direct Alpaca-style (ticker, qty, side).
        """
        if not self._connected:
            return {'status': 'error', 'reason': 'not_connected'}
        # Map PaperBroker direction conventions to Alpaca side strings
        if direction:
            side_map = {'buy': 'buy', 'sell': 'sell', 'short': 'sell', 'cover': 'buy'}
            side = side_map.get(direction, side)
        try:
            body: dict = {
                'symbol': ticker,
                'qty': str(int(max(1, qty))),
                'side': side,
                'type': order_type,
                'time_in_force': 'day',
            }
            if order_type == 'limit' and limit_price is not None:
                body['limit_price'] = str(limit_price)
            r = requests.post(f'{self.base_url}/v2/orders',
                              json=body, headers=self.headers, timeout=10)
            if r.status_code in (200, 201):
                data = r.json()
                return {
                    'status': 'submitted',
                    'order_id': data.get('id'),
                    'ticker': ticker,
                    'qty': qty,
                    'side': side,
                }
            return {'status': 'error', 'reason': r.text[:200]}
        except Exception as exc:
            logger.error('AlpacaPaperBroker.place_order %s: %s', ticker, exc)
            return {'status': 'error', 'reason': str(exc)}

    def close_position(self, ticker: str) -> dict:
        """DELETE /v2/positions/{ticker}."""
        if not self._connected:
            return {'status': 'error', 'reason': 'not_connected'}
        try:
            r = requests.delete(f'{self.base_url}/v2/positions/{ticker}',
                                headers=self.headers, timeout=10)
            return r.json() if r.status_code in (200, 207) else {'status': 'error', 'code': r.status_code}
        except Exception as e:
            logger.error('AlpacaPaperBroker.close_position %s: %s', ticker, e)
            return {'status': 'error', 'reason': str(e)}

    def get_order_history(self, limit: int = 500) -> list:
        """GET /v2/orders?status=all&limit=..."""
        if not self._connected:
            return []
        try:
            r = requests.get(f'{self.base_url}/v2/orders',
                             params={'status': 'all', 'limit': limit},
                             headers=self.headers, timeout=10)
            return r.json() if r.status_code == 200 else []
        except Exception as e:
            logger.error('AlpacaPaperBroker.get_order_history: %s', e)
            return []

    # ------------------------------------------------------------------
    # Market data
    # ------------------------------------------------------------------

    def get_realtime_price(self, ticker: str) -> float:
        """GET data_url/v2/stocks/{ticker}/quotes/latest."""
        try:
            r = requests.get(
                f'{self.data_url}/v2/stocks/{ticker}/quotes/latest',
                headers=self.headers, timeout=10)
            if r.status_code == 200:
                data = r.json()
                quote = data.get('quote', {})
                ask = quote.get('ap', 0)
                bid = quote.get('bp', 0)
                if ask and bid:
                    return (float(ask) + float(bid)) / 2
                if ask:
                    return float(ask)
                if bid:
                    return float(bid)
        except Exception as e:
            logger.debug('AlpacaPaperBroker.get_realtime_price %s: %s', ticker, e)
        return 0.0

    def _get_bars(self, ticker: str, timeframe: str, days_back: int) -> pd.DataFrame:
        """Internal helper: fetch OHLCV bars from Alpaca data API."""
        try:
            start = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%dT%H:%M:%SZ')
            end = datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
            r = requests.get(
                f'{self.data_url}/v2/stocks/{ticker}/bars',
                params={
                    'timeframe': timeframe,
                    'start': start,
                    'end': end,
                    'limit': 10000,
                    'adjustment': 'raw',
                },
                headers=self.headers, timeout=20)
            if r.status_code == 200:
                bars = r.json().get('bars', [])
                if bars:
                    df = pd.DataFrame(bars)
                    df['t'] = pd.to_datetime(df['t'])
                    df = df.rename(columns={
                        't': 'timestamp', 'o': 'open', 'h': 'high',
                        'l': 'low', 'c': 'close', 'v': 'volume'
                    })
                    df = df.set_index('timestamp')
                    return df
        except Exception as e:
            logger.debug('AlpacaPaperBroker._get_bars %s %s: %s', ticker, timeframe, e)
        return pd.DataFrame()

    def get_bars_30min(self, ticker: str, days_back: int = 30) -> pd.DataFrame:
        """Fetch 30-minute OHLCV bars."""
        return self._get_bars(ticker, '30Min', days_back)

    def get_bars_1min(self, ticker: str, days_back: int = 5) -> pd.DataFrame:
        """Fetch 1-minute OHLCV bars."""
        return self._get_bars(ticker, '1Min', days_back)

    # ------------------------------------------------------------------
    # Market hours
    # ------------------------------------------------------------------

    def is_market_open(self) -> bool:
        """Check if US market is currently open."""
        hours = self.get_market_hours()
        return hours.get('is_open', False)

    def get_market_hours(self) -> dict:
        """GET /v2/clock."""
        try:
            r = requests.get(f'{self.base_url}/v2/clock',
                             headers=self.headers, timeout=10)
            if r.status_code == 200:
                return r.json()
        except Exception as e:
            logger.debug('AlpacaPaperBroker.get_market_hours: %s', e)
        return {'is_open': False}

    # ------------------------------------------------------------------
    # Legacy aliases
    # ------------------------------------------------------------------

    def submit_order(self, ticker: str, qty: float, side: str,
                     order_type: str = 'market', **kwargs) -> dict:
        """Alias for place_order using Alpaca-style side ('buy'/'sell')."""
        return self.place_order(ticker, qty, side, order_type=order_type)

    def get_open_orders(self) -> list:
        """Return list of open orders."""
        if not self._connected:
            return []
        try:
            r = requests.get(f'{self.base_url}/v2/orders',
                             params={'status': 'open'},
                             headers=self.headers, timeout=10)
            return r.json() if r.status_code == 200 else []
        except Exception:
            return []

    def cancel_order(self, order_id: str) -> bool:
        """Cancel an open order by ID."""
        if not self._connected:
            return False
        try:
            r = requests.delete(f'{self.base_url}/v2/orders/{order_id}',
                                headers=self.headers, timeout=10)
            return r.status_code in (200, 204)
        except Exception:
            return False

    def get_price(self, ticker: str) -> Optional[float]:
        """Fetch last price — tries Alpaca then yfinance fallback."""
        price = self.get_realtime_price(ticker)
        if price > 0:
            return price
        try:
            import yfinance as yf
            info = yf.Ticker(ticker).fast_info
            return float(info.last_price) if hasattr(info, 'last_price') else None
        except Exception:
            return None
