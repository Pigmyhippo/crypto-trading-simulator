"""
Automated trading simulator

This script fetches real‑time cryptocurrency prices from Binance's public API
and implements a simple moving average crossover strategy. It maintains a
virtual portfolio in an SQLite database and executes trades based on the
strategy rules. The simulator runs continuously using APScheduler to fetch
data at a configured interval (default: every five minutes).

Important: This simulator is for educational purposes only. It executes
virtual trades and does not place any real orders with Binance or any other
exchange.
"""

import datetime as dt
import json
import sqlite3
import threading
from typing import List, Dict

import requests


class TradingSimulator:
    """A simple moving‑average crossover trading simulator for crypto."""

    def __init__(
        self,
        symbols: List[str],
        db_path: str = "trading_sim.db",
        fetch_interval_minutes: int = 5,
        short_ma: int = 5,
        long_ma: int = 20,
        position_size_fraction: float = 0.05,
        starting_balance: float = 10000.0,
    ) -> None:
        self.symbols = symbols
        self.db_path = db_path
        self.fetch_interval_minutes = fetch_interval_minutes
        self.short_ma = short_ma
        self.long_ma = long_ma
        self.position_size_fraction = position_size_fraction

        # set up database
        self._init_db(starting_balance)

    def _init_db(self, starting_balance: float) -> None:
        """Initialise SQLite database with required tables."""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        # Table to store price history
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS prices (
                timestamp TEXT NOT NULL,
                symbol TEXT NOT NULL,
                price REAL NOT NULL,
                PRIMARY KEY (timestamp, symbol)
            )
            """
        )
        # Table to store trades
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                symbol TEXT NOT NULL,
                action TEXT NOT NULL,
                price REAL NOT NULL,
                quantity REAL NOT NULL,
                balance REAL NOT NULL
            )
            """
        )
        # Table to store portfolio state
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS portfolio (
                symbol TEXT PRIMARY KEY,
                quantity REAL NOT NULL
            )
            """
        )
        # Table to store cash balance
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS cash (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                balance REAL NOT NULL
            )
            """
        )
        # Initialise cash balance if not present
        c.execute("SELECT balance FROM cash WHERE id = 1")
        row = c.fetchone()
        if row is None:
            c.execute("INSERT INTO cash (id, balance) VALUES (1, ?)", (starting_balance,))
        conn.commit()
        conn.close()

    def _fetch_price(self, symbol: str) -> float:
        """
        Fetch the latest price for a symbol from Binance API.
        This uses the public ticker price endpoint. Returns the price as float.
        """
        url = f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}"
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            return float(data["price"])
        except Exception as e:
            print(f"Error fetching price for {symbol}: {e}")
            return None

    def _store_price(self, timestamp: dt.datetime, symbol: str, price: float) -> None:
        """Store price into the database."""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute(
            "INSERT OR REPLACE INTO prices (timestamp, symbol, price) VALUES (?, ?, ?)",
            (timestamp.isoformat(), symbol, price),
        )
        conn.commit()
        conn.close()

    def _get_recent_prices(self, symbol: str, periods: int) -> List[float]:
        """Retrieve the most recent `periods` prices for a symbol."""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute(
            "SELECT price FROM prices WHERE symbol = ? ORDER BY timestamp DESC LIMIT ?",
            (symbol, periods),
        )
        rows = c.fetchall()
        conn.close()
        return [row[0] for row in reversed(rows)]  # return in chronological order

    def _get_cash_balance(self) -> float:
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute("SELECT balance FROM cash WHERE id = 1")
        row = c.fetchone()
        conn.close()
        return row[0] if row else 0.0

    def _update_cash_balance(self, new_balance: float) -> None:
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute("UPDATE cash SET balance = ? WHERE id = 1", (new_balance,))
        conn.commit()
        conn.close()

    def _get_portfolio_quantity(self, symbol: str) -> float:
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute("SELECT quantity FROM portfolio WHERE symbol = ?", (symbol,))
        row = c.fetchone()
        conn.close()
        return row[0] if row else 0.0

    def _update_portfolio_quantity(self, symbol: str, quantity: float) -> None:
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        if quantity == 0:
            c.execute("DELETE FROM portfolio WHERE symbol = ?", (symbol,))
        else:
            c.execute(
                "INSERT OR REPLACE INTO portfolio (symbol, quantity) VALUES (?, ?)",
                (symbol, quantity),
            )
        conn.commit()
        conn.close()

    def _log_trade(
        self,
        timestamp: dt.datetime,
        symbol: str,
        action: str,
        price: float,
        quantity: float,
        balance: float,
    ) -> None:
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute(
            """
            INSERT INTO trades (timestamp, symbol, action, price, quantity, balance)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (timestamp.isoformat(), symbol, action, price, quantity, balance),
        )
        conn.commit()
        conn.close()

    def _execute_trade(self, symbol: str, action: str, price: float) -> None:
        """
        Execute a virtual trade. For buy actions, we spend a fraction of the cash
        balance; for sell actions we liquidate the entire position of the symbol.
        """
        timestamp = dt.datetime.utcnow()
        cash = self._get_cash_balance()
        quantity_owned = self._get_portfolio_quantity(symbol)

        if action == "BUY":
            # Determine the amount to invest based on position_size_fraction
            spend = cash * self.position_size_fraction
            if spend < 0.0001:
                # Too little cash to place an order
                print(f"{timestamp}: Not enough cash to buy {symbol}")
                return
            qty = spend / price
            new_cash = cash - spend
            new_qty = quantity_owned + qty
            self._update_cash_balance(new_cash)
            self._update_portfolio_quantity(symbol, new_qty)
            self._log_trade(timestamp, symbol, action, price, qty, new_cash)
            print(f"{timestamp}: Bought {qty:.6f} {symbol} at {price:.2f}, new cash balance {new_cash:.2f}")
        elif action == "SELL" and quantity_owned > 0:
            # Sell entire position
            qty = quantity_owned
            proceeds = qty * price
            new_cash = cash + proceeds
            self._update_cash_balance(new_cash)
            self._update_portfolio_quantity(symbol, 0.0)
            self._log_trade(timestamp, symbol, action, price, -qty, new_cash)
            print(f"{timestamp}: Sold {qty:.6f} {symbol} at {price:.2f}, new cash balance {new_cash:.2f}")
        else:
            # No action needed
            pass

    def _evaluate_strategy(self, symbol: str) -> None:
        """
        Evaluate the strategy for the given symbol and execute trades accordingly.
        The strategy: if the short moving average crosses above the long moving
        average, buy; if it crosses below, sell; otherwise, hold.
        """
        prices = self._get_recent_prices(symbol, periods=self.long_ma + 1)
        if len(prices) < self.long_ma + 1:
            # Not enough data yet
            return
        # compute moving averages excluding the current price
        short_ma_old = sum(prices[-(self.short_ma + 1):-1]) / self.short_ma
        long_ma_old = sum(prices[-(self.long_ma + 1):-1]) / self.long_ma
        short_ma_new = sum(prices[-self.short_ma:]) / self.short_ma
        long_ma_new = sum(prices[-self.long_ma:]) / self.long_ma

        # Determine crossovers
        if short_ma_old <= long_ma_old and short_ma_new > long_ma_new:
            # Golden cross: buy signal
            current_price = prices[-1]
            self._execute_trade(symbol, "BUY", current_price)
        elif short_ma_old >= long_ma_old and short_ma_new < long_ma_new:
            # Death cross: sell signal
            current_price = prices[-1]
            self._execute_trade(symbol, "SELL", current_price)
        else:
            # Hold
            pass

    def _job(self):
        """Job executed at each scheduled interval."""
        timestamp = dt.datetime.utcnow()
        for symbol in self.symbols:
            price = self._fetch_price(symbol)
            if price is not None:
                self._store_price(timestamp, symbol, price)
                self._evaluate_strategy(symbol)

    def run(self) -> None:
        """
        Run the simulator in a continuous loop.

        At each iteration it fetches the latest price data, stores it in the
        database and evaluates the trading strategy for each symbol. The loop
        then sleeps until the next interval. This implementation avoids
        external scheduling libraries and relies on Python's built‑in
        threading.Event to wait between iterations.
        """
        print(
            f"Trading simulator started. Fetching prices every {self.fetch_interval_minutes} minutes."
        )
        while True:
            start_time = dt.datetime.utcnow()
            self._job()
            elapsed = (dt.datetime.utcnow() - start_time).total_seconds()
            sleep_seconds = max(0, self.fetch_interval_minutes * 60 - elapsed)
            threading.Event().wait(sleep_seconds)



def main():
    # Define the symbols to trade: using Binance symbols like BTCUSDT and ETHUSDT
    symbols = ["BTCUSDT", "ETHUSDT"]
    simulator = TradingSimulator(
        symbols=symbols,
        db_path="trading_sim.db",
        fetch_interval_minutes=5,
        short_ma=5,
        long_ma=20,
        position_size_fraction=0.05,
        starting_balance=10000.0,
    )
    try:
        simulator.run()
    except (KeyboardInterrupt, SystemExit):
        print("Simulator stopped.")


if __name__ == "__main__":
    main()
