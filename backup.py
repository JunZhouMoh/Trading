import json
import websocket
import threading
import time
import requests
import os
import asyncio
import websockets
import re
##from py_clob_client.client import ClobClient
##from py_clob_client.clob_types import OrderArgs
##from py_clob_client.order_builder.constants import BUY
from py_clob_client_v2 import ClobClient, OrderArgs, PartialCreateOrderOptions
from py_clob_client_v2.order_builder.constants import BUY
from eth_account import Account
from dotenv import load_dotenv

load_dotenv()

# Global client instance (lazy initialization)
_clob_client = None

class PolymarketLive:
    def __init__(self):
        self.strike_price = 0.0
        self.current_market_start = 0
        self.ws_url = "wss://ws-live-data.polymarket.com"
        self.current_token_ids = None
        self.latest_prices = {}  # Thread-safe dictionary for price caching
        self.loop = asyncio.new_event_loop() # Dedicated loop for Poly WSS
        self.result=[]
        self.current_market_price = 0.0
        self.Buy_signal = []
        self.traded = False  # Flag to prevent multiple trades in the same market
        self.current_market_price_yes = 0.0
        self.current_market_price_no = 0.0
        self.client = self.get_clob_client()

    
    def get_clob_client(self):
        """Initialize and return the CLOB client."""
        global _clob_client
        if _clob_client is None:
            host = "https://clob.polymarket.com"
            chain_id = 137  # Polygon mainnet
            private_key = os.getenv("POLY_PK")
            
            if not private_key:
                raise ValueError("POLY_PK not found in environment variables")
            
            # Derive API credentials
            temp_client = ClobClient(host, key=private_key, chain_id=chain_id)
            ##api_creds = temp_client.create_or_derive_api_creds()
            api_creds = temp_client.create_or_derive_api_key()
            
            # Initialize trading client
            _clob_client = ClobClient(
                host, 
                key=private_key, 
                chain_id=chain_id, 
                creds=api_creds,
                signature_type=1,
                funder=os.getenv("FUNDER")
            )
        return _clob_client
    def execute_buy(self,token_id: str, price: float, size: float) -> dict:
        """
        Execute a buy order on Polymarket.
        
        Args:
            token_id: The token ID to buy (yes_token or no_token)
            price: The price to buy at (e.g., 0.50 for 50 cents)
            size: The number of shares to buy
            
        Returns:
            dict: Order response with orderID and status
        """
        try:
            size=6/price            
            order_args = OrderArgs(
                token_id=token_id,
                price=price,
                size=float(os.getenv("ORDER_SIZE", 5.0)),  # Default to 5.0 if not set
                side=BUY
            )
            print(order_args)
            response = self.client.create_and_post_order(order_args)
            
            print(f"   Status: {response.get('status')}")
            
            return response
            
        except Exception as e:
            print(f"❌ Failed to execute buy: {e}")
            return {"error": str(e)}

    def get_market_ids(self, slug):
        url = f"https://gamma-api.polymarket.com/markets?slug={slug.strip()}"
        try:
            response = requests.get(url)
            data = response.json()
            if not data: return None
            market = data[0]
            raw_tokens = market.get("clobTokenIds")
            token_ids = json.loads(raw_tokens) if isinstance(raw_tokens, str) else raw_tokens
            question = market.get("question") or ""
            print("question",question)
            match = re.search(r"\$([\d,]+\.?\d*)", question)
            strike_price = float(match.group(1).replace(",", "")) if match else 0.0
            print(f"🔍 Fetched New Market: {question} | Strike Price: ${strike_price} | Yes Token: {token_ids[0]} | No Token: {token_ids[1]}")
            return {
                "question": question,
                "yes_token": token_ids[0],
                "no_token": token_ids[1],
                "strike_price": strike_price
            }
        except Exception as e:
            print(f"❌ Gamma API Error: {e}")
            return None

    # --- POLYMARKET WSS LOGIC ---
    async def price_listener(self, token_ids):
            uri = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
            while True:
                try:
                    async with websockets.connect(uri) as ws:
                        subscribe_msg = {
                            "type": "subscribe",
                            "assets_ids": token_ids,
                            "event_types": ["best_bid_ask", "book", "last_trade_price"],
                            "custom_feature_enabled": True 
                        }
                        await ws.send(json.dumps(subscribe_msg))
                        
                        async for message in ws:
                            raw_data = json.loads(message)
                            
                            # Fix: Polymarket often sends a list of updates
                            updates = raw_data if isinstance(raw_data, list) else [raw_data]
                            
                            for msg in updates:
                                event = msg.get("event_type")
                                tid = msg.get("asset_id")
                                
                                if not tid: continue

                                # 1. Best Bid/Ask (Highest Frequency)
                                if event == "best_bid_ask":
                                    self.latest_prices[tid] = float(msg.get("best_ask", 0.5))

                                # 2. Book Update (If someone changes a limit order)
                                elif event == "book":
                                    if msg.get("asks") and len(msg["asks"]) > 0:
                                        self.latest_prices[tid] = float(msg["asks"][0]["price"])

                                # 3. Last Trade (Actual execution)
                                elif event == "last_trade_price":
                                    self.latest_prices[tid] = float(msg.get("price", 0.5))
                                    
                except Exception as e:
                    print(f"🔄 Poly WSS Error: {e}. Reconnecting...")
                    await asyncio.sleep(2)

    def start_poly_wss(self, token_ids):
        """Helper to run the async listener in its own thread."""
        def run_async():
            asyncio.set_event_loop(self.loop)
            self.loop.run_until_complete(self.price_listener(token_ids))
        
        threading.Thread(target=run_async, daemon=True).start()

    def get_market_price(self, token_id):
        """
        Checks the local WSS cache first. 
        If data is missing (0.5), it fetches the actual price via REST.
        """
        
        # Fallback to REST API if WSS is stale/empty
        try:
            url = f"https://clob.polymarket.com/price?token_id={token_id}&side=BUY"
            resp = requests.get(url, timeout=1).json()
            price = float(resp.get("price", 0.5))
            self.latest_prices[token_id] = price # Cache it for the next call
            return price
        except:
            return 10000

    # --- TRADING LOGIC ---
    def evaluate_trade(self, btc_price, seconds_left):
        BASE_DIFF = 30
        STEP_TIME = 60
        MAX_TIME = 300
        MAX_PRICE_LIMIT = float(os.getenv("MAX_PRICE_LIMIT", 0.9))  # Default to 0.5 if not set

        if not self.current_token_ids:
            return None

        if seconds_left > MAX_TIME:
            return None

        # Dynamic diff calculation
        # e.g. 0–60 → 1 * 30, 60–120 → 2 * 30, etc.
        time_bucket = max(1, int(seconds_left // STEP_TIME))
        min_diff = BASE_DIFF * time_bucket

        diff = btc_price - self.strike_price

        target_side = (
            'yes_token' if diff > min_diff
            else 'no_token' if diff < -min_diff
            else None
        )

        if not target_side:
            return None

        token_id = self.current_token_ids[target_side]
        self.current_market_price = self.get_market_price(token_id)

        if self.current_market_price > MAX_PRICE_LIMIT:
            return None

        return {
            "side": "BUY",
            "token": token_id,
            "label": "YES" if target_side == 'yes_token' else "NO",
            "price": self.current_market_price,
            "diff": diff
        }

    def on_message(self, ws, message):
        try:
            data = json.loads(message)
            payload = data.get("payload", {})
            
            btc_price = float(payload.get("value", 0))

            now = time.time()
            window_start = int(now - (now % 300))
            seconds_left = 300 - (now % 300)
                
            # Market Rotation
            if window_start > self.current_market_start:
                new_slug = self.get_current_5m_slug()
                market_data = self.get_market_ids(new_slug)
                if not market_data:
                    print(f"❌ Market not ready yet for slug {new_slug}. Waiting for strike price...")
                    return

                self.current_token_ids = market_data
                self.strike_price = market_data.get("strike_price", 0.0)
                self.current_market_start = window_start
                print(f"\n{'='*40}\n✨ NEW MARKET: {new_slug} | Strike Price: ${self.strike_price}\nBTC Feed: ${btc_price}\nStarting time: {time.ctime(window_start)}\n{'='*40} ")
                self.traded = False  # Reset trade flag for new market

            if not self.current_token_ids:
                return

            diff = btc_price - self.strike_price
            status = "🟢 UP" if diff > 0 else "🔴 DOWN"
            self.current_market_price_yes = self.get_market_price(self.current_token_ids['yes_token']) if self.current_token_ids else 0.0
            self.current_market_price_no = self.get_market_price(self.current_token_ids['no_token']) if self.current_token_ids else 0.0
            trade = self.evaluate_trade(btc_price, seconds_left)
                
            print(f"💰 BTC: ${btc_price:,.2f} | Diff: {diff:+.2f} ({status}) | ⏳ {int(seconds_left)}s left | market_price_yes: ${self.current_market_price_yes:.2f} | market_price_no: ${self.current_market_price_no:.2f}", end="\r" )
                
            if trade and diff <30000:  # Basic sanity check to avoid crazy signals                
                # Execute the buy order
                if self.traded!=True:
                    print(trade)
                    self.traded = True
                    result = self.execute_buy(
                    token_id=trade['token'],
                    price=trade['price'],
                    size=1.0  # Default size of 1 share
                    )
                    trade_diff = diff
                    trade_strike = trade['price']
                    trade_time = time.ctime(now)

                    print(f"📋 Order Result: {result}")
            if int(seconds_left) == 0 and self.traded==True:
                overall_outcome = "YES" if (btc_price > self.strike_price and diff >0) or (btc_price < self.strike_price and diff <0) else "NO"
                self.Buy_signal.append({"overall_outcome": overall_outcome,"diff": trade_diff, "strike_price": trade_strike, "market_price": self.current_market_price, "time": trade_time})
                with open("Buy_Signals.json", "w") as f:
                        json.dump(self.Buy_signal, f, indent=4)
                print(f"\n⏰ Market Closed! Final Outcome: {overall_outcome}")
        except Exception as e:
            print(f"\n❌ Error: {e}")

    # ... (rest of your existing methods like get_current_5m_slug, on_open, run) ...

    def get_current_5m_slug(self):
        current_unix = int(time.time())
        window_start = current_unix - (current_unix % 300)
        return f"btc-updown-5m-{window_start}"

    def on_open(self, ws):
        subscribe_msg_1hr = {
            "action": "subscribe",
            "subscriptions": [{"topic": "crypto_prices", "type": "update"}]
        }

        subscibe_msg_5mints = {
            "action": "subscribe",
            "subscriptions": [
                {
                "topic": "crypto_prices_chainlink",
                "type": "*",
                "filters": "{\"symbol\":\"btc/usd\"}"
                }
            ]
            }
        ##ws.send(json.dumps(subscribe_msg_1hr))
        ws.send(json.dumps(subscibe_msg_5mints))

    def run(self):
        ws = websocket.WebSocketApp(
            self.ws_url,
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=lambda ws, err: print(f"Socket Error: {err}")
        )
        ws.run_forever(ping_interval=15)

if __name__ == "__main__":
    bot = PolymarketLive()
    initial_slug = bot.get_current_5m_slug()
    ids = bot.get_market_ids(initial_slug)
    
    if ids:
        bot.current_token_ids = ids
        bot.strike_price = ids.get("strike_price", 0.0)
        # Start the Polymarket Price Stream
        bot.start_poly_wss([ids['yes_token'], ids['no_token']])
    else:
        print(f"❌ Initial market not ready for slug {initial_slug}. Waiting for the first market window.")
    
    # Start the BTC Price Stream (Primary Thread)
    bot.run()