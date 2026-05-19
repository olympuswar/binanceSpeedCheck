import asyncio
import json
import time
import websockets

# Binance Aggregate Trade stream for BTC/USDT
WS_URL = "wss://stream.binance.com:9443/ws/btcusdt@aggTrade"

async def track_time_delta():
    print("Connecting to Binance WebSocket...")
    
    async with websockets.connect(WS_URL) as ws:
        print("Connected! Streaming timestamps (Ctrl+C to stop)...\n")
        print(f"{'Binance Event Time':<20} | {'Ireland Server Time':<20} | {'Difference (Latency)'}")
        print("-" * 68)
        
        while True:
            # 1. Wait for incoming trade data
            message = await ws.recv()
            
            # 2. Grab local server timestamp immediately upon packet arrival
            server_time_ms = int(time.time() * 1000)
            
            data = json.loads(message)
            
            # E = Event time (The timestamp Binance engine stamped on the trade in Tokyo)
            binance_event_time_ms = data.get('E')
            
            if binance_event_time_ms:
                # 3. Calculate total continental transit time
                diff_ms = server_time_ms - binance_event_time_ms
                
                print(f"{binance_event_time_ms:<20} | {server_time_ms:<20} | {diff_ms} ms")

if __name__ == "__main__":
    try:
        asyncio.run(track_time_delta())
    except KeyboardInterrupt:
        print("\nTest stopped by user.")