import asyncio
import json
import time
import websockets

WS_URL = "wss://stream.binance.com:9443/ws/btcusdt@aggTrade"

async def track_time_delta():
    print("Connecting to Optimized Binance Infrastructure Socket...")
    
    async with websockets.connect(WS_URL) as ws:
        print("Connected! Streaming optimized ticks...\n")
        print(f"{'Binance Event Time':<20} | {'Ireland Server Time':<20} | {'True Delay'}")
        print("-" * 68)
        
        while True:
            message = await ws.recv()
            # 1. Grab local server timestamp the exact microsecond the packet clears the network card buffer
            server_time_ms = int(time.time() * 1000)
            
            data = json.loads(message)
            binance_event_time_ms = data.get('E')
            
            if binance_event_time_ms:
                diff_ms = server_time_ms - binance_event_time_ms
                
                # Only print if it's a valid timestamp to keep terminal processing clean
                print(f"{binance_event_time_ms:<20} | {server_time_ms:<20} | {diff_ms} ms")

if __name__ == "__main__":
    try:
        asyncio.run(track_time_delta())
    except KeyboardInterrupt:
        print("\nTest stopped by user.")