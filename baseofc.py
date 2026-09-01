#!/usr/bin/env python3
"""
Base Network Auto-Sweeper — ULTRA FAST
WebSocket + Mempool + Async mode
Install: pip install web3 colorama websockets aiohttp
Run:     python baseofc.py
"""

import asyncio
import sys
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

try:
    from web3 import Web3
    from web3.middleware import ExtraDataToPOAMiddleware
    from colorama import Fore, Style, init
    init(autoreset=True)
except ImportError:
    print("\n[!] Missing packages. Run:  pip install web3 colorama\n")
    sys.exit(1)

# ── Config ────────────────────────────────────────────────────────────────────
TOKEN_ADDRESSES = [
    "0x752C5a95d202972E124390F30a50154409d3c858",
    # "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",  # USDC
]

WSS_URL             = ""   # filled at runtime
HTTP_URL            = ""   # filled at runtime
PRIORITY_FEE_MULT   = 5.0
SWEEP_LOCK          = asyncio.Lock() if False else None  # init in main

ERC20_ABI = [
    {"name": "balanceOf", "type": "function", "inputs": [{"name": "account", "type": "address"}], "outputs": [{"name": "", "type": "uint256"}], "stateMutability": "view"},
    {"name": "transfer",  "type": "function", "inputs": [{"name": "to", "type": "address"}, {"name": "amount", "type": "uint256"}], "outputs": [{"name": "", "type": "bool"}], "stateMutability": "nonpayable"},
    {"name": "symbol",    "type": "function", "inputs": [], "outputs": [{"name": "", "type": "string"}], "stateMutability": "view"},
    {"name": "decimals",  "type": "function", "inputs": [], "outputs": [{"name": "", "type": "uint8"}], "stateMutability": "view"},
]

NETWORKS = {
    "1": {"http": "https://crimson-floral-gadget.base-mainnet.quiknode.pro/ccbc78715a130c2acb0ce2a31469572182f7c3d5/",
          "wss":  "wss://crimson-floral-gadget.base-mainnet.quiknode.pro/ccbc78715a130c2acb0ce2a31469572182f7c3d5/",
          "chain_id": 8453,  "label": "Base Mainnet (QuickNode)"},
    "2": {"http": "https://sepolia.base.org",
          "wss":  "wss://base-sepolia-rpc.publicnode.com",
          "chain_id": 84532, "label": "Base Sepolia (Testnet)"},
}

BANNER = f"""
{Fore.RED}
  ██╗    ██╗██╗  ██╗██╗████████╗███████╗      ██████╗ ███████╗██╗   ██╗██╗██╗
  ██║    ██║██║  ██║██║╚══██╔══╝██╔════╝      ██╔══██╗██╔════╝██║   ██║██║██║
  ██║ █╗ ██║███████║██║   ██║   █████╗  █████╗██║  ██║█████╗  ██║   ██║██║██║
  ██║███╗██║██╔══██║██║   ██║   ██╔══╝  ╚════╝██║  ██║██╔══╝  ╚██╗ ██╔╝██║██║
  ╚███╔███╔╝██║  ██║██║   ██║   ███████╗      ██████╔╝███████╗ ╚████╔╝ ██║███████╗
   ╚══╝╚══╝ ╚═╝  ╚═╝╚═╝   ╚═╝   ╚══════╝      ╚═════╝ ╚══════╝  ╚═══╝  ╚═╝╚══════╝
{Style.RESET_ALL}
{Fore.WHITE}                ──────────────────────────────────────────────{Style.RESET_ALL}
{Fore.YELLOW}            BASE AUTO-SWEEPER  ⚡  WEBSOCKET + MEMPOOL + ASYNC{Style.RESET_ALL}
{Fore.WHITE}                ──────────────────────────────────────────────{Style.RESET_ALL}
"""


def log(msg, color=Fore.WHITE):
    ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    print(f"{Fore.CYAN}[{ts}]{Style.RESET_ALL} {color}{msg}{Style.RESET_ALL}")


# ── EIP-1559 fees ─────────────────────────────────────────────────────────────
def get_fees(w3):
    try:
        block            = w3.eth.get_block("latest")
        base_fee         = block.get("baseFeePerGas", 100_000_000)
        max_priority_fee = int(w3.eth.max_priority_fee * PRIORITY_FEE_MULT)
        max_fee          = int(base_fee * 2) + max_priority_fee
        return max_fee, max_priority_fee
    except Exception:
        return w3.to_wei(5, "gwei"), w3.to_wei(3.5, "gwei")


# ── Parallel token fetch ──────────────────────────────────────────────────────
def fetch_token(w3, token_addr, wallet):
    addr     = Web3.to_checksum_address(token_addr)
    contract = w3.eth.contract(address=addr, abi=ERC20_ABI)
    try:
        symbol   = contract.functions.symbol().call()
        decimals = contract.functions.decimals().call()
    except Exception:
        symbol, decimals = "TOKEN", 18
    balance = contract.functions.balanceOf(wallet).call()
    return {"addr": addr, "symbol": symbol, "decimals": decimals,
            "balance": balance, "contract": contract}


# ── Core sweep logic ──────────────────────────────────────────────────────────
def do_sweep(w3, account, dest, chain_id, sweep_eth_flag, trigger="BLOCK"):
    max_fee, max_priority = get_fees(w3)
    nonce      = w3.eth.get_transaction_count(account.address, "pending")
    signed_txs = []
    token_gas  = 0

    # parallel token fetch
    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = {ex.submit(fetch_token, w3, ta, account.address): ta
                   for ta in TOKEN_ADDRESSES}
        results = {}
        for f in futures:
            try:
                results[futures[f]] = f.result()
            except Exception as e:
                log(f"[ TOKEN ERROR ] {e}", Fore.RED)

    cur_nonce = nonce

    # build token txs
    for ta in TOKEN_ADDRESSES:
        d = results.get(ta)
        if not d or d["balance"] == 0:
            if d:
                log(f"[ {d['symbol']} ] 0", Fore.WHITE)
            continue

        human = d["balance"] / (10 ** d["decimals"])
        fn    = d["contract"].functions.transfer(dest, d["balance"])
        try:
            gas = int(fn.estimate_gas({"from": account.address}) * 1.25)
        except Exception:
            gas = 65000

        tx = fn.build_transaction({
            "from": account.address, "gas": gas,
            "maxFeePerGas": max_fee, "maxPriorityFeePerGas": max_priority,
            "nonce": cur_nonce, "chainId": chain_id, "type": 2,
        })
        signed_txs.append((f"{d['symbol']} {human:.4f}", account.sign_transaction(tx)))
        token_gas += gas * max_fee
        cur_nonce += 1

    # build ETH tx
    if sweep_eth_flag:
        try:
            bal      = w3.eth.get_balance(account.address)
            eth_gas  = 21000 * max_fee
            reserve  = eth_gas + token_gas
            send_amt = None

            if bal > reserve:
                send_amt = bal - reserve
            elif bal > eth_gas and token_gas == 0:
                send_amt = bal - eth_gas

            if send_amt and send_amt > 0:
                tx = {
                    "to": dest, "value": send_amt, "gas": 21000,
                    "maxFeePerGas": max_fee, "maxPriorityFeePerGas": max_priority,
                    "nonce": cur_nonce, "chainId": chain_id, "type": 2,
                }
                signed_txs.append((f"ETH {w3.from_wei(send_amt,'ether'):.6f}",
                                    account.sign_transaction(tx)))
            else:
                log("[ ETH ] Too low", Fore.WHITE)
        except Exception as e:
            log(f"[ ETH ERROR ] {e}", Fore.RED)

    if not signed_txs:
        log(f"[ IDLE ] Nothing to sweep  ({trigger})", Fore.WHITE)
        return

    log(f"[ FIRE ] {len(signed_txs)} tx(s) — trigger: {trigger}", Fore.YELLOW)
    for label, signed in signed_txs:
        try:
            txhash = w3.eth.send_raw_transaction(signed.raw_transaction)
            log(f"[ SENT ] {label}  ->  {txhash.hex()[:22]}...", Fore.GREEN)
            log(f"[ LINK ] https://basescan.org/tx/{txhash.hex()}", Fore.GREEN)
        except Exception as e:
            log(f"[ FAIL ] {label} — {e}", Fore.RED)


# ── WebSocket listener (new block) ────────────────────────────────────────────
async def block_listener(w3, account, dest, chain_id, sweep_eth, lock):
    wss = w3.provider.endpoint_uri if hasattr(w3.provider, "endpoint_uri") else WSS_URL
    try:
        from web3 import AsyncWeb3
        from web3.providers import WebSocketProvider
        aw3 = AsyncWeb3(WebSocketProvider(WSS_URL))
        async with aw3 as conn:
            sub = await conn.eth.subscribe("newHeads")
            log("[ WS ] Block subscription active", Fore.GREEN)
            async for block in sub:
                num = block["number"] if isinstance(block, dict) else int(block.hex(), 16)
                log(f"[ BLOCK #{num} ] New block via WebSocket", Fore.BLUE)
                async with lock:
                    loop = asyncio.get_event_loop()
                    await loop.run_in_executor(
                        None, do_sweep, w3, account, dest, chain_id, sweep_eth, f"BLOCK #{num}"
                    )
    except Exception as e:
        log(f"[ WS ERROR ] {e} — falling back to polling", Fore.RED)
        await poll_fallback(w3, account, dest, chain_id, sweep_eth, lock)


# ── Mempool listener (pending tx TO source wallet) ────────────────────────────
async def mempool_listener(w3, account, dest, chain_id, sweep_eth, lock):
    try:
        from web3 import AsyncWeb3
        from web3.providers import WebSocketProvider
        aw3  = AsyncWeb3(WebSocketProvider(WSS_URL))
        addr = account.address.lower()

        async with aw3 as conn:
            sub = await conn.eth.subscribe("newPendingTransactions")
            log("[ MEMPOOL ] Pending tx subscription active", Fore.GREEN)
            async for tx_hash in sub:
                try:
                    tx = await conn.eth.get_transaction(tx_hash)
                    if tx and tx.get("to") and tx["to"].lower() == addr:
                        log(f"[ MEMPOOL ] Incoming tx detected! Hash: {tx_hash.hex()[:20]}...", Fore.YELLOW)
                        async with lock:
                            loop = asyncio.get_event_loop()
                            await loop.run_in_executor(
                                None, do_sweep, w3, account, dest, chain_id, sweep_eth, "MEMPOOL"
                            )
                except Exception:
                    pass
    except Exception as e:
        log(f"[ MEMPOOL ERROR ] {e}", Fore.RED)


# ── Fallback polling (if WebSocket fails) ─────────────────────────────────────
async def poll_fallback(w3, account, dest, chain_id, sweep_eth, lock):
    log("[ POLL ] Running block polling mode", Fore.YELLOW)
    last_block = 0
    cycle      = 0
    while True:
        try:
            cur = w3.eth.block_number
            if cur > last_block:
                cycle     += 1
                last_block = cur
                log(f"[ BLOCK #{cur} ]  Cycle {cycle}", Fore.BLUE)
                async with lock:
                    loop = asyncio.get_event_loop()
                    await loop.run_in_executor(
                        None, do_sweep, w3, account, dest, chain_id, sweep_eth, f"POLL #{cur}"
                    )
        except Exception as e:
            log(f"[ POLL ERROR ] {e}", Fore.RED)
        await asyncio.sleep(0)


# ── Input ─────────────────────────────────────────────────────────────────────
def get_inputs():
    print(BANNER)
    print(f"  {Fore.CYAN}Select Network:{Style.RESET_ALL}")
    print(f"  {Fore.YELLOW}1{Style.RESET_ALL}  ->  Base Mainnet  (QuickNode)")
    print(f"  {Fore.YELLOW}2{Style.RESET_ALL}  ->  Base Sepolia  (Testnet)")
    print()
    while True:
        choice = input(f"  {Fore.YELLOW}>{Style.RESET_ALL} ").strip()
        if choice in NETWORKS:
            net = NETWORKS[choice]
            break
        print(f"  {Fore.RED}Enter 1 or 2{Style.RESET_ALL}")

    global WSS_URL
    WSS_URL = net["wss"]

    print()
    while True:
        pk = input(f"  {Fore.YELLOW}Source Wallet Private Key:{Style.RESET_ALL} ").strip()
        if not pk:
            continue
        if not pk.startswith("0x"):
            pk = "0x" + pk
        try:
            acc = Web3().eth.account.from_key(pk)
            print(f"  {Fore.GREEN}Wallet loaded  ->  {acc.address}{Style.RESET_ALL}")
            break
        except Exception:
            print(f"  {Fore.RED}Invalid private key. Try again.{Style.RESET_ALL}")

    print()
    while True:
        dest = input(f"  {Fore.YELLOW}Destination Address (0x...):{Style.RESET_ALL} ").strip()
        if Web3.is_address(dest):
            dest = Web3.to_checksum_address(dest)
            print(f"  {Fore.GREEN}Destination set  ->  {dest}{Style.RESET_ALL}")
            break
        print(f"  {Fore.RED}Invalid address. Try again.{Style.RESET_ALL}")

    print()
    eth_in = input(f"  {Fore.YELLOW}Sweep ETH as well? (y/n, default y):{Style.RESET_ALL} ").strip().lower()
    print()
    return pk, dest, net, eth_in != "n"


# ── Main ──────────────────────────────────────────────────────────────────────
async def async_main():
    try:
        pk, dest, net, sweep_eth = get_inputs()
    except KeyboardInterrupt:
        print(f"\n{Fore.RED}  Cancelled.{Style.RESET_ALL}")
        sys.exit(0)

    w3 = Web3(Web3.HTTPProvider(net["http"], request_kwargs={"timeout": 5}))
    if not w3.is_connected():
        print(f"  {Fore.RED}HTTP RPC connection failed.{Style.RESET_ALL}")
        sys.exit(1)

    account  = w3.eth.account.from_key(pk)
    chain_id = net["chain_id"]
    lock     = asyncio.Lock()

    print(f"  {Fore.WHITE}{'─'*58}{Style.RESET_ALL}")
    log(f"  Network     :  {net['label']}  (Chain {chain_id})", Fore.CYAN)
    log(f"  Source      :  {account.address}", Fore.CYAN)
    log(f"  Destination :  {dest}", Fore.CYAN)
    log(f"  Tokens      :  {len(TOKEN_ADDRESSES)} contract(s)", Fore.CYAN)
    log(f"  ETH Sweep   :  {'ON' if sweep_eth else 'OFF'}", Fore.CYAN)
    log(f"  Gas         :  EIP-1559  |  {PRIORITY_FEE_MULT}x priority", Fore.CYAN)
    log(f"  Mode        :  WebSocket Block + Mempool + Async", Fore.CYAN)
    print(f"  {Fore.WHITE}{'─'*58}{Style.RESET_ALL}\n")

    # run block listener + mempool listener concurrently
    await asyncio.gather(
        block_listener(w3, account, dest, chain_id, sweep_eth, lock),
        mempool_listener(w3, account, dest, chain_id, sweep_eth, lock),
    )


def main():
    try:
        asyncio.run(async_main())
    except KeyboardInterrupt:
        log("[ STOP ] Sweeper stopped.", Fore.RED)


if __name__ == "__main__":
    main()
