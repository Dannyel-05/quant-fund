"""
ChunkedScanner — scans large universes in batches of 200 tickers.
Avoids timeouts when processing 2000+ stocks.
"""
import logging
import time
from datetime import datetime
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

TIER_SIZE_MULTIPLIERS = {
    'TIER_1': 1.0,
    'TIER_2': 0.8,
    'TIER_3': 0.6,
    'MICRO':  0.4,
    'UK_1':   1.0,
    'UK_2':   0.7,
    'UNKNOWN': 0.5,
}


class ChunkedScanner:
    """
    Splits a large ticker universe into chunks of `chunk_size` tickers,
    fetches price data for each chunk as a batch, then runs all signals
    on every ticker in the chunk.
    """

    def __init__(
        self,
        paper_trader,
        chunk_size: int = 200,
        max_workers: int = 1,
        delay_between_chunks: float = 1.5,
    ):
        self.trader = paper_trader
        self.chunk_size = chunk_size
        self.max_workers = max_workers
        self.delay = delay_between_chunks

    # ------------------------------------------------------------------
    def scan_chunk(
        self,
        tickers_chunk: List[str],
        chunk_num: int,
        total_chunks: int,
        account_equity: float,
        max_new_positions: int,
    ) -> Dict:
        """Process one chunk. Returns dict with counts."""
        result = {
            'chunk': chunk_num,
            'tickers_in_chunk': len(tickers_chunk),
            'tickers_processed': 0,
            'signals_found': 0,
            'positions_opened': 0,
            'errors': 0,
        }
        if not tickers_chunk:
            return result

        # Batch-fetch price data for whole chunk
        logger.info('  Chunk %d/%d: fetching prices for %d tickers',
                    chunk_num, total_chunks, len(tickers_chunk))
        all_prices: Dict = {}
        try:
            from datetime import timedelta
            import pandas as pd
            end_date = datetime.now().strftime('%Y-%m-%d')
            start_date = (datetime.now() - timedelta(days=600)).strftime('%Y-%m-%d')
            if self.trader.fetcher:
                all_prices = self.trader.fetcher.fetch_universe_data(
                    tickers_chunk, start_date, end_date, 'us')
        except Exception as e:
            logger.warning('  Chunk %d price fetch error: %s', chunk_num, e)

        # Process each ticker
        for ticker in tickers_chunk:
            if result['positions_opened'] >= max_new_positions:
                break
            try:
                price_data = all_prices.get(ticker)
                if price_data is None or len(price_data) < 20:
                    continue

                # Build context
                context = {}
                try:
                    context = self.trader.build_full_context(ticker)
                except Exception:
                    pass

                # Skip if crisis filing
                if context.get('has_crisis_filing'):
                    continue

                # Run all signals
                signals = []
                try:
                    signals = self.trader.run_all_signals(ticker, price_data, context)
                except Exception:
                    pass

                result['tickers_processed'] += 1
                if not signals:
                    continue

                result['signals_found'] += len(signals)

                # Find best signal
                best = max(signals, key=lambda s: float(s.get('score', 0)))
                best_score = float(best.get('score', 0))

                # Phase threshold check
                sizer = self.trader.sizer
                if sizer and not sizer.should_trade(best_score,
                                                     signal_type=best.get('signal_type'),
                                                     context=context):
                    continue

                # Tier-based size reduction
                tier = 'UNKNOWN'
                if self.trader.universe:
                    tier = self.trader.universe.get_ticker_tier(ticker)
                tier_mult = TIER_SIZE_MULTIPLIERS.get(tier, 0.5)

                # Size position
                sizing = {'position_value': 100, 'position_pct': 0.001,
                          'phase': 'PHASE_1', 'scaling_reason': 'default'}
                if sizer:
                    try:
                        sizing = sizer.size_position(
                            best_score,
                            best.get('signal_type', 'UNKNOWN'),
                            account_equity,
                            ticker,
                            context,
                            signals,
                        )
                    except Exception:
                        pass

                final_value = sizing['position_value'] * tier_mult
                if final_value < 50:
                    continue

                # Current price
                try:
                    current_price = float(price_data['close'].iloc[-1]
                                          if 'close' in price_data.columns
                                          else price_data.iloc[:, 3].iloc[-1])
                    if current_price <= 0:
                        continue
                except Exception:
                    continue

                shares = max(1, int(final_value / current_price))
                side = best.get('direction', 'LONG')
                order_dir = 'buy' if side == 'LONG' else 'short'

                # Place order
                success = False
                try:
                    if self.trader.use_alpaca and self.trader.broker:
                        order_result = self.trader.broker.place_order(
                            ticker, shares, order_dir, current_price)
                        success = order_result.get('status') == 'filled'
                    elif self.trader.broker:
                        order_result = self.trader.broker.place_order(
                            ticker, shares, order_dir, current_price)
                        success = order_result.get('status') == 'filled'
                except Exception as e:
                    logger.debug('Order failed %s: %s', ticker, e)

                if success:
                    result['positions_opened'] += 1
                    # Record in closeloop
                    try:
                        if self.trader.closeloop:
                            self.trader.closeloop.open_trade(
                                ticker=ticker,
                                direction=side,
                                entry_price=current_price,
                                shares=float(shares),
                                position_value=final_value,
                                signal_type=best.get('signal_type', 'UNKNOWN'),
                                signal_score=best_score,
                                all_signals=signals,
                                context=context,
                                sizing_reasoning=sizing['scaling_reason'] + f' tier_mult={tier_mult}',
                                phase=sizing.get('phase', 'PHASE_1'),
                            )
                    except Exception as e:
                        logger.debug('open_trade record failed: %s', e)

                    logger.info('    OPENED %s %s x%d @ $%.2f ($%.0f) [%s %.2f] %s',
                                side, ticker, shares, current_price,
                                final_value, best.get('signal_type', '?'), best_score, tier)

            except Exception as e:
                result['errors'] += 1
                logger.debug('Chunk ticker error %s: %s', ticker, e)

        return result

    # ------------------------------------------------------------------
    def scan_all(
        self,
        tickers: List[str],
        account_equity: float,
        max_new_positions: int,
    ) -> Dict:
        """Split tickers into chunks and scan all of them."""
        chunks = [tickers[i:i + self.chunk_size]
                  for i in range(0, len(tickers), self.chunk_size)]
        total_chunks = len(chunks)

        totals = {
            'total_tickers': len(tickers),
            'total_chunks': total_chunks,
            'chunk_size': self.chunk_size,
            'tickers_processed': 0,
            'signals_found': 0,
            'positions_opened': 0,
            'errors': 0,
            'chunks_completed': 0,
        }

        print(f'ChunkedScanner: {len(tickers)} tickers | '
              f'{total_chunks} chunks of {self.chunk_size} | '
              f'~{total_chunks * 12}s estimated')

        for i, chunk in enumerate(chunks):
            if totals['positions_opened'] >= max_new_positions:
                print(f'  Position limit reached ({max_new_positions}). Stopping scan.')
                break

            remaining_slots = max_new_positions - totals['positions_opened']
            chunk_result = self.scan_chunk(
                chunk, i + 1, total_chunks, account_equity, remaining_slots)

            for key in ('tickers_processed', 'signals_found', 'positions_opened', 'errors'):
                totals[key] += chunk_result[key]
            totals['chunks_completed'] += 1

            pct = (i + 1) / total_chunks * 100
            print(f'  Chunk {i+1}/{total_chunks} ({pct:.0f}%) | '
                  f'signals={totals["signals_found"]} | '
                  f'opened={totals["positions_opened"]}')

            if i < total_chunks - 1:
                time.sleep(self.delay)

        return totals
