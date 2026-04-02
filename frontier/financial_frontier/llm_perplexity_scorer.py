"""
LLM Perplexity Scorer — lpas_mean Signal.

Computes the LLM Perplexity Anomaly Score (LPAS) from financial news
headlines by measuring how "surprising" current language is to a GPT-2
model trained on general English corpora.

Economic hypothesis
-------------------
Language models trained on large corpora assign probability scores to
token sequences.  When applied to financial news, elevated perplexity
(i.e. unusually surprising language patterns) signals that market
participants are discussing genuinely novel events — situations for which
the market may not yet have a pricing template.

Specific mechanisms:
  1. Headline novelty -> delayed price discovery: markets are better at
     pricing recurrent events (earnings beats, Fed rate decisions) than
     structurally novel ones (first AI-driven bank run, novel contagion
     pathway).  High perplexity in news headlines is a proxy for novelty
     not yet incorporated in prices.

  2. Communication stress signal: when individual companies or sectors
     appear with unusual phrasing, it may signal corporate communication
     under duress — a complementary signal to transcript tone analysis.

  3. Regime change detector: rapid perplexity spikes have historically
     coincided with volatility regime changes (black swan onset, policy
     pivot surprises).

LPAS formula (from derived_formulas.py):
    LPAS = (current_perplexity - rolling_mean) / rolling_std

  - LPAS > +2.0: statistically unusual language -> elevated novelty risk
  - LPAS < -2.0: unusually formulaic language -> possible boilerplate
    saturation or low-information environment

Perplexity measurement
----------------------
Primary: GPT-2 (124M parameters) loaded via HuggingFace transformers.
  - Model is open-weight, free to use, no API key required.
  - Inference is local; first run downloads ~500MB model weights.
  - Perplexity = exp(mean negative log-likelihood) over headline tokens.

Fallback (if transformers unavailable): text length variance proxy.
  - Computes std of sentence lengths in characters.
  - Scales to a pseudo-perplexity value.
  - quality_score = 0.4 to flag lower confidence.

Rolling history
---------------
Last 30 LPAS-relevant readings stored in logs/lpas_history.json.
Used to compute rolling_mean and rolling_std for LPAS calculation.

Data source
-----------
yfinance Ticker.news (free, no key): recent news for SPY/QQQ/IWM.
HuggingFace transformers / GPT-2 (optional, free, open-weight).
"""

import json
import logging
import os
from datetime import datetime, timezone
from typing import Optional

from frontier.equations.derived_formulas import calc_lpas

logger = logging.getLogger(__name__)

_DEFAULT_TICKERS = ["SPY", "QQQ", "IWM"]
_HISTORY_FILE = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "..", "logs", "lpas_history.json")
)
_MAX_HISTORY = 30
_MAX_TOKENS = 512
_SOURCE = "yfinance news + GPT-2 perplexity (HuggingFace transformers)"


class LLMPerplexityScorer:
    """
    Fetches recent financial news headlines, computes text perplexity
    using GPT-2 (or a length-variance proxy), maintains a rolling history,
    and returns the LPAS signal.

    GPT-2 model is cached at class level to avoid repeated disk loads
    across multiple collect() calls within the same process.
    """

    # Class-level model cache
    _gpt2_model = None
    _gpt2_tokenizer = None
    _gpt2_available: Optional[bool] = None  # None = not yet attempted

    # ---------------------------------------------------------------------------
    # Model loading
    # ---------------------------------------------------------------------------

    def _load_gpt2(self) -> bool:
        """
        Attempt to load GPT-2 model and tokenizer from HuggingFace.

        Returns True if successful, False otherwise.  Result is cached at
        class level so the load only happens once per process.
        """
        if LLMPerplexityScorer._gpt2_available is not None:
            return LLMPerplexityScorer._gpt2_available

        try:
            from transformers import GPT2LMHeadModel, GPT2TokenizerFast

            logger.info("LLMPerplexityScorer: loading GPT-2 model (one-time)...")
            LLMPerplexityScorer._gpt2_tokenizer = GPT2TokenizerFast.from_pretrained(
                "gpt2"
            )
            LLMPerplexityScorer._gpt2_model = GPT2LMHeadModel.from_pretrained("gpt2")
            LLMPerplexityScorer._gpt2_model.eval()
            LLMPerplexityScorer._gpt2_available = True
            logger.info("LLMPerplexityScorer: GPT-2 loaded successfully.")
            return True
        except ImportError:
            logger.info(
                "LLMPerplexityScorer: transformers not installed; "
                "will use length-variance proxy."
            )
            LLMPerplexityScorer._gpt2_available = False
        except Exception as exc:
            logger.warning(
                "LLMPerplexityScorer: GPT-2 load error: %s; "
                "will use length-variance proxy.",
                exc,
            )
            LLMPerplexityScorer._gpt2_available = False
        return False

    # ---------------------------------------------------------------------------
    # Headline fetching
    # ---------------------------------------------------------------------------

    def _fetch_headlines(self, tickers: list) -> list[str]:
        """
        Fetch recent news headlines for the given tickers via yfinance.

        Returns a deduplicated list of headline strings.
        Returns an empty list on any error.
        """
        headlines = []
        seen: set[str] = set()
        try:
            import yfinance as yf

            for symbol in tickers:
                try:
                    ticker = yf.Ticker(symbol)
                    news_items = ticker.news or []
                    for item in news_items:
                        # yfinance news schema varies by version
                        title = (
                            item.get("title")
                            or (item.get("content") or {}).get("title", "")
                            or ""
                        )
                        title = title.strip()
                        if title and title not in seen:
                            headlines.append(title)
                            seen.add(title)
                except Exception as exc:
                    logger.warning(
                        "LLMPerplexityScorer: news fetch error for %s: %s",
                        symbol,
                        exc,
                    )
        except ImportError:
            logger.warning("LLMPerplexityScorer: yfinance not installed.")
        except Exception as exc:
            logger.warning("LLMPerplexityScorer: headline fetch error: %s", exc)
        return headlines

    # ---------------------------------------------------------------------------
    # Perplexity computation
    # ---------------------------------------------------------------------------

    def _compute_gpt2_perplexity(self, text: str) -> float:
        """
        Compute GPT-2 perplexity for the given text (max _MAX_TOKENS tokens).

        Returns perplexity as a positive float.  Returns 0.0 on failure so
        the caller can detect and fall back to the proxy method.
        """
        try:
            import math
            import torch

            tokenizer = LLMPerplexityScorer._gpt2_tokenizer
            model = LLMPerplexityScorer._gpt2_model

            encodings = tokenizer(
                text,
                return_tensors="pt",
                truncation=True,
                max_length=_MAX_TOKENS,
            )
            input_ids = encodings["input_ids"]

            if input_ids.shape[1] < 2:
                logger.warning(
                    "LLMPerplexityScorer: text too short for GPT-2 scoring."
                )
                return 0.0

            with torch.no_grad():
                outputs = model(input_ids, labels=input_ids)
                loss = outputs.loss  # mean cross-entropy NLL

            perplexity = math.exp(float(loss))
            return perplexity

        except Exception as exc:
            logger.warning(
                "LLMPerplexityScorer: GPT-2 inference error: %s", exc
            )
            return 0.0

    def _compute_proxy_perplexity(self, text: str) -> float:
        """
        Fallback proxy: standard deviation of sentence lengths in characters,
        scaled to a plausible perplexity range.

        Longer and more varied sentences -> higher pseudo-perplexity.
        Range is clamped to [10, 500] to stay within realistic GPT-2 bounds.
        """
        if not text:
            return 50.0  # neutral baseline

        sentences = [s.strip() for s in text.split(".") if s.strip()]
        if len(sentences) < 2:
            return max(10.0, float(len(text.split())))

        lengths = [len(s) for s in sentences]
        mean_len = sum(lengths) / len(lengths)
        variance = sum((x - mean_len) ** 2 for x in lengths) / len(lengths)
        std_len = variance ** 0.5

        # Map: std=0 -> 20, std=100 -> 200
        pseudo_perplexity = 20.0 + std_len * 1.8
        return max(10.0, min(500.0, pseudo_perplexity))

    # ---------------------------------------------------------------------------
    # Rolling history
    # ---------------------------------------------------------------------------

    def _load_history(self) -> list[float]:
        """
        Load the rolling perplexity history from logs/lpas_history.json.

        Returns a list of float values (oldest first).
        Returns an empty list if file is absent or unreadable.
        """
        try:
            if os.path.exists(_HISTORY_FILE):
                with open(_HISTORY_FILE, "r", encoding="utf-8") as fh:
                    data = json.load(fh)
                raw = data.get("perplexity_history", [])
                return [float(x) for x in raw]
        except Exception as exc:
            logger.warning(
                "LLMPerplexityScorer: could not load history file: %s", exc
            )
        return []

    def _save_history(self, history: list[float]) -> None:
        """
        Persist the rolling history (trimmed to _MAX_HISTORY entries) to disk.
        """
        try:
            os.makedirs(os.path.dirname(_HISTORY_FILE), exist_ok=True)
            trimmed = history[-_MAX_HISTORY:]
            with open(_HISTORY_FILE, "w", encoding="utf-8") as fh:
                json.dump(
                    {
                        "perplexity_history": trimmed,
                        "last_updated": datetime.now(timezone.utc).isoformat(),
                    },
                    fh,
                    indent=2,
                )
        except Exception as exc:
            logger.warning(
                "LLMPerplexityScorer: could not save history file: %s", exc
            )

    # ---------------------------------------------------------------------------
    # Public interface
    # ---------------------------------------------------------------------------

    def collect(self, tickers: Optional[list] = None) -> dict:
        """
        Fetch news headlines, compute perplexity, update rolling history,
        and return the LPAS signal dict.

        Parameters
        ----------
        tickers : list of str, optional
            Tickers whose recent news is fetched.
            Defaults to ["SPY", "QQQ", "IWM"].

        Returns
        -------
        dict with keys:
            signal_name   : "lpas_mean"
            value         : float — LPAS z-score
            raw_data      : dict — perplexity, history stats, headline details
            quality_score : 1.0 if real GPT-2, 0.4 if length-variance proxy
            timestamp     : ISO-8601 UTC string
            source        : data source description
        """
        timestamp = datetime.now(timezone.utc).isoformat()

        if tickers is None:
            tickers = _DEFAULT_TICKERS

        # --- Step 1: fetch headlines ---
        headlines = self._fetch_headlines(tickers)
        text_blob = ". ".join(headlines) if headlines else ""

        # --- Step 2: compute perplexity ---
        using_gpt2 = self._load_gpt2()
        current_perplexity: float
        quality_score: float

        if not text_blob:
            logger.warning(
                "LLMPerplexityScorer: no headlines available; using neutral baseline."
            )
            current_perplexity = 50.0
            quality_score = 0.2
        elif using_gpt2:
            gpt2_result = self._compute_gpt2_perplexity(text_blob)
            if gpt2_result > 0.0:
                current_perplexity = gpt2_result
                quality_score = 1.0
            else:
                # GPT-2 inference failed; fall back
                current_perplexity = self._compute_proxy_perplexity(text_blob)
                quality_score = 0.4
        else:
            current_perplexity = self._compute_proxy_perplexity(text_blob)
            quality_score = 0.4

        # --- Step 3: load history and compute rolling stats ---
        history = self._load_history()

        if len(history) >= 2:
            rolling_mean = sum(history) / len(history)
            variance = sum((x - rolling_mean) ** 2 for x in history) / len(history)
            rolling_std = max(variance ** 0.5, 1e-9)
        elif len(history) == 1:
            rolling_mean = history[0]
            rolling_std = max(abs(current_perplexity - rolling_mean), 1.0)
        else:
            # First reading: define LPAS = 0 by convention
            rolling_mean = current_perplexity
            rolling_std = 1.0

        lpas_value = calc_lpas(
            current_perplexity=current_perplexity,
            rolling_mean=rolling_mean,
            rolling_std=rolling_std,
        )

        # --- Step 4: persist updated history ---
        history.append(current_perplexity)
        self._save_history(history)

        raw_data = {
            "headline_count": len(headlines),
            "tickers": tickers,
            "text_blob_chars": len(text_blob),
            "current_perplexity": round(current_perplexity, 4),
            "rolling_mean": round(rolling_mean, 4),
            "rolling_std": round(rolling_std, 4),
            "history_readings": len(history),
            "gpt2_used": using_gpt2 and quality_score == 1.0,
            "sample_headlines": headlines[:5],
        }

        return {
            "signal_name": "lpas_mean",
            "value": float(lpas_value),
            "raw_data": raw_data,
            "quality_score": float(quality_score),
            "timestamp": timestamp,
            "source": _SOURCE,
        }
