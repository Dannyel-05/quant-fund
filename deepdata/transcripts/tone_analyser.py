"""
tone_analyser.py — Sophisticated NLP analysis of earnings call transcripts.
"""

import logging
import re
from pathlib import Path

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Word lists
# ---------------------------------------------------------------------------

CERTAINTY_WORDS = [
    "will", "definitely", "certain", "confident", "clear", "absolutely",
    "committed", "expect", "plan", "target", "goal",
]
HEDGE_WORDS = [
    "may", "might", "could", "possibly", "potentially", "subject to",
    "depending on", "if conditions", "approximately", "around", "roughly",
]
FORWARD_WORDS = [
    "will", "going to", "expect", "anticipate", "next quarter", "ahead",
    "future", "plan", "outlook", "guidance",
]
BACKWARD_WORDS = [
    "was", "were", "had", "achieved", "delivered", "completed", "last quarter",
]

POSITIVE_WORDS = [
    "growth", "increase", "strong", "record", "exceeded", "beat", "outperform",
    "momentum", "accelerating", "positive", "improve", "higher", "gain",
    "opportunity", "robust", "excellent", "solid", "outstanding",
]
NEGATIVE_WORDS = [
    "decline", "decrease", "weak", "miss", "underperform", "loss", "challenge",
    "headwind", "difficult", "lower", "below", "disappoint", "concern",
    "pressure", "risk", "uncertainty", "slower",
]

PUSHBACK_PHRASES = [
    r"just to be clear",
    r"can you clarify",
    r"what i.?m asking is",
    r"maybe i.?ll rephrase",
    r"sorry,?\s+just to follow up",
    r"i.?m not sure you answered",
    r"to be more specific",
    r"let me ask again",
]

# ---------------------------------------------------------------------------
# Optional heavy deps
# ---------------------------------------------------------------------------

try:
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
    _vader = SentimentIntensityAnalyzer()
    HAS_VADER = True
except ImportError:
    HAS_VADER = False

try:
    import spacy
    HAS_SPACY = True
except ImportError:
    HAS_SPACY = False

# ---------------------------------------------------------------------------
# ToneAnalyser
# ---------------------------------------------------------------------------


class ToneAnalyser:
    def __init__(self, config: dict):
        self.config = config

    # ------------------------------------------------------------------
    # Main entry
    # ------------------------------------------------------------------

    def analyse(self, transcript: dict) -> dict:
        """
        Full linguistic analysis. Returns a dict with all tone metrics.
        """
        sections = transcript.get("sections", {})
        prepared = sections.get("prepared_remarks", "")
        qa = sections.get("qa_section", "")
        full_text = prepared + "\n" + qa

        try:
            hedge_ratio = self.calc_hedge_ratio(full_text)
        except Exception as exc:
            logger.warning("hedge_ratio error: %s", exc)
            hedge_ratio = 0.0

        try:
            certainty_ratio = self._calc_certainty_ratio(full_text)
        except Exception as exc:
            logger.warning("certainty_ratio error: %s", exc)
            certainty_ratio = 0.0

        try:
            forward_ratio = self.calc_forward_ratio(full_text)
        except Exception as exc:
            logger.warning("forward_ratio error: %s", exc)
            forward_ratio = 0.0

        try:
            backward_ratio = self._calc_backward_ratio(full_text)
        except Exception as exc:
            logger.warning("backward_ratio error: %s", exc)
            backward_ratio = 0.0

        try:
            we_ratio = self.calc_we_ratio(full_text)
        except Exception as exc:
            logger.warning("we_ratio error: %s", exc)
            we_ratio = 0.0

        try:
            passive_ratio = self.calc_passive_ratio(full_text)
        except Exception as exc:
            logger.warning("passive_ratio error: %s", exc)
            passive_ratio = 0.0

        try:
            tone_shift = self.calc_tone_shift(prepared, qa)
        except Exception as exc:
            logger.warning("tone_shift error: %s", exc)
            tone_shift = 0.0

        try:
            prepared_sentiment = self.calc_sentiment(prepared)
        except Exception as exc:
            logger.warning("prepared_sentiment error: %s", exc)
            prepared_sentiment = 0.0

        try:
            qa_sentiment = self.calc_sentiment(qa)
        except Exception as exc:
            logger.warning("qa_sentiment error: %s", exc)
            qa_sentiment = 0.0

        try:
            analyst_pushback_score = self.calc_analyst_pushback(qa)
        except Exception as exc:
            logger.warning("analyst_pushback_score error: %s", exc)
            analyst_pushback_score = 0.0

        try:
            management_interruption_count = self.count_interruptions(transcript)
        except Exception as exc:
            logger.warning("interruption count error: %s", exc)
            management_interruption_count = 0

        # Deflection score (simple — DeflectionDetector handles full version)
        deflection_score = min(analyst_pushback_score, 1.0)

        # Overall tone: weighted aggregate
        overall_tone = (
            prepared_sentiment * 0.35
            + qa_sentiment * 0.20
            + (1.0 - hedge_ratio) * 0.20
            + forward_ratio * 0.15
            + (certainty_ratio - hedge_ratio) * 0.10
        )
        overall_tone = max(-1.0, min(1.0, overall_tone))

        return {
            "hedge_ratio": hedge_ratio,
            "certainty_ratio": certainty_ratio,
            "forward_ratio": forward_ratio,
            "backward_ratio": backward_ratio,
            "we_ratio": we_ratio,
            "passive_ratio": passive_ratio,
            "tone_shift": tone_shift,
            "deflection_score": deflection_score,
            "prepared_sentiment": prepared_sentiment,
            "qa_sentiment": qa_sentiment,
            "management_interruption_count": management_interruption_count,
            "analyst_pushback_score": analyst_pushback_score,
            "overall_tone": overall_tone,
        }

    # ------------------------------------------------------------------
    # Ratio calculations
    # ------------------------------------------------------------------

    def calc_hedge_ratio(self, text: str) -> float:
        """hedge_count / (hedge_count + certainty_count + 0.001)"""
        text_lower = text.lower()
        hedge_count = sum(text_lower.count(w) for w in HEDGE_WORDS)
        certainty_count = sum(text_lower.count(w) for w in CERTAINTY_WORDS)
        return hedge_count / (hedge_count + certainty_count + 0.001)

    def _calc_certainty_ratio(self, text: str) -> float:
        text_lower = text.lower()
        certainty_count = sum(text_lower.count(w) for w in CERTAINTY_WORDS)
        hedge_count = sum(text_lower.count(w) for w in HEDGE_WORDS)
        return certainty_count / (hedge_count + certainty_count + 0.001)

    def calc_forward_ratio(self, text: str) -> float:
        """forward_word_count / max(total_words * 0.1, 1)"""
        words = text.lower().split()
        total = len(words)
        forward_count = sum(
            1 for w in FORWARD_WORDS
            if re.search(r"\b" + re.escape(w) + r"\b", text.lower())
        )
        return forward_count / max(total * 0.1, 1)

    def _calc_backward_ratio(self, text: str) -> float:
        words = text.lower().split()
        total = len(words)
        backward_count = sum(
            text.lower().count(w) for w in BACKWARD_WORDS
        )
        return backward_count / max(total * 0.1, 1)

    def calc_we_ratio(self, text: str) -> float:
        """we_count / (we_count + i_count + 0.001)"""
        text_lower = text.lower()
        we_count = len(re.findall(r"\bwe\b", text_lower))
        i_count = len(re.findall(r"\bi\b", text_lower))
        return we_count / (we_count + i_count + 0.001)

    def calc_passive_ratio(self, text: str) -> float:
        """
        Detect passive voice patterns.
        Uses spaCy if available, else regex fallback.
        passive_count / sentence_count
        """
        sentences = re.split(r"[.!?]+", text)
        sentence_count = max(len([s for s in sentences if s.strip()]), 1)

        if HAS_SPACY:
            try:
                import spacy
                nlp = spacy.load("en_core_web_sm")
                passive_count = 0
                for sent in sentences[:200]:  # cap for performance
                    doc = nlp(sent)
                    for token in doc:
                        if token.dep_ == "nsubjpass":
                            passive_count += 1
                            break
                return passive_count / sentence_count
            except Exception as exc:
                logger.warning("spaCy passive detection failed, using regex: %s", exc)

        # Regex fallback
        passive_patterns = [
            re.compile(r"\bwas\s+\w+ed\b", re.I),
            re.compile(r"\bwere\s+\w+ed\b", re.I),
            re.compile(r"\bhas\s+been\s+\w+ed\b", re.I),
            re.compile(r"\bhave\s+been\s+\w+ed\b", re.I),
            re.compile(r"\bis\s+being\s+\w+ed\b", re.I),
            re.compile(r"\bare\s+being\s+\w+ed\b", re.I),
            re.compile(r"\bwill\s+be\s+\w+ed\b", re.I),
        ]
        passive_count = 0
        for sent in sentences:
            for pat in passive_patterns:
                if pat.search(sent):
                    passive_count += 1
                    break
        return passive_count / sentence_count

    def calc_tone_shift(self, prepared: str, qa: str) -> float:
        """
        Sentiment(prepared) - Sentiment(qa).
        Positive value = management more positive in scripted remarks.
        """
        prepared_sent = self.calc_sentiment(prepared)
        qa_sent = self.calc_sentiment(qa)
        return prepared_sent - qa_sent

    def calc_analyst_pushback(self, qa_text: str) -> float:
        """
        Count pushback phrases. Return score = pushback_count / total_analyst_questions.
        """
        if not qa_text.strip():
            return 0.0

        pushback_count = 0
        qa_lower = qa_text.lower()
        for phrase_pattern in PUSHBACK_PHRASES:
            pushback_count += len(re.findall(phrase_pattern, qa_lower))

        # Approximate analyst question count: lines ending with "?"
        question_count = max(len(re.findall(r"\?", qa_text)), 1)
        return min(pushback_count / question_count, 1.0)

    def count_interruptions(self, transcript: dict) -> int:
        """
        Count speaker transitions mid-sentence (speaker change before period/question mark).
        Approximated with regex on speaker-tagged text.
        """
        sections = transcript.get("sections", {})
        full_text = sections.get("prepared_remarks", "") + "\n" + sections.get("qa_section", "")

        # Speaker label pattern: lines like "Name:" or "NAME:"
        speaker_line = re.compile(r"^[A-Z][A-Za-z\s\-']{1,30}:", re.MULTILINE)
        positions = [m.start() for m in speaker_line.finditer(full_text)]

        interruption_count = 0
        for i in range(1, len(positions)):
            # Check text between previous speaker end and current speaker start
            prev_end = positions[i - 1]
            curr_start = positions[i]
            segment = full_text[prev_end:curr_start].strip()

            # If segment does not end with sentence-ending punctuation = interruption
            if segment and not re.search(r"[.!?]\s*$", segment):
                interruption_count += 1

        return interruption_count

    def calc_sentiment(self, text: str) -> float:
        """Use VADER compound if available, else weighted pos/neg word count. Return -1 to 1."""
        if not text.strip():
            return 0.0

        if HAS_VADER:
            try:
                scores = _vader.polarity_scores(text)
                return float(scores["compound"])
            except Exception as exc:
                logger.warning("VADER sentiment failed, using fallback: %s", exc)

        # Simple word count fallback
        text_lower = text.lower()
        words = text_lower.split()
        total = max(len(words), 1)

        pos_count = sum(1 for w in words if any(pw in w for pw in POSITIVE_WORDS))
        neg_count = sum(1 for w in words if any(nw in w for nw in NEGATIVE_WORDS))

        raw_score = (pos_count - neg_count) / total
        # Normalise to roughly -1 to 1 (empirically scale by 10)
        return max(-1.0, min(1.0, raw_score * 10))
