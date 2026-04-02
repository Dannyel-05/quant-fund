"""
deflection_detector.py — Detects when management avoids answering questions.
"""

import logging
import re
from pathlib import Path

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Optional heavy deps
# ---------------------------------------------------------------------------

try:
    from sentence_transformers import SentenceTransformer
    _st_model = SentenceTransformer("all-MiniLM-L6-v2")
    HAS_SENTENCE_TRANSFORMERS = True
except ImportError:
    HAS_SENTENCE_TRANSFORMERS = False

try:
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.metrics.pairwise import cosine_similarity as sk_cosine
    HAS_SKLEARN = True
except ImportError:
    HAS_SKLEARN = False

try:
    import numpy as np
    HAS_NUMPY = True
except ImportError:
    HAS_NUMPY = False


# ---------------------------------------------------------------------------
# DeflectionDetector
# ---------------------------------------------------------------------------


class DeflectionDetector:
    def __init__(self, config: dict):
        self.config = config

    # ------------------------------------------------------------------
    # Main entry
    # ------------------------------------------------------------------

    def detect(self, transcript: dict) -> dict:
        """
        Returns deflection analysis dict.
        """
        sections = transcript.get("sections", {})
        qa_text = sections.get("qa_section", "")

        if not qa_text.strip():
            return {
                "deflection_score": 0.0,
                "deflected_topics": [],
                "high_deflection_qa": [],
                "pushback_questions": [],
            }

        try:
            qa_pairs = self.extract_qa_pairs(qa_text)
        except Exception as exc:
            logger.warning("extract_qa_pairs error: %s", exc)
            qa_pairs = []

        if not qa_pairs:
            return {
                "deflection_score": 0.0,
                "deflected_topics": [],
                "high_deflection_qa": [],
                "pushback_questions": [],
            }

        # Compute relevance per pair
        relevances = []
        for pair in qa_pairs:
            try:
                rel = self.calc_response_relevance(pair.get("question", ""), pair.get("answer", ""))
                pair["relevance"] = rel
                relevances.append(rel)
            except Exception as exc:
                logger.warning("relevance calc error: %s", exc)
                pair["relevance"] = 0.5
                relevances.append(0.5)

        # Mean deflection = 1 - mean_relevance
        mean_relevance = sum(relevances) / len(relevances) if relevances else 0.5
        deflection_score = 1.0 - mean_relevance

        # High-deflection Q&A pairs
        high_deflection_qa = [
            p for p in qa_pairs if p.get("relevance", 1.0) < 0.5
        ]

        # Deflected topics
        try:
            deflected_topics = self.identify_deflected_topics(
                [p for p in qa_pairs if p.get("relevance", 1.0) < 0.5]
            )
        except Exception as exc:
            logger.warning("identify_deflected_topics error: %s", exc)
            deflected_topics = []

        # Repeated questions
        try:
            pushback_questions = self.detect_repeated_questions(qa_pairs)
        except Exception as exc:
            logger.warning("detect_repeated_questions error: %s", exc)
            pushback_questions = []

        return {
            "deflection_score": round(deflection_score, 4),
            "deflected_topics": deflected_topics,
            "high_deflection_qa": high_deflection_qa,
            "pushback_questions": pushback_questions,
        }

    # ------------------------------------------------------------------
    # Q&A parsing
    # ------------------------------------------------------------------

    def extract_qa_pairs(self, qa_text: str) -> list:
        """Parse Q&A section into list of {question, answer, analyst, exec} dicts."""
        pairs = []
        lines = qa_text.splitlines()

        # Speaker label pattern: "Name (Role):" or "Name:"
        speaker_re = re.compile(
            r"^([A-Z][a-zA-Z\-']+(?:\s[A-Z][a-zA-Z\-']+){0,3})\s*(?:\([^)]*\))?\s*:"
        )

        # Collect speaker turns
        turns: list = []  # list of (speaker, text)
        current_speaker = None
        current_lines: list = []

        for line in lines:
            stripped = line.strip()
            if not stripped:
                continue
            m = speaker_re.match(stripped)
            if m:
                if current_speaker:
                    turns.append((current_speaker, " ".join(current_lines).strip()))
                current_speaker = m.group(1).strip()
                remainder = stripped[m.end():].strip()
                current_lines = [remainder] if remainder else []
            else:
                if current_speaker:
                    current_lines.append(stripped)

        if current_speaker:
            turns.append((current_speaker, " ".join(current_lines).strip()))

        # Pair up analyst questions with exec answers
        i = 0
        while i < len(turns) - 1:
            speaker, text = turns[i]
            role = _infer_role(speaker)
            if role == "Analyst" or _is_question(text):
                # Find next management response
                j = i + 1
                while j < len(turns):
                    resp_speaker, resp_text = turns[j]
                    if _infer_role(resp_speaker) != "Analyst":
                        pairs.append({
                            "question": text,
                            "answer": resp_text,
                            "analyst": speaker,
                            "exec": resp_speaker,
                        })
                        i = j + 1
                        break
                    j += 1
                else:
                    i += 1
            else:
                i += 1

        return pairs

    # ------------------------------------------------------------------
    # Relevance
    # ------------------------------------------------------------------

    def calc_response_relevance(self, question: str, answer: str) -> float:
        """
        Semantic similarity between question and answer.
        Primary: sentence-transformers. Fallback: TF-IDF cosine. Fallback 2: keyword overlap.
        Returns 0-1 where 1 = perfectly on-topic.
        """
        if not question.strip() or not answer.strip():
            return 0.5

        # Method 1: sentence-transformers
        if HAS_SENTENCE_TRANSFORMERS:
            try:
                embeddings = _st_model.encode([question, answer])
                if HAS_NUMPY:
                    import numpy as np
                    q_vec = embeddings[0]
                    a_vec = embeddings[1]
                    sim = float(
                        np.dot(q_vec, a_vec) / (np.linalg.norm(q_vec) * np.linalg.norm(a_vec) + 1e-9)
                    )
                    return max(0.0, min(1.0, sim))
            except Exception as exc:
                logger.warning("sentence-transformer similarity failed: %s", exc)

        # Method 2: TF-IDF cosine
        if HAS_SKLEARN:
            try:
                vec = TfidfVectorizer(stop_words="english", min_df=1)
                matrix = vec.fit_transform([question, answer])
                sim = float(sk_cosine(matrix[0], matrix[1])[0][0])
                return max(0.0, min(1.0, sim))
            except Exception as exc:
                logger.warning("TF-IDF similarity failed: %s", exc)

        # Method 3: keyword overlap ratio
        return _keyword_overlap(question, answer)

    # ------------------------------------------------------------------
    # Topic identification
    # ------------------------------------------------------------------

    def identify_deflected_topics(self, low_relevance_pairs: list) -> list:
        """Extract noun/entity topics from questions with low relevance scores."""
        topics = []
        stop_words = {
            "the", "a", "an", "is", "are", "was", "were", "be", "been",
            "have", "has", "had", "do", "does", "did", "will", "would",
            "can", "could", "should", "may", "might", "shall", "to",
            "of", "in", "on", "at", "by", "for", "with", "about",
            "from", "or", "and", "but", "if", "as", "that", "this",
            "what", "how", "when", "where", "why", "which", "who",
            "your", "our", "you", "we", "i", "it", "they",
        }

        for pair in low_relevance_pairs:
            question = pair.get("question", "")
            words = re.findall(r"\b[a-zA-Z]{3,}\b", question.lower())
            for word in words:
                if word not in stop_words and word not in topics:
                    topics.append(word)

        # Return top unique topics by frequency across deflected questions
        from collections import Counter
        all_words = []
        for pair in low_relevance_pairs:
            question = pair.get("question", "")
            words = re.findall(r"\b[a-zA-Z]{3,}\b", question.lower())
            all_words.extend([w for w in words if w not in stop_words])

        top = [w for w, _ in Counter(all_words).most_common(10)]
        return top

    # ------------------------------------------------------------------
    # Repeated questions
    # ------------------------------------------------------------------

    def detect_repeated_questions(self, qa_pairs: list) -> list:
        """Detect when analyst asks essentially the same question twice."""
        repeated = []
        questions = [p.get("question", "") for p in qa_pairs]

        for i in range(len(questions)):
            for j in range(i + 1, len(questions)):
                sim = _keyword_overlap(questions[i], questions[j])
                if sim > 0.5:
                    repeated.append({
                        "first_question": questions[i],
                        "repeated_question": questions[j],
                        "similarity": round(sim, 3),
                        "analyst": qa_pairs[i].get("analyst", ""),
                    })

        return repeated


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _keyword_overlap(text_a: str, text_b: str) -> float:
    """Fraction of unique words in text_a that appear in text_b."""
    stop = {"the", "a", "an", "is", "are", "was", "to", "of", "in", "and", "or"}
    words_a = set(re.findall(r"\b[a-zA-Z]{3,}\b", text_a.lower())) - stop
    words_b = set(re.findall(r"\b[a-zA-Z]{3,}\b", text_b.lower())) - stop
    if not words_a:
        return 0.0
    return len(words_a & words_b) / len(words_a)


def _is_question(text: str) -> bool:
    return "?" in text or any(
        text.lower().startswith(kw)
        for kw in ["can you", "could you", "what is", "how do", "why did",
                   "when will", "what are", "is there", "do you"]
    )


def _infer_role(name: str) -> str:
    name_lower = name.lower()
    analyst_keywords = ["analyst", "research", "capital", "securities", "partners",
                        "management", "asset", "fund", "investment"]
    mgmt_keywords = ["ceo", "cfo", "coo", "president", "officer", "director", "head of"]
    if any(k in name_lower for k in analyst_keywords):
        return "Analyst"
    if any(k in name_lower for k in mgmt_keywords):
        return "Management"
    # Heuristic: management names tend to be just first+last, analysts often have company
    return "Unknown"
