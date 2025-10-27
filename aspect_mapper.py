from typing import Dict, List, Tuple, Optional
import json
import numpy as np
from sentence_transformers import SentenceTransformer
from load_ontology import AspectOntology, build_comprehensive_ontology


class AspectMapper:
    """
    Uses cosine similarity in embedding space to:
    - choose the best category for a candidate phrase
    - choose the best sub-aspect within that category

    Workflow:
      mapper.map_phrase("the check took forever")
      -> {
           "category": "service",
           "category_score": 0.82,
           "sub_aspect": "payment / closing experience",
           "sub_aspect_score": 0.79
         }

    You will later attach sentiment to that node.
    """

    def __init__(
        self,
        ontology: AspectOntology,
        model_name: str = "sentence-transformers/all-MiniLM-L6-v2"
    ):
        self.ontology = ontology
        self.model = SentenceTransformer(model_name)

    def _embed(self, phrases: List[str]) -> np.ndarray:
        if not phrases:
            return np.zeros((0, 384))
        return self.model.encode(
            phrases,
            convert_to_numpy=True,
            show_progress_bar=False
        )

    @staticmethod
    def _cosine_sim_matrix(A: np.ndarray, B: np.ndarray) -> np.ndarray:
        if A.size == 0 or B.size == 0:
            return np.zeros((A.shape[0], B.shape[0]))
        A_norm = A / (np.linalg.norm(A, axis=1, keepdims=True) + 1e-12)
        B_norm = B / (np.linalg.norm(B, axis=1, keepdims=True) + 1e-12)
        return A_norm @ B_norm.T

    def _best_match(self, query: str, candidates: List[str]) -> Tuple[Optional[str], float]:
        """
        Returns (best_candidate_string, similarity_score).
        """
        if not candidates:
            return None, -1.0

        q_emb = self._embed([query])          # (1, d)
        c_emb = self._embed(candidates)       # (n, d)
        sims = self._cosine_sim_matrix(q_emb, c_emb)  # (1, n)

        idx = int(np.argmax(sims[0]))
        return candidates[idx], float(sims[0][idx])

    def choose_category(self, phrase: str) -> Tuple[Optional[str], float, Optional[str]]:
        """
        Pick which top-level category the phrase belongs to.

        Returns:
            (best_category, best_score, best_supporting_trigger_phrase)
        """
        best_cat = None
        best_score = -1.0
        best_trigger = None

        for cat in self.ontology.get_all_categories():
            trigger_phrases = self.ontology.get_all_phrases_for_category(cat)
            trigger, score = self._best_match(phrase, trigger_phrases)

            if score > best_score:
                best_score = score
                best_cat = cat
                best_trigger = trigger

        return best_cat, best_score, best_trigger

    def choose_sub_aspect(self, phrase: str, category: str) -> Tuple[Optional[str], float, Optional[str]]:
        """
        Within a known category, choose the best sub-aspect.

        Returns:
            (best_sub_aspect_name, best_score, best_supporting_trigger_phrase)
        """
        best_sub = None
        best_score = -1.0
        best_trigger = None

        for sub in self.ontology.get_all_sub_aspects(category):
            triggers = self.ontology.get_all_phrases_for_sub_aspect(category, sub)
            trigger, score = self._best_match(phrase, triggers)

            if score > best_score:
                best_score = score
                best_sub = sub
                best_trigger = trigger

        return best_sub, best_score, best_trigger

    def map_phrase(
        self,
        phrase: str,
        category_threshold: float = 0.50,
        sub_aspect_threshold: float = 0.50
    ) -> Dict[str, object]:
        """
        High-level convenience method.

        1. pick category
        2. pick sub-aspect in that category
        3. apply thresholds so we can reject very low-confidence matches
        """
        cat, cat_score, cat_trigger = self.choose_category(phrase)

        if (cat is None) or (cat_score < category_threshold):
            return {
                "matched": False,
                "phrase": phrase,
                "reason": "no category above threshold",
                "category": None,
                "category_score": cat_score,
                "category_trigger": cat_trigger,
                "sub_aspect": None,
                "sub_aspect_score": None,
                "sub_aspect_trigger": None
            }

        sub, sub_score, sub_trigger = self.choose_sub_aspect(phrase, cat)

        if (sub is None) or (sub_score < sub_aspect_threshold):
            # We know the category, but sub-aspect is weak
            return {
                "matched": True,
                "phrase": phrase,
                "category": cat,
                "category_score": cat_score,
                "category_trigger": cat_trigger,
                "sub_aspect": None,
                "sub_aspect_score": sub_score,
                "sub_aspect_trigger": sub_trigger,
                "note": "low-confidence sub-aspect match"
            }

        # Confident assignment
        return {
            "matched": True,
            "phrase": phrase,
            "category": cat,
            "category_score": cat_score,
            "category_trigger": cat_trigger,
            "sub_aspect": sub,
            "sub_aspect_score": sub_score,
            "sub_aspect_trigger": sub_trigger
        }


############################################
# Demo
############################################

# if __name__ == "__main__":

#     with open("input_ontology.json", "r") as f:
#         ONTLOLOGY = json.load(f)

#     ontology = build_comprehensive_ontology(ONTLOLOGY)

#     # Create mapper
#     mapper = AspectMapper(ontology)

#     # Example phrases from rule-based extraction
#     test_phrases = [
#         "the check took forever",
#         "bathroom was disgusting",
#         "the steak was cold",
#         "music was too loud, had to shout",
#         "portion size was tiny for the price",
#         "our server was super rude",
#         "great value for money",
#         "the patio view was gorgeous",
#         "felt way too expensive for what we got",
#     ]

#     for p in test_phrases:
#         result = mapper.map_phrase(p)
#         print("\nPhrase:", p)
#         for k, v in result.items():
#             print(f"  {k}: {v}")
