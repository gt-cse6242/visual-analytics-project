from typing import Dict, Set, List, Tuple
import json

class AspectOntology:
    """
    Internal flattened representation that keeps:
    - category level
    - sub-aspect level
    - all trigger phrases per sub-aspect

    We'll store:
    self.ontology = {
        "service": {
            "category_synonyms": {...},
            "sub_aspects": {
                "speed / timeliness": {"fast service","slow service","took forever",...},
                "attentiveness": {...},
                ...
            }
        },
        ...
    }
    """

    def __init__(self):
        self.ontology: Dict[str, Dict[str, object]] = {}

    def add_category(self, category: str):
        cat = category.lower().strip()
        if cat not in self.ontology:
            self.ontology[cat] = {
                "category_synonyms": set(),
                "sub_aspects": {}  # sub_aspect_name -> set(phrases)
            }

    def add_category_synonym(self, category: str, phrase: str):
        '''add categories if not exists'''
        cat = category.lower().strip()
        self.add_category(cat)
        self.ontology[cat]["category_synonyms"].add(phrase.lower().strip())

    def add_sub_aspect(self, category: str, sub_aspect: str):
        '''add sub level aspect if not exists'''
        cat = category.lower().strip()
        sub = sub_aspect.lower().strip()
        self.add_category(cat)
        if sub not in self.ontology[cat]["sub_aspects"]:
            self.ontology[cat]["sub_aspects"][sub] = set()

    def add_phrase(self, category: str, sub_aspect: str, phrase: str):
        ''''''
        cat = category.lower().strip()
        sub = sub_aspect.lower().strip()
        self.add_sub_aspect(cat, sub)
        self.ontology[cat]["sub_aspects"][sub].add(phrase.lower().strip())

    def get_all_phrases_for_category(self, category: str) -> List[str]:
        """
        Everything humans might say that implies this category,
        including category synonyms + all sub-aspect phrases.
        Used for cosine similarity to choose the best category.
        """
        cat = category.lower().strip()
        if cat not in self.ontology:
            return []
        phrases = set(self.ontology[cat]["category_synonyms"])
        for sub, trigger_set in self.ontology[cat]["sub_aspects"].items():
            phrases.update(trigger_set)
        return list(phrases)

    def get_all_phrases_for_sub_aspect(self, category: str, sub_aspect: str) -> List[str]:
        cat = category.lower().strip()
        sub = sub_aspect.lower().strip()
        if cat not in self.ontology:
            return []
        return list(self.ontology[cat]["sub_aspects"].get(sub, set()))
    
    def get_all_categories(self) -> List[str]:
        return list(self.ontology.keys())

    def get_all_sub_aspects(self, category: str) -> List[str]:
        cat = category.lower().strip()
        if cat not in self.ontology:
            return []
        return list(self.ontology[cat]["sub_aspects"].keys())

    def get_all_phrases_for_sub_aspect(self, category: str, sub_aspect: str) -> List[str]:
        cat = category.lower().strip()
        sub = sub_aspect.lower().strip()
        if cat not in self.ontology:
            return []
        return list(self.ontology[cat]["sub_aspects"].get(sub, set()))

    def export_json(self, path: str = None) -> str:
        """
        Export a clean JSON view of the ontology.
        """
        clean = {}
        for cat, cat_info in self.ontology.items():
            clean[cat] = {
                "category_synonyms": sorted(list(cat_info["category_synonyms"])),
                "sub_aspects": {
                    sub: sorted(list(triggers))
                    for sub, triggers in cat_info["sub_aspects"].items()
                }
            }
        text = json.dumps(clean, indent=2)
        if path:
            with open(path, "w") as f:
                f.write(text)
        return text


def build_comprehensive_ontology(raw_spec: Dict[str, Dict[str, object]]) -> AspectOntology:
    """
    Turn ontology json into an AspectOntology instance.
    """
    ont = AspectOntology()

    for category, cat_obj in raw_spec.items():
        # category synonyms
        for cat_syn in cat_obj.get("category_synonyms", []):
            ont.add_category_synonym(category, cat_syn)

        # sub-aspects
        sub_aspects = cat_obj.get("sub_aspects", {})
        for sub_aspect_name, trigger_list in sub_aspects.items():
            for phrase in trigger_list:
                ont.add_phrase(category, sub_aspect_name, phrase)

    return ont


############################################
# Demo
############################################
# if __name__ == "__main__":
#     # Load the comprehensive ontology spec from JSON file
#     with open("input_ontology.json", "r") as f:
#         ONTLOLOGY = json.load(f)

#     ontology = build_comprehensive_ontology(ONTLOLOGY)

#     print("=== Ontology snapshot ===")
#     print(ontology.export_json(path="output_ontology.json"))
