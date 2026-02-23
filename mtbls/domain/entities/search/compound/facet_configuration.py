from typing import Any, Dict

COMPOUND_FACET_CONFIG: Dict[str, Dict[str, Any]] = {
    #
    # ---- FLAG FACETS (booleans) ----
    # Each one will become a facet; with `only_true`
    # you can hide the `false` bucket in the UI mapping.
    #
    "hasLiterature": {
        "type": "value",
        "field": "flags.hasLiterature",
        "only_true": True,
    },
    "hasReactions": {
        "type": "value",
        "field": "flags.hasReactions",
        "only_true": True,
    },
    "hasSpecies": {
        "type": "value",
        "field": "flags.hasSpecies",
        "only_true": True,
    },
    "hasPathways": {
        "type": "value",
        "field": "flags.hasPathways",
        "only_true": True,
    },
    "hasNMR": {
        "type": "value",
        "field": "flags.hasNMR",
        "only_true": True,
    },
    "hasMS": {
        "type": "value",
        "field": "flags.hasMS",
        "only_true": True,
    },
    #
    # ---- OTHER VALUE FACETS ----
    #
    "charge": {
        "type": "value",
        "field": "charge",
    },
    "organisms": {
        "type": "value",
        "field": "organisms",
    },
    # If you don’t want to deal with nested aggs yet, you can still
    # facet on species like this.
    # (It will count documents, not individual species_hits rows,
    # but it’s usually fine.)
    "species": {
        "type": "value",
        "field": "species_hits.species",
    },
    "organismParts": {
        "type": "value",
        "field": "organismParts",
    },
    #
    # ---- RANGE FACETS ----
    #
    "exactmass": {
        "type": "range",
        "field": "exactmass",
        "ranges": [
            {"from": 0, "to": 200, "name": "0–200 Da"},
            {"from": 200, "to": 500, "name": "200–500 Da"},
            {"from": 500, "to": 1000, "name": "500–1000 Da"},
            {"from": 1000, "to": 2000, "name": "1000–2000 Da"},
            {"from": 2000, "name": "2000+ Da"},
        ],
    },
    "studyCount": {
        "type": "range",
        "field": "studyCount",
        "ranges": [
            {"from": 1, "to": 1, "name": "1"},
            {"from": 2, "to": 5, "name": "2–5"},
            {"from": 6, "to": 9, "name": "6–9"},
            {"from": 10, "name": "10+"},
        ],
    },
    "averagemass": {
        "type": "range",
        "field": "averagemass",
        "ranges": [
            {"from": 0, "to": 200, "name": "0–200 Da"},
            {"from": 200, "to": 500, "name": "200–500 Da"},
            {"from": 500, "to": 1000, "name": "500–1000 Da"},
            {"from": 1000, "to": 2000, "name": "1000–2000 Da"},
            {"from": 2000, "name": "2000+ Da"},
        ],
    },
    # "spectra": {
    #     "type": "range",
    #     "field": "counts.spectra",
    #     "ranges": [
    #         {"from": 0, "to": 1, "name": "0"},
    #         {"from": 1, "to": 5, "name": "1–4"},
    #         {"from": 5, "to": 20, "name": "5–19"},
    #         {"from": 20, "name": "20+"},
    #     ],
    # },
    # "citations": {
    #     "type": "range",
    #     "field": "counts.citations",
    #     "ranges": [
    #         {"from": 0, "to": 1, "name": "0"},
    #         {"from": 1, "to": 5, "name": "1–4"},
    #         {"from": 5, "to": 20, "name": "5–19"},
    #         {"from": 20, "name": "20+"},
    #     ],
    # },
    # "synonyms_count": {
    #     "type": "range",
    #     "field": "counts.synonyms",
    #     "ranges": [
    #         {"from": 0, "to": 1, "name": "0"},
    #         {"from": 1, "to": 5, "name": "1–4"},
    #         {"from": 5, "to": 20, "name": "5–19"},
    #         {"from": 20, "name": "20+"},
    #     ],
    # },
}
