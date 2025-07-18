from typing import Any

from metabolights_utils.models.isa.investigation_file import OntologyAnnotation


class OntologyItem(OntologyAnnotation):
    def __str__(self) -> str:
        return f"{self.term_source_ref or ''}:{self.term or ''}:{self.term_accession_number or ''}"

    def __hash__(self) -> int:
        return hash(self.__str__())


class BaseModifier:
    def get_list_string(self, list_item: list[Any], limit: int = 10):
        if not list_item or limit < 1:
            return ""
        list_item.sort()
        items = [str(x) for x in list_item if x]
        if len(items) <= limit:
            str_value = ", ".join([str(x) for x in items])
        else:
            str_value = (
                ", ".join([str(x) for x in items[:limit]])
                + " ... "
                + f"(total: {len(items)})"
            )
        return str_value

    @staticmethod
    def first_character_uppercase(factor: str):
        if not factor or not factor.strip():
            return ""
        terms = " ".join(factor.split("_"))
        terms = [x.strip() for x in terms.split() if x and x.strip()]
        new_terms = []
        for idx, term in enumerate(terms):
            sub_terms = [x.strip() for x in term.split("-") if x and x and x.strip()]
            new_sub_terms = []
            for idx_2, sub_term in enumerate(sub_terms):
                if len(sub_term) > 1 and sub_term.isupper():
                    new_sub_terms.append(sub_term)
                elif idx == 0 and idx_2 == 0:
                    if len(sub_term) > 1:
                        new_sub_terms.append(sub_term[0].upper() + sub_term[1:])
                    else:
                        new_sub_terms.append(sub_term.upper())
                else:
                    new_sub_terms.append(sub_term.lower())
            new_terms.append("-".join(new_sub_terms))
        return " ".join(new_terms)
