from typing import Any, Dict, List, Union

import util
from parsing.criteria import Criterion


class ParserRule:
    """ Validation rule used by a Parser to determine if/how to parse a value """

    def __init__(
        self,
        criteria: List[Criterion],
        name: str = None,
        allow_partial: bool = True,
        **kwargs,
    ):
        """
        Arguments:
            criteria {List[Criterion]} -- Criteria list

        Keyword Arguments:
            name {str} -- Rule name
            partial {bool} -- If True, the rule will pass if any criteria are satisfied.
            If False, the rule will pass only if all criteria are satisfied.

        """
        self.name = name or ""
        self.criteria = criteria
        self.allow_partial = allow_partial

    def __repr__(self):
        size = len(self.criteria)
        return f"ParserRule:{self.name} ({self.match_mode}) -  {size} criteria"

    def __call__(
        self, value: Any, return_partials: bool = False
    ) -> Union[bool, List[bool]]:
        """ Enables the ParserRule to be callable, such that invoking the rule with
            a passed value will return the evaluation result of the called rule.

            Example: MyIntegerParserRule("13") -> True

            return_partials: set to True to return a list of the result of each
                             criteria. If set to False (default), return the result of
                             the appropriate boolean operation:

                            if self.allow_partials = True: ->
                                return any([partial1, partial2, ...]
                            else:
                                return all([partial1, partial2, ...]
        """
        partials = [c(value) for c in self.criteria]
        if return_partials:
            return partials
        if self.allow_partial:
            return any(partials)
        else:
            return all(partials)

    @property
    def match_mode(self):
        """ Indicates if all criteria must be met to consider a parse successful """
        return "PARTIAL" if self.allow_partial else "FULL"

    @classmethod
    def from_list(cls, criteria: List[Dict], **kwargs) -> "ParserRule":
        """ Initialize a rule from a list of criteria specifications.
                Example criteria spec:
                    criteria = \
                        [
                            {
                                "name": "parse_integers",
                                "type": "RegexCriterion",
                                "value": r"^[-+]?[0-9]+$",
                            },
                        ],
         """
        criteriaObjs: List[Criterion] = []
        for c in criteria:
            CriteriaType = util.locate(c["type"], "parsing.criteria")
            criteriaObjs.append(CriteriaType(c["value"], c["name"]))
        return cls(criteriaObjs, **kwargs)
