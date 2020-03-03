from __future__ import annotations

from collections import ChainMap
from typing import Dict, Iterable, List


class EntityLineage(list):
    """ Facility for managing cascading configurations for tiered object configuration.
        If initialized with an iterable, the iterable should be sorted from most important
        to least important. The lineage is always searched starting at index 0 of the underlying
        list and searching in an ascending fashion until the list is exhausted. When searching
        the lineage for an attribute, the first occurance of an attribute will returned and
        subsequent items will not be searched.
        """

    def __init__(self, iterable: Iterable[EntityBase] = None):
        iterable = iterable or []
        super().__init__(iterable)

    def add(self, value: EntityBase):
        """ Insert a new entity to the head of the lineage """
        if isinstance(value, EntityBase):
            self.insert(0, value)
        else:
            raise TypeError("value does not extend EntityBase")

    @property
    def options(self) -> ChainMap:
        """ Get an ordinal chained mapping of option sets for each entity in the lineage.
            The returned ChainMap is comprised of a list of mappings. These mappins are
            referential to the option set of each entity, not copies, which means the
            returned ChainMap will reflect changes in the underlying entities without
            needing to recreate the ChainMap."""
        return ChainMap(*[x._options for x in self])


class EntityBase:
    _options: Dict  # type: ignore
    inherit_options: bool = False

    def __init__(
        self, name: str, options: Dict = None, lineage: List[EntityBase] = None,
    ):
        self.name: str = name
        self._options: Dict = options or {}  # type: ignore
        self.lineage: EntityLineage = EntityLineage(lineage)
        self.lineage.add(self)

    @property
    def options(self) -> ChainMap:
        """ Get an ordinal chained mapping of cascading option sets comprised of
            the options of each entity in this entity's lineage, including those
            of this entity. """
        return self.lineage.options

    @options.setter
    def options(self, values: Dict):
        """ Set the entity's options and set the `inherit_options` flag on the
            entity itself """
        self.inherit_options = values.pop("inherit", False)
        self._options = values

    def dict(self):
        return {"name": self.name, "options": self.options}


if __name__ == "__main__":
    l1 = EntityLineage(
        [
            EntityBase(name="l3", options={"l3": "l3", "shared": "l3"}),
            EntityBase(name="l2", options={"l2": "l2", "shared": "l2"}),
            EntityBase(name="l1", options={"l1": "l1", "shared": "l1"}),
        ]
    )

    l1.add(EntityBase(name="l4", options={"l4": "l4", "shared": "l4"}))

    assert l1.options["l1"] == "l1"
    assert l1.options["l3"] == "l3"
    assert l1.options["shared"] == "l4"

    dict(l1.options)
