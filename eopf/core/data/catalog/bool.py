from abc import ABC

from eopf.core.data.catalog import FeaturePropertyPredicate, Predicate


class BooleanPredicate(Predicate):
    """BooleanPredicate is an abstract class from which all concrete BooleanPredicate must inherit
    Boolean predicates can be used to combine FeaturePropertyPredicate or GeometricPredicate
    """

class OrPredicate(BooleanPredicate):
    """OrPredicate combines two predicates using 'OR' boolean operator
    OrPredicate is functionnaly equivalent to (left_predicate(left_property.value) OR right_predicate(right_property.value))
    """
    left_predicate: FeaturePropertyPredicate
    right_predicate: FeaturePropertyPredicate

class AndPredicate(BooleanPredicate):
    """AndPredicate combines two predicates using 'AND' boolean operator
    AndPredicate is functionnaly equivalent to (left_predicate(left_property.value) AND right_predicate(right_property.value))
    """
    left_predicate: FeaturePropertyPredicate
    right_predicate: FeaturePropertyPredicate

class NotPredicate(BooleanPredicate):
    """NotPredicate negates a predicate
    NotPredicate is functionnaly equivalent to NOT(predicate(property.value))
    """
    predicate: FeaturePropertyPredicate
