from eopf.core.data.catalog import FeaturePropertyTransformation


class DateTransformation(FeaturePropertyTransformation):
    pass


class Seconds(DateTransformation):
    pass


class Minutes(DateTransformation):
    pass


class Hours(DateTransformation):
    pass


class Days(DateTransformation):
    pass


class Month(DateTransformation):
    pass


class Year(DateTransformation):
    pass
