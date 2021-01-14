from dataclasses import dataclass
from enum import Enum
from eopf.core.data.storage import Credentials
from typing import Any, Dict, List, Optional
import geojson # type: ignore
from abc import ABC, abstractmethod


class Geometry(geojson.GeoJSON):
    """Defines a Geometry
    """

class Feature(geojson.Feature):
    """A Feature object represents a spatially bounded asset:
    A feature object has a member with the name "geometry". The value of the geometry member is a geometry object as defined above or a None value.
    A feature object must have a member with the name "properties". The value of the properties member is an object (any JSON object or a None value.
    If a feature has a commonly used identifier, that identifier should be included as a member of the feature object with the name "id".
    """

    def __init__(self, id, geometry: Geometry, properties: Dict[str, Any]):
        super().__init__(id=id, geometry=geometry, properties=properties)


class FeatureCollection(geojson.FeatureCollection):
    """ FeatureCollection is a collection of Features
    """

    def __init__(self, features: List[Feature]):
        super().__init__(features)

@dataclass
class FeatureCollectionDescription:
    """FeatureCollectionDescription contains the name of the feature collection and its description
    """
    name: str
    description : str


class FeaturePropertyTransformation(ABC):
    """FeaturePropertyTransformation is an abstract class from which all Feature property transformation types must inherit
    """

class Predicate(ABC):
    """Predicate is an abstract class from which all concrete predicates must inherit
    """

@dataclass
class FeaturePropertyPredicate(Predicate):
    """FeaturePropertyPredicate is an abstract class from which all Feature property predicates types must inherit
    Functionaly a FeaturePropertyPredicate is equivalent to Predicate(Transform(property.value)) 
    """
    property_name: str
    """Name of the property to test
    """
    transformation: FeaturePropertyTransformation
    """Transformation to apply to the property to compute the predicate
    """


@dataclass
class FeaturePropertyFilter:
    """FeaturePropertyFilter is used in SearchRequest to define filters that must applied to Feature properties
    """
    predicates: List[FeaturePropertyPredicate]
    """List of predicates to apply the the transformed value used to filter features
    """

class GeometricOperator(Enum):
    IS_INSIDE = 0
    CONTAINS = 1
    INTERSECTS = 2

@dataclass
class GeometryPredicate(Predicate):
    """GeometryFilter is used in SearchRequest to define filters that must applied to Feature geometry
    Functionaly a geometric filter is equivalent to:
        if negate:
              return not (feature.geometry operator geometry)
        else:
            return (feature.geometry operator geometry)
    """
    operator: GeometricOperator
    geometry: Geometry

@dataclass
class GeometricFilter:
    predicates: List[GeometryPredicate]


class OrderType(Enum):
    """OrderType allows choosing if features are ordered in ascending order or in descending order
    """
    ASC = 0
    DSC = 1


@dataclass
class OrderByOption:
    """OrderByOption is used by SearchRequest to order Feature of the selected FeatureCollection
    """
    property_name: str
    direction: OrderType


@dataclass
class TopOption:
    """TopOption is used by SearchRequest to keep only the first 'top' Features of the selected FeatureCollection
    """
    top: int


@dataclass
class SkipOption:
    """SkipOption is used by SearchRequest skip the first 'skip' Features of the selected FeatureCollection
    """
    skip: int


@dataclass
class SearchRequest:
    """SearchRequest is used by the concrete ClientAPIs to select Features among feature collection referenced by a service
    Functionnaly a SearchRequest is equavalent to top(skip(geometric_filters(property_filter(feature_collection))))
    """

    feature_collection_name: str
    """feature_collection_name is the name of the feature collection in where features are searched
    """
    feature_id : Optional[str] = None
    """feature_id is the identifier of the feature
    """    
    properties_filter: Optional[FeaturePropertyFilter] = None
    """properties_filter is used to select features in the feature collection applying predicates to properties values
    A feature is selected if all predicate are validated
    """
    geometry_filter: Optional[GeometricFilter] = None
    """geometry_filter is used to select features in the feature collection applying predicates to features geometry
    """
    order_by: Optional[OrderByOption] = None
    """order_by is used to sort feature in ascending or descending order
    """
    skip: Optional[SkipOption] = None
    """skip is used to skip the n first features
    """
    top: Optional[TopOption] = None
    """top is used to keep only the n first feature
    """


@dataclass
class UpdateRequest():
    """UpdateRequest is used to add a Feature to a FeatureCollection
    """
    feature_collection_name: str
    feature_id: str
    feature: Feature


@dataclass
class AddRequest():
    """RemoveRequest is used to add a Feature to a FeatureCollection
    """
    feature_collection_name: str
    feature: Feature


@dataclass
class DeleteRequest():
    """RemoveRequest is used to remove Features that match search_request
    """
    search_request: SearchRequest


class ClientAPI(ABC):
    """ClientAPI is an abstract class defining the methods for interacting with the EOPF External Interfaces.
    """

    def __init__(self, service_url: str, credentials: Credentials) -> None:
        """Constructor of the client API

        :param service_url: URL of the service to request
        :type service_url: str
        :param credentials: User credentials to use to authenticate to the service
        :type credentials: Credentials
        """
        super().__init__()
        self.service_url: str = service_url
        self.credentials: Credentials = credentials
    
    @abstractmethod
    def list_collections(self) -> List[FeatureCollectionDescription]:
        """Returns The list of descriptions of the feature collections that are made available by the service addressed

        :return: The list of descriptions of the feature collections
        :rtype: List[FeatureCollectionDescription]
        """

    @abstractmethod
    def get(self, feature_collection_name: str, feature_id: str) -> Optional[Feature]:
        """Returns the Feature referenced by a feature collection name and a feature id

        :param feature_collection_name: feature collection name
        :type feature_collection_name: str
        :param feature_id: feature id
        :type feature_id: str
        :return: the Feature or None if the Feature does not exits
        :rtype: Optional[Feature]
        """

    @abstractmethod
    def search(self, request: SearchRequest) -> FeatureCollection:
        """Returns a feature collection regrouping the features selected using the request

        :param request: defines filters and options that are used to select features into a feature collection
        :type request: SearchRequest
        :return: The feature collection regrouping the features selected using the reques
        :rtype: FeatureCollection
        """

    @abstractmethod
    def update(self, request: UpdateRequest) -> Optional[Feature]:
        """Uses to update a Feature and returns the updated Feature

        :param request: the update request containing the new Feature content
        :type request: UpdateRequest
        :return: the updated feature or None if the Feature does not exists
        :rtype: Feature
        """

    @abstractmethod
    def add(self, request: AddRequest) -> Feature:
        """Adds a new feature into a feature collection 

        :param request: contains the new Feature
        :type request: AddRequest
        :return: the added feature with an updated id
        :rtype: Feature
        """

    @abstractmethod
    def delete(self, request: DeleteRequest) -> Feature:
        """Removes a feature from a feature collection

        :param request: contains the reference to the feature to remove
        :type request: RemoveRequest
        :return: the removed feature
        :rtype: Feature
        """
