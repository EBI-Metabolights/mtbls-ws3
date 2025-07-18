from mtbls.domain.exceptions.base import NotFoundError


class RepositoryError(Exception): ...


class RepositoryConnectionError(RepositoryError): ...


class StudyResourceError(Exception): ...


class StudyResourceNotFoundError(StudyResourceError, NotFoundError):
    def __init__(self, resource_id: str) -> None:
        self.resource_id = resource_id


class StudyBucketNotFoundError(StudyResourceNotFoundError):
    def __init__(self, resource_id: str, bucket_name: str) -> None:
        super().__init__(resource_id)
        self.bucket_name = bucket_name


class StudyObjectNotFoundError(StudyResourceNotFoundError):
    def __init__(self, resource_id: str, bucket_name: str, object_key: str) -> None:
        super().__init__(resource_id)
        self.bucket_name = bucket_name
        self.object_key = object_key


class UriError(Exception):
    def __init__(self, uri: str) -> None:
        self.uri = uri


class InvalidUriError(UriError):
    def __init__(self, uri: str) -> None:
        super().__init__(uri)


class UnaccessibleUriError(UriError):
    def __init__(self, uri: str) -> None:
        super().__init__(uri)


class UnsupportedUriError(UriError):
    def __init__(self, uri: str) -> None:
        super().__init__(uri)


class AlreadyExistsError(StudyResourceError):
    def __init__(self, resource_id: str) -> None:
        super().__init__(resource_id)


class StudyResourceAlreadyExistsError(AlreadyExistsError):
    def __init__(self, resource_id: str) -> None:
        super().__init__(resource_id)


class StudyBucketAlreadyExistsError(AlreadyExistsError):
    def __init__(self, resource_id: str, bucket_name: str) -> None:
        super().__init__(resource_id)
        self.bucket_name = bucket_name


class StudyObjectAlreadyExistsError(AlreadyExistsError):
    def __init__(self, resource_id: str, bucket_name: str, object_key: str) -> None:
        super().__init__(resource_id)
        self.bucket_name = bucket_name
        self.object_key = object_key


class UnexpectedStudyObjectTypeError(StudyResourceError):
    def __init__(self, resource_id: str, bucket_name: str, object_key: str) -> None:
        self.resource_id = resource_id
        self.bucket_name = bucket_name
        self.object_key = object_key


class StudyObjectIsNotFolderError(UnexpectedStudyObjectTypeError):
    def __init__(self, resource_id: str, bucket_name: str, object_key: str) -> None:
        super().__init__(resource_id, bucket_name, object_key)


class StudyObjectIsNotFileError(UnexpectedStudyObjectTypeError):
    def __init__(self, resource_id: str, bucket_name: str, object_key: str) -> None:
        super().__init__(resource_id, bucket_name, object_key)
