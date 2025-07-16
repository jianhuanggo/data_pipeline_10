class MetaSingleton:

    object_registration = {}

    def __new__(cls):
        if not hasattr(cls, "instance"):
            cls.instance = super(MetaSingleton, cls).__new__(cls)
        return cls.instance


class Meta(type):

    def __new__(mcs, class_name, bases, attrs):
        """This is a meta class, it search for a specific class name prefix and save it along with its class ptr

        Args:
            class_name: name of the object
            bases: its parents
            attrs: its member objects
        """
        _object_register = MetaSingleton()
        _new_attrs = {}
        _object_name = None
        _orig_object_name = None
        for _attr_name, _attr_value in attrs.items():
            _new_attrs[_attr_name] = _attr_value
            if _attr_name == "__qualname__" and _attr_value.startswith("AwsApi"):
                _object_name = _attr_value[len("AwsApi"):]
                _orig_object_name = _attr_value

        _identity_object = type(class_name, bases, _new_attrs)
        if _object_name and _object_name not in _object_register.object_registration:
            _object_register.object_registration[_object_name] = {"object_name": _orig_object_name,
                                                                  "object_ptr": _identity_object}

        return _identity_object


class MetaDirectiveSingleton:

    object_registration = {}

    def __new__(cls):
        if not hasattr(cls, "instance"):
            cls.instance = super(MetaDirectiveSingleton, cls).__new__(cls)
        return cls.instance


class MetaDirective(type):

    def __new__(mcs, class_name, bases, attrs):
        """This is a meta class for directive, it search for a specific class name prefix and save it along with its class ptr

        Args:
            class_name: name of the object
            bases: its parents
            attrs: its member objects
        """
        _object_register = MetaDirectiveSingleton()
        _new_attrs = {}
        _object_name = None
        _orig_object_name = None
        for _attr_name, _attr_value in attrs.items():
            _new_attrs[_attr_name] = _attr_value
            if _attr_name == "__qualname__" and _attr_value.startswith("Directive"):
                _object_name = _attr_value[len("Directive"):]
                _orig_object_name = _attr_value

        _identity_object = type(class_name, bases, _new_attrs)
        if _object_name and _object_name not in _object_register.object_registration:
            _object_register.object_registration[_object_name] = {"object_name": _orig_object_name,
                                                                  "object_ptr": _identity_object}

        return _identity_object



class MetaAPISingleton:

    object_registration = {}

    def __new__(cls):
        if not hasattr(cls, "instance"):
            cls.instance = super(MetaAPISingleton, cls).__new__(cls)
        return cls.instance


class MetaAPI(type):

    def __new__(mcs, class_name, bases, attrs):
        """This is a meta class for directive, it search for a specific class name prefix and save it along with its class ptr

        Args:
            class_name: name of the object
            bases: its parents
            attrs: its member objects
        """
        _object_register = MetaAPISingleton()
        _new_attrs = {}
        _object_name = None
        _orig_object_name = None
        for _attr_name, _attr_value in attrs.items():
            _new_attrs[_attr_name] = _attr_value
            if _attr_name == "__qualname__" and _attr_value.startswith("API"):
                _object_name = _attr_value[len("API"):]
                _orig_object_name = _attr_value

        _identity_object = type(class_name, bases, _new_attrs)
        if _object_name and _object_name not in _object_register.object_registration:
            _object_register.object_registration[_object_name] = {"object_name": _orig_object_name,
                                                                  "object_ptr": _identity_object}

        return _identity_object