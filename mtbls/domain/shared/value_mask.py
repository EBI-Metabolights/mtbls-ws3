import re


class ValueMaskUtility(object):
    EXACT_MATCHES = {"token"}
    SEARCH_PATTERNS = {
        "user_token": "uuid",
        "apitoken": "uuid",
        "api_token": "uuid",
        "to_address": "email",
        "email": "email",
        "password": "secret",
        "credential": "secret",
        "secret": "secret",
        "private_key": "secret",
        "consumer_key": "uuid",
        "bearer": "uuid",
        "client_id": "uuid",
        "access_key": "secret",
    }

    @classmethod
    def mask_value(cls, name: str, value: str):
        if name:
            for val in cls.SEARCH_PATTERNS:
                if val in name.lower() or name.lower() in cls.EXACT_MATCHES:
                    mask_type = cls.SEARCH_PATTERNS[val]
                    if mask_type == "uuid":
                        return cls.mask_uuid(value)
                    elif mask_type == "email":
                        return cls.mask_email(value)
                    elif mask_type == "secret":
                        return "***********"
        return value

    @classmethod
    def mask_uuid(cls, value):
        if len(value) < 2:
            return value
        replaced = re.sub("[^-]", "*", value)
        if len(value) > 7:
            replaced = value[:3] + replaced[3:-3] + value[-3:]
        else:
            replaced = value[:1] + replaced[1:-1] + value[-1:]

        return replaced

    @classmethod
    def mask_email(cls, value: str):
        if not value:
            return ""
        data = value.split("@")
        if len(data) > 1:
            head = data[0]
            if len(head) > 3:
                replaced = re.sub(r"[\w]", "*", head)
                replaced = head[0] + replaced[1:-1] + head[-1:]
            else:
                if len(head) > 1:
                    replaced = head[0] + replaced[1:]
                else:
                    replaced = "*"
            data[0] = replaced
            email = "@".join(data)
            return email

        replaced = re.sub(r"[\w]", "*", value)
        if len(replaced) > 2:
            replaced = value[0] + replaced[1:-1] + value[-1:]
        return replaced
