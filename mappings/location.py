class address:
    line = [None, None]
    city = None
    state = None
    postal_code = None
    country = None


def get_location_id(address):
    return hash(address)


def hash(input_str):
    return input_str


def map_location(df):
    return df
