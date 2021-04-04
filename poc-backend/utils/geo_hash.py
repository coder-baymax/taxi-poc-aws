from collections import namedtuple

Point = namedtuple("Point", ["lat", "lng"])


class GeoHash:
    MAX_BIT_PRECISION = 64

    def __init__(self):
        self.bits = 0
        self.point = None
        self.precision = 0

    def add_on_bit(self):
        self.precision += 1
        self.bits <<= 1
        self.bits |= 1

    def add_off_bit(self):
        self.precision += 1
        self.bits <<= 1

    @classmethod
    def from_lat_lng(cls, latitude, longitude, precision):
        def divide_range_encode(value, value_range):
            mid = sum(value_range) / 2
            if value >= mid:
                geo.add_on_bit()
                value_range[0] = mid
            else:
                geo.add_off_bit()
                value_range[1] = mid

        geo = GeoHash()
        is_even_bit = True
        latitude_range = [-90, 90]
        longitude_range = [-180, 180]

        for _ in range(min(precision, cls.MAX_BIT_PRECISION)):
            if is_even_bit:
                divide_range_encode(longitude, longitude_range)
            else:
                divide_range_encode(latitude, latitude_range)
            is_even_bit = not is_even_bit

        geo.bits <<= (cls.MAX_BIT_PRECISION - geo.precision)
        geo.point = Point(latitude, longitude)

        return geo

    @classmethod
    def from_bits(cls, bits, precision):
        def divideRangeDecode(value_range, flag):
            mid = sum(value_range) / 2
            if flag:
                geo.add_on_bit()
                value_range[0] = mid
            else:
                geo.add_off_bit()
                value_range[1] = mid

        geo = GeoHash()
        is_even_bit = True
        latitude_range = [-90, 90]
        longitude_range = [-180, 180]
        for c in ("{:0" + str(cls.MAX_BIT_PRECISION) + "b}").format(bits)[:precision]:
            if is_even_bit:
                divideRangeDecode(longitude_range, c != '0')
            else:
                divideRangeDecode(latitude_range, c != '0')
            is_even_bit = not is_even_bit

        latitude = sum(latitude_range) / 2
        longitude = sum(longitude_range) / 2

        geo.bits <<= (cls.MAX_BIT_PRECISION - geo.precision)
        geo.point = Point(latitude, longitude)

        return geo


if __name__ == '__main__':
    geohash = GeoHash.from_lat_lng(40.7643574, -73.92346189999999, 40)
    print(bin(geohash.bits))
    print(geohash.bits, geohash.point)
    newhash = GeoHash.from_bits(geohash.bits, 40)
    print(newhash.bits, newhash.point)
