from bson import ObjectId
from bson.codec_options import TypeCodecBase


class ObjectIdCodec(TypeCodecBase):
    @property
    def bson_type(self):
        """The BSON type acted upon by this type codec."""
        return ObjectId

    def transform_bson(self, value):
        """Function that transforms a vanilla BSON type value into our
    custom type."""
        return str(value)
