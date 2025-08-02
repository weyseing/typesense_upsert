import os
import logging
from rest_framework.authentication import BaseAuthentication
from rest_framework.exceptions import AuthenticationFailed

logger = logging.getLogger(__name__)

class TypesenseKeyAuth(BaseAuthentication):
    def authenticate(self, request):
        api_key = request.META.get('HTTP_X_API_KEY')
        if not api_key:
            logger.error('API Key not found')
            raise AuthenticationFailed('Invalid API Key')

        try:
            if api_key != os.getenv("TYPESENSE_API_KEY"):
                logger.error('API Key incorrect')
                raise AuthenticationFailed('Invalid API Key')
        except:
            raise AuthenticationFailed('Invalid API Key')

        return (None, None)