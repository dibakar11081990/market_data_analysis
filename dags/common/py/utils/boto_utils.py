##########################################################################
"""
Utility file containing commonly used BOTO functions
"""
##########################################################################
# Imports
import boto3
import base64


##########################################################################
# Functions


def kms_decrypt(kms_encrypted_secret):
    """
    THis function would be used for decrypting the passwords stored in KmS
    DONOT INCLUDE A PRINT STATEMENT IN HERE AS THE PASSWORD WOULD BE EXPOSED...
    :param kms_encrypted_secret: KMS_ENCRYPTED_SECRET
    :return: password in plaintext
    """

    # kms client
    client = boto3.client('kms', region_name='us-east-1')

    # Call AWS to decrypt encrypted secret (needs to be base64encoded)
    decrypted_key = client.decrypt(CiphertextBlob=base64.b64decode(kms_encrypted_secret))

    # Set returned value to client_secret variable
    client_secret = decrypted_key['Plaintext']

    # return the client secret
    return client_secret.decode("utf-8")
