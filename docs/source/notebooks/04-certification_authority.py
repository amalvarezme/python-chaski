# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.16.2
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% editable=true slideshow={"slide_type": ""} nbsphinx="hidden"
import sys
import asyncio

sys.path.append('../../..')

# %% [markdown]
# # Certification Authority
#
# A Certification Authority (CA) is a trusted entity that issues digital certificates.
# These digital certificates are used to verify the identity of entities such as
# websites, email addresses, or companies. The CA plays a critical role in
# Public Key Infrastructure (PKI), which ensures secure, encrypted communication
# over networks like the internet.
#
# The main functions of a CA include:
#
# - **Issuing Certificates**: They generate and distribute digital certificates
#   to entities after verifying their identity.
# - **Revoking Certificates**: If a certificate is found to be compromised or
#   invalid, the CA can revoke it and maintain a list of revoked certificates.
# - **Validating Certificates**: They provide mechanisms to verify the validity
#   of certificates, ensuring that entities are communicating securely.
#
# Popular Certification Authorities include Let's Encrypt, Verisign, and
# Comodo. They follow stringent protocols to maintain security and trust
# across digital communications.

# %% [markdown]
# ## Chaski Certification Authority
#
# The Chaski Certification Authority (CA) module provides essential functions for managing digital certificates within the Chaski distributed network framework. It plays a critical role in ensuring secure and encrypted communication between nodes in a distributed network environment.
#
# The implemented functionalities of the Chaski CA include:
#
# - **Certificate Generation**: The CA generates digital certificates for nodes, which can be used to establish secure connections. This involves creating a public-private key pair and signing the certificate with the CA's private key.
# - **Certificate Revocation**: If a certificate is compromised or needs to be invalidated, the CA can revoke it. The revoked certificates are maintained in a Certificate Revocation List (CRL).
# - **Certificate Validation**: Nodes can validate digital certificates issued by the CA to ensure they are authentic and have not been revoked.
# - **Secure Communication**: The CA facilitates the establishment of SSL/TLS contexts for secure communications between nodes using issued certificates.
#
# The `CertificateAuthority` class in the `chaski.utils.certificate_authority` module implements these features. This class is used by Chaski nodes to request certificates, validate peers, and manage secure communications:

# %%
from chaski.utils.certificate_authority import CertificateAuthority
import ipaddress

# %%
ca = CertificateAuthority(
    'Test-ID',
    ipaddress.IPv4Address('192.168.0.1'),
    ssl_certificates_location='location_dir',
    ssl_certificate_attributes={
        'Country Name': "CO",
        'Locality Name': "Manizales",
        'Organization Name': "DunderLab",
        'State or Province Name': "Caldas",
        'Common Name': "Chaski-Confluent",
    },
)

# %% [markdown]
# This code initializes an instance of the `CertificateAuthority` class using the provided parameters.
#
# - `'Test-ID'`: A string identifier for the certificate authority instance.
# - `ipaddress.IPv4Address('192.168.0.1')`: The IP address assigned to the CA.
# - `ssl_certificates_location=self.ssl_certificates_location`: The directory path where SSL certificates are stored.
# - `ssl_certificate_attributes`: A dictionary containing attributes for the SSL certificate, such as the country, locality, organization, state, and common name.
#
# This instance will be used for generating, revoking, and validating digital certificates, and for facilitating secure communications in the Chaski network.


# %%
ca.setup_certificate_authority()

# %% [markdown]
# This code initializes the Certificate Authority, setting up the necessary
# directory structure and creating the CA's private key and self-signed root certificate.

# %%
ca.generate_key_and_csr()

# %% [markdown]
# This code generates a new private key and a Certificate Signing Request (CSR) for the certificate authority.
# The private key is stored securely, and the CSR is later used to generate valid certificates.

# %%
ca.sign_csr(
    """-----BEGIN CERTIFICATE REQUEST-----
MIICpjCCAY4CAQAwYTELMAkGA1UEBhMCQ08xDzANBgNVBAgMBkNhbGRhczESMBAG
A1UEBwwJTWFuaXphbGVzMRIwEAYDVQQKDAlEdW5kZXJMYWIxGTAXBgNVBAMMEENo
YXNraS1Db25mbHVlbnQwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQCn
BX6Qsyc7+idCKIvONxvXQNWuSjvREJFjyf4+dT0bbMW6E2C77uJXuNwZ0pA3eIkb
if3zK9tvfCtScitmZulo0rTyG+hGT5i6SI/CJu2icgl05/67EKr8ffCSij1fj/bW
9IMY6qu5Nxd+9fdRMgpS1O2td3HuXnT+SsI3H+zillXq2oEsClivnREN4gyd2gYj
KiRyXw7xNReAkr/ixU9/ODEhhHO/ZoA3JpCdrJ5bCiiInx4SuUdKEgqft1IxF1Qt
V9NyGacv2YhuNc/T2rRqkt8EOP8GTKaKc8DpToTwpKz7jCObPB3A7A9g/3QBvcqz
hXosRTCswC/iFoKdJENxAgMBAAGgADANBgkqhkiG9w0BAQsFAAOCAQEAkrdgYq54
NwCkJWfPM7oPako3kKama0GnZBOOHhQvnWOiHY3C6pfUmgzhUuyiXhxy/oA54rQn
YuREmXDqdr7yBRA25uFEavt3LpvGxZQZbySFWGE6e1C1OzUhdGlcup0w+e9OXaMs
A5q0q7aBrRpAhAizVVzLdZMZZEPv2d2ngswnneZqv0BQuCTD15KYpYgFYK20ZRq8
GLX9B7MHDUbfPean+AOTPz62pDYyR/3Pgt86G6SGPJ46vPYCjDJJpjoxdAxrw8Tl
7lWTN+4zP/WY5IyY5+nKGUV6lSxtH9rl02vNaeYc+Ucvq/S8VwlYLIcGIVuOpNpl
y+BBeyMYAw3A1Q==
-----END CERTIFICATE REQUEST-----"""
)

# %% [markdown]
# The `sign_csr` method signs the previously generated Certificate Signing Request (CSR) with the CA's private key to create a valid digital certificate. This step finalizes the certificate creation process, allowing it to be used for secure communications.

# %% [markdown]
# ## Chaski CA Server
# The following code creates an instance of the `ChaskiCA` class. This class
# is responsible for managing the certification authority server in the Chaski
# network.

# %%
from chaski.ca import ChaskiCA

ca = ChaskiCA(
    port=65432,
    ssl_certificates_location='chaski_ca_dir',
    name='ChaskiCA',
    ssl_certificate_attributes={
        'Country Name': "CO",
        'Locality Name': "Manizales",
        'Organization Name': "DunderLab",
        'State or Province Name': "Caldas",
        'Common Name': "Chaski-Confluent",
    },
)

# %% [markdown]
# You can also run it using the command `chaski_ca`.
#
# Once started, the Chaski Certification Authority Server (`ChaskiCA`) will:
# - Listen for incoming certificate requests on the specified port.
# - Process these requests to generate and sign new certificates.
# - Provide functionalities to revoke and validate certificates.
# - Ensure secure and authenticated communication within the Chaski network.
#
# Additional functionalities of the ChaskiCA include:
# - Logging: Monitor and log activities for auditing and troubleshooting purposes.
# - Configuration: Easily customizable settings for your specific network environment.
# - Scalability: Designed to handle multiple certificate requests simultaneously, ensuring seamless operations.

# %% [markdown]
# ## Integrate CA in all nodes
#
# The Certification Authority (CA) can be used by any Chaski Node to manage
# secure communications. By integrating the Chaski CA, nodes can request SSL
# certificates, set up secure connections, and validate peers within the Chaski network.
# This ensures all nodes communicate securely, maintaining the integrity and
# confidentiality of the transmitted data.

# %% [markdown]
#
# The following code initializes an instance of the `ChaskiStreamer` class. The `ChaskiStreamer`
# acts as a producer node in the network. The producer node subscribes to a topic named `topic1`
# and uses SSL certificates for secure communication. The SSL certificates are stored in the
# `certs_ca` directory.
#
# In the first part of the script, the producer node is instantiated, and then it requests an SSL
# certificate from the Chaski Certification Authority located at `ChaskiCA@127.0.0.1:65432`.
#
# The second part of the script demonstrates an alternative way of requesting the SSL certificate.
# Here, the SSL certificate request is made inline during the initialization of the `ChaskiStreamer`
# instance.

# %%
node = ChaskiStreamer(
    port=65433,
    name='Producer',
    subscriptions=['topic1'],
    reconnections=None,
    ssl_certificates_location='certs_ca',
)

await producer.request_ssl_certificate('ChaskiCA@127.0.0.1:65432')

# %%
node = ChaskiStreamer(
    port=65433,
    name='Producer',
    subscriptions=['topic1'],
    reconnections=None,
    ssl_certificates_location='certs_ca',
    request_ssl_certificate='ChaskiCA@127.0.0.1:65432',
)

# %% [markdown]
# ## Conclusion
#
# The Chaski Certification Authority (CA) is a critical component to ensure the integrity and security
# of communications within a distributed network. By leveraging the CA, nodes can securely request,
# validate, and manage digital certificates, establishing trust and encrypted communication channels.
# This notebook has demonstrated the process of setting up the CA, generating and signing certificates,
# and integrating the CA into the Chaski nodes.
#
# Make sure that the CA server is running before attempting to request certificates from any node.
# With this setup, you can confidently manage and secure the communication within your Chaski network,
# ensuring data integrity and confidentiality.
