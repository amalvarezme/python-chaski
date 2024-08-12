"""
=============================================
Unit tests for the CertificateAuthority class
=============================================

These tests ensure the correct functionality of the CertificateAuthority,
including the creation of certificate authority (CA) certificates, private keys,
certificate signing requests (CSRs), and the signing of CSRs.

Dependencies:
- os
- unittest
- ipaddress
- cryptography (x509, default_backend, padding, hashes, serialization)
- chaski.utils.certificate_authority (CertificateAuthority)

Classes:
- TestCertificateAuthority: Unit tests for CertificateAuthority.

Methods:
- setUpClass: Sets up the class by creating a directory for SSL certificates.
- ca: Property that returns an instance of CertificateAuthority for testing.
- test_ca: Tests the creation of CA certificate and private key.
- test_csr: Tests the generation of private keys and CSRs.
- test_sign: Tests signing of client and server CSRs by the CA.
"""

import os
import unittest
import ipaddress

from cryptography import x509
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives import serialization

from chaski.utils.certificate_authority import CertificateAuthority


########################################################################
class TestCertificateAuthority(unittest.IsolatedAsyncioTestCase):
    """"""

    ssl_certificates_location = 'ssl_certificates_location'

    @classmethod
    # ----------------------------------------------------------------------
    def setUpClass(cls) -> None:
        """Set up the class for the Certificate Authority tests.

        This method is a class-level setup method that prepares the
        environment for testing the Certificate Authority (CA). It
        creates a directory for storing SSL certificates if it does
        not already exist.

        Attributes
        ----------
        cls.ssl_certificates_location : str
            The location where SSL certificates are stored.
        """
        cls.ssl_certificates_location = 'ssl_certificates_location'
        if not os.path.exists(cls.ssl_certificates_location):
            os.mkdir(cls.ssl_certificates_location)

    # ----------------------------------------------------------------------
    @property
    def ca(self) -> CertificateAuthority:
        """Create and return a CertificateAuthority instance.

        This method instantiates a CertificateAuthority object with preset
        configurations for testing purposes. The attributes include an ID,
        an IP address, the SSL certificates location, and SSL certificate
        attributes like the country, locality, organization, state, and common name.

        Returns
        -------
        CertificateAuthority
            An instance of CertificateAuthority configured for testing.
        """
        ca = CertificateAuthority(
            'Test-ID',
            ipaddress.IPv4Address('192.168.0.1'),
            ssl_certificates_location=self.ssl_certificates_location,
            ssl_certificate_attributes={
                'Country Name': "CO",
                'Locality Name': "Manizales",
                'Organization Name': "DunderLab",
                'State or Province Name': "Caldas",
                'Common Name': "Chaski-Confluent",
            },
        )
        return ca

    # ----------------------------------------------------------------------
    def test_ca(self) -> None:
        """Test the creation of CA certificate and private key.

        This test verifies the functionality of the `setup_certificate_authority` method
        in the `CertificateAuthority` class. It ensures that the CA certificate and private key
        are generated and saved correctly. Additionally, it performs a validation to check
        if the private key corresponds to the generated certificate by signing and verifying a message.

        Raises
        ------
        AssertionError
            If the CA certificate or private key files do not exist.
            If the private key does not correspond to the generated certificate.
        """
        ca = self.ca
        ca.setup_certificate_authority()

        self.assertTrue(
            os.path.exists(ca.ca_certificate_path),
            'ca_certificate_path does not exist',
        )
        self.assertTrue(
            os.path.exists(ca.ca_private_key_path),
            'ca_private_key_path does not exist',
        )

        certificate = x509.load_pem_x509_certificate(
            ca.load_certificate(ca.ca_certificate_path), default_backend()
        )

        private_key = serialization.load_pem_private_key(
            ca.load_certificate(ca.ca_private_key_path),
            password=None,
            backend=default_backend(),
        )

        public_key = certificate.public_key()

        try:
            message = b"Test message"

            signature = private_key.sign(
                message, padding.PKCS1v15(), hashes.SHA256()
            )

            public_key.verify(
                signature, message, padding.PKCS1v15(), hashes.SHA256()
            )
            self.assertTrue(
                True, "The private key corresponds to the certificate."
            )
        except Exception as e:
            self.assertTrue(
                False,
                f"The private key does not correspond to the certificate: {str(e)}",
            )

    # ----------------------------------------------------------------------
    def test_csr(self) -> None:
        """Test the generation of private keys and CSRs.

        This test verifies the functionality of the `generate_key_and_csr` method
        in the `CertificateAuthority` class. It ensures that both client and server
        private keys and certificate signing requests (CSRs) are generated and saved correctly.
        It also verifies that the moduli of the private keys match those of the corresponding CSRs.

        Raises
        ------
        AssertionError
            If the private key or CSR files for either client or server do not exist.
            If the moduli of the private keys do not match those of the corresponding CSRs.
        """
        ca = self.ca

        ca.generate_key_and_csr()

        self.assertTrue(
            os.path.exists(ca.private_key_paths['client']),
            '',
        )
        self.assertTrue(
            os.path.exists(ca.private_key_paths['server']),
            '',
        )
        self.assertTrue(
            os.path.exists(ca.certificate_paths['client']),
            '',
        )
        self.assertTrue(
            os.path.exists(ca.certificate_paths['server']),
            '',
        )

        def key_modulus(key: bytes) -> int:
            private_key = serialization.load_pem_private_key(
                key, password=None, backend=default_backend()
            )
            modulus = private_key.private_numbers().public_numbers.n
            return modulus

        def csr_modulus(key: bytes) -> int:
            csr = x509.load_pem_x509_csr(key, backend=default_backend())
            public_key = csr.public_key()
            modulus = public_key.public_numbers()
            return modulus.n

        client_key_modulus = key_modulus(
            ca.load_certificate(ca.private_key_paths['client'])
        )
        client_csr_modulus = csr_modulus(
            ca.load_certificate(ca.certificate_paths['client'])
        )
        self.assertEqual(
            client_key_modulus,
            client_csr_modulus,
            "Client key modulus does not match client CSR modulus",
        )

        server_key_modulus = key_modulus(
            ca.load_certificate(ca.private_key_paths['server'])
        )
        server_csr_modulus = csr_modulus(
            ca.load_certificate(ca.certificate_paths['server'])
        )
        self.assertEqual(
            server_key_modulus,
            server_csr_modulus,
            "Server key modulus does not match Server CSR modulus",
        )

    # ----------------------------------------------------------------------
    def test_sign(self) -> None:
        """Test signing of client and server CSRs by the CA.

        This test verifies the functionality of the `sign_csr` method
        in the `CertificateAuthority` class. It ensures that the client and server
        certificate signing requests (CSRs) are signed correctly by the CA.
        After signing the CSRs, it checks that the signatures on the resulting
        certificates are valid and were created by the CA's private key.

        Raises
        ------
        AssertionError
            If there is an issue loading the CA certificates or keys.
            If the client or server certificate was not signed by the CA properly.
        """
        ca = self.ca

        ca.load_ca(
            ca_key_path=os.path.join(
                self.ssl_certificates_location, 'ca.key'
            ),
            ca_cert_path=os.path.join(
                self.ssl_certificates_location, 'ca.cert'
            ),
        )

        ca.load_key_and_csr(
            private_key_client_path=os.path.join(
                self.ssl_certificates_location, 'client_Test-ID.key'
            ),
            certificate_client_path=os.path.join(
                self.ssl_certificates_location, 'client_Test-ID.csr'
            ),
            private_key_server_path=os.path.join(
                self.ssl_certificates_location, 'server_Test-ID.key'
            ),
            certificate_server_path=os.path.join(
                self.ssl_certificates_location, 'server_Test-ID.csr'
            ),
        )

        with open(ca.certificate_signed_paths['client'], 'wb') as file:
            file.write(
                ca.sign_csr(
                    ca.load_certificate(ca.certificate_paths['client'])
                )
            )

        with open(ca.certificate_signed_paths['server'], 'wb') as file:
            file.write(
                ca.sign_csr(
                    ca.load_certificate(ca.certificate_paths['server'])
                )
            )

        ca_cert = x509.load_pem_x509_certificate(
            ca.load_certificate(ca.ca_certificate_path), default_backend()
        )
        ca_public_key = ca_cert.public_key()

        client_cert = x509.load_pem_x509_certificate(
            ca.load_certificate(ca.certificate_signed_paths['client']),
            default_backend(),
        )
        server_cert = x509.load_pem_x509_certificate(
            ca.load_certificate(ca.certificate_signed_paths['server']),
            default_backend(),
        )

        try:
            ca_public_key.verify(
                client_cert.signature,
                client_cert.tbs_certificate_bytes,
                padding.PKCS1v15(),
                client_cert.signature_hash_algorithm,
            )
            self.assertTrue(
                True, "The client certificate was signed by the CA."
            )
        except Exception as e:
            self.assertTrue(
                False,
                f"The client certificate was NOT signed by the CA: {str(e)}",
            )

        try:
            ca_public_key.verify(
                server_cert.signature,
                server_cert.tbs_certificate_bytes,
                padding.PKCS1v15(),
                server_cert.signature_hash_algorithm,
            )
            self.assertTrue(
                True, "The server certificate was signed by the CA."
            )
        except Exception as e:
            self.assertTrue(
                False,
                f"The server certificate was NOT signed by the CA: {str(e)}",
            )
