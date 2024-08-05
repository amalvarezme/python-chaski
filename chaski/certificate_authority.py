import os
import ssl
import datetime

# Importing cryptography modules for X509 certificates, private key generation,
# and serialization, including RSA key generation and PEM encoding without encryption.
from cryptography import x509
from cryptography.x509.oid import NameOID
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.serialization import (
    load_pem_private_key,
    Encoding,
    PrivateFormat,
    NoEncryption,
)


########################################################################
class CertificateAuthority:
    """"""

    # ----------------------------------------------------------------------
    def __init__(self, id, certificates_location='.', ssl_certificate_attributes={}):
        """"""
        self.id = id
        self.certificates_location = certificates_location
        self.ssl_certificate_attributes = ssl_certificate_attributes

    # ----------------------------------------------------------------------
    def setup_certificate_authority(self):
        """"""
        ca_key_path = os.path.join(self.certificates_location, 'ca.pk')
        ca_cert_path = os.path.join(self.certificates_location, 'ca_certificate')

        if not os.path.exists(ca_key_path) or not os.path.exists(ca_cert_path):
            # Generate a new RSA private key with a public exponent of 65537 and a key size of 2048 bits
            self.ca_key = rsa.generate_private_key(
                public_exponent=65537,
                key_size=2048,
            )

            # Save the CA's private key to a file in PEM format without encryption
            with open(ca_key_path, "wb") as key_file:
                key_file.write(
                    self.ca_key.private_bytes(
                        encoding=Encoding.PEM,
                        format=PrivateFormat.TraditionalOpenSSL,
                        encryption_algorithm=NoEncryption(),
                    )
                )

            # Create the subject and issuer name for the CA certificate with relevant attributes
            subject = issuer = x509.Name(
                [
                    x509.NameAttribute(
                        NameOID.COUNTRY_NAME,
                        self.ssl_certificate_attributes['Country Name'],
                    ),
                    x509.NameAttribute(
                        NameOID.STATE_OR_PROVINCE_NAME,
                        self.ssl_certificate_attributes['State or Province Name'],
                    ),
                    x509.NameAttribute(
                        NameOID.LOCALITY_NAME,
                        self.ssl_certificate_attributes['Locality Name'],
                    ),
                    x509.NameAttribute(
                        NameOID.ORGANIZATION_NAME,
                        self.ssl_certificate_attributes['Organization Name'],
                    ),
                    x509.NameAttribute(
                        NameOID.COMMON_NAME,
                        self.id,
                    ),
                ]
            )

            # Create a new CA certificate using the built CertificateBuilder
            # object, with details such as subject, issuer, public key,
            # serial number, validity period, and basic constraints.
            ca_cert = (
                x509.CertificateBuilder()
                .subject_name(subject)
                .issuer_name(issuer)
                .public_key(self.ca_key.public_key())
                .serial_number(x509.random_serial_number())
                .not_valid_before(datetime.datetime.utcnow())
                .not_valid_after(
                    datetime.datetime.utcnow() + datetime.timedelta(days=365)
                )
                .add_extension(
                    x509.BasicConstraints(ca=True, path_length=None),
                    critical=True,
                )
                .sign(self.ca_key, hashes.SHA256())
            )

            # Write the CA's certificate to the specified path in PEM format
            with open(ca_cert_path, "wb") as cert_file:
                cert_file.write(ca_cert.public_bytes(Encoding.PEM))
        else:
            # Load the existing CA private key from the specified file path in PEM format
            with open(ca_key_path, "rb") as key_file:
                self.ca_key = load_pem_private_key(key_file.read(), password=None)

        self.ca_key_path_ = ca_key_path
        self.ca_cert_path_ = ca_cert_path

    # ----------------------------------------------------------------------
    @property
    def ca_private_key_path(self) -> str:
        """
        Return the path to the Certificate Authority (CA) private key.

        This property method checks if the `ca_key_path_` attribute is set, which
        stores the file path to the CA's private key. If the attribute is set,
        the method returns this path. If not, an exception is raised indicating
        that the CA key path is not set.

        Returns
        -------
        str
            The file path to the CA's private key.

        Raises
        ------
        Exception
            If the CA key path is not set.
        """
        if hasattr(self, 'ca_key_path_'):
            return self.ca_key_path_
        else:
            raise Exception("CA key path not set")

    # ----------------------------------------------------------------------
    @property
    def ca_certificate_path(self) -> str:
        """Return the path to the Certificate Authority (CA) certificate.

        This property retrieves the file path to the CA's certificate,
        ensuring the path is set before returning it. If the path is not
        set, an exception will be raised.

        Returns
        -------
        str
            The file path to the CA's certificate.

        Raises
        ------
        Exception
            If the CA certificate path is not set.
        """
        if hasattr(self, 'ca_cert_path_'):
            return self.ca_cert_path_
        else:
            raise Exception("CA certificate path not set")

    # ----------------------------------------------------------------------
    def sign_csr(self, csr_data: bytes, ca_key_path: str, ca_cert_path: str) -> bytes:
        """
        Sign a Certificate Signing Request (CSR) with the Certificate Authority (CA) key.

        This method signs a given CSR using the CA's private key and generates a certificate
        that is valid for one year. The signed certificate is returned in PEM format.

        Parameters
        ----------
        csr_data : bytes
            The certificate signing request data in PEM format.
        ca_key_path : str
            The file path to the CA's private key in PEM format.
        ca_cert_path : str
            The file path to the CA's certificate in PEM format.

        Returns
        -------
        bytes
            The signed certificate in PEM format.

        Raises
        ------
        ValueError
            If the provided CSR data is invalid or cannot be loaded.
        FileNotFoundError
            If the CA key or certificate files do not exist at the specified paths.
        """
        # Load the CA's private key from the specified file path in PEM format
        with open(ca_key_path, 'rb') as key_file:
            ca_key = load_pem_private_key(key_file.read(), password=None)

        # Load the existing CA certificate from the specified file path in PEM format
        with open(ca_cert_path, 'rb') as cert_file:
            ca_cert = x509.load_pem_x509_certificate(cert_file.read())

        # Load the Certificate Signing Request (CSR) from PEM-formatted data
        csr = x509.load_pem_x509_csr(csr_data)

        # Create a new certificate signed by the CA based on the provided Certificate Signing Request (CSR)
        cert = (
            x509.CertificateBuilder()
            .subject_name(csr.subject)  # Set the subject name from the CSR information
            .issuer_name(ca_cert.subject)  # Set the issuer name from the CA certificate
            .public_key(csr.public_key())  # Set the public key from the CSR information
            .serial_number(
                x509.random_serial_number()
            )  # Generate a random serial number for the certificate
            .not_valid_before(
                datetime.datetime.now(datetime.UTC)
            )  # Set the certificate's validity start time to current UTC time
            .not_valid_after(  # Set the certificate's validity end time to one year from now
                datetime.datetime.now(datetime.UTC) + datetime.timedelta(days=365)
            )
            .sign(
                ca_key, hashes.SHA256()
            )  # Sign the certificate with the CA's private key using SHA256 hashing algorithm
        )

        # Return the signed certificate in PEM format
        return cert.public_bytes(encoding=serialization.Encoding.PEM)

    # ----------------------------------------------------------------------
    def generate_keys_and_csr(self) -> None:
        """
        Generate RSA private key and a Certificate Signing Request (CSR).

        This method generates a new 2048-bit RSA private key and creates a Certificate Signing Request (CSR)
        with the specified attributes. The private key and CSR are saved in PEM format in the specified location.

        The private key is saved as `<id>.pk.pem`, and the CSR is saved as `<id>.csr.pem`, where `id` is the
        unique identifier of the instance.

        Attributes used for the CSR include:
        - Country Name
        - State or Province Name
        - Locality Name
        - Organization Name
        - Common Name

        The CSR is signed using the SHA256 hashing algorithm.

        Raises
        ------
        IOError
            If there is an error in writing the private key or CSR to the file system.
        """
        # Generate a new RSA private key with a public exponent of 65537 and a key size of 2048 bits
        key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048,
        )

        self.private_key_path_ = os.path.join(
            self.certificates_location, f'{self.id}.pk.pem'
        )
        self.certificate_path_ = os.path.join(
            self.certificates_location, f'{self.id}.csr.pem'
        )

        # Write the newly generated private key to a PEM file named after the node
        with open(self.private_key_path_, 'wb') as key_file:
            key_file.write(
                key.private_bytes(
                    encoding=serialization.Encoding.PEM,
                    format=serialization.PrivateFormat.TraditionalOpenSSL,
                    encryption_algorithm=serialization.NoEncryption(),
                )
            )

        # Create a Certificate Signing Request (CSR) with the specified details,
        # including country, state, locality, organization, and common name,
        # and sign it with the generated RSA private key using SHA256 hashing algorithm
        csr = (
            x509.CertificateSigningRequestBuilder()
            .subject_name(
                x509.Name(
                    [
                        x509.NameAttribute(
                            NameOID.COUNTRY_NAME,
                            self.ssl_certificate_attributes['Country Name'],
                        ),
                        x509.NameAttribute(
                            NameOID.STATE_OR_PROVINCE_NAME,
                            self.ssl_certificate_attributes['State or Province Name'],
                        ),
                        x509.NameAttribute(
                            NameOID.LOCALITY_NAME,
                            self.ssl_certificate_attributes['Locality Name'],
                        ),
                        x509.NameAttribute(
                            NameOID.ORGANIZATION_NAME,
                            self.ssl_certificate_attributes['Organization Name'],
                        ),
                        x509.NameAttribute(
                            NameOID.COMMON_NAME,
                            self.id,
                        ),
                    ]
                )
            )
            .sign(key, hashes.SHA256())
        )

        # Save the generated Certificate Signing Request (CSR) to a PEM file named after the node
        with open(self.certificate_path_, 'wb') as csr_file:
            csr_file.write(csr.public_bytes(serialization.Encoding.PEM))

    # ----------------------------------------------------------------------
    @property
    def private_key_path(self) -> str:
        """
        Return the path to the node's private key.

        This property checks if the `private_key_path_` attribute is set,
        which stores the file path to the private key. If set, the method
        returns this path. Otherwise, it raises an exception.

        Returns
        -------
        str
            The file path to the private key.

        Raises
        ------
        Exception
            If the private key path is not set.
        """
        if hasattr(self, 'private_key_path_'):
            return self.private_key_path_
        else:
            raise Exception("Private key path not set")

    # ----------------------------------------------------------------------
    @property
    def certificate_path(self) -> str:
        """
        Retrieve the path to the node's Certificate Signing Request (CSR).

        This property checks if the `certificate_path_` attribute is set,
        which stores the file path to the CSR. If set, the method returns this path.
        Otherwise, it raises an exception indicating that the CSR path is not set.

        Returns
        -------
        str
            The file path to the CSR.

        Raises
        ------
        Exception
            If the CSR path is not set.
        """
        if hasattr(self, 'certificate_path_'):
            return self.certificate_path_
        else:
            raise Exception("CA certificate path not set")

    # ----------------------------------------------------------------------
    @property
    def certificate_signed_path(self) -> str:
        """
        Provide the path to the signed certificate.

        This property retrieves the file path to the signed certificate
        by replacing the '.csr.pem' suffix of the CSR path with
        '.sign.csr.pem'.

        Returns
        -------
        str
            The file path to the signed certificate.

        Raises
        ------
        Exception
            If the CSR path is not set.
        """
        return self.certificate_path.replace('.csr.pem', '.sign.csr.pem')

    # ----------------------------------------------------------------------
    def load_certificate(self, path: str) -> bytes:
        """
        Load a certificate from the specified file path.

        This method reads a certificate file in binary mode from the given path
        and returns its content as bytes. It is used to load certificates that
        may be required for various cryptographic operations.

        Parameters
        ----------
        path : str
            The file path to the certificate file to be loaded.

        Returns
        -------
        bytes
            The content of the certificate file in byte format.

        Raises
        ------
        FileNotFoundError
            If the certificate file does not exist at the specified path.
        IOError
            If there is an error reading the certificate file.
        """
        with open(path, 'rb') as file:
            return file.read()

    # ----------------------------------------------------------------------
    def write_certificate(self, path: str, certificate: bytes) -> None:
        """
        Write the given certificate to a specified file path.

        This method saves the provided certificate in binary mode to the specified path.
        It ensures that the certificate data is properly written to the file system,
        making it available for subsequent cryptographic operations or verification.

        Parameters
        ----------
        path : str
            The file path where the certificate should be saved.
        certificate : bytes
            The certificate data in byte format to be written to the file.

        Raises
        ------
        IOError
            If there is an error writing the certificate file to the specified path.
        """
        with open(path, 'wb') as cert_file:
            cert_file.write(certificate)

    # ----------------------------------------------------------------------
    def get_context(self) -> ssl.SSLContext:
        """
        Create and configure an SSL context for secure communication.

        This method creates and configures an SSL context for use in secure
        communications, typically over TCP/IP connections. The method sets up
        the context to require client authentication, load the necessary certificates,
        and define verification settings.

        Returns
        -------
        ssl.SSLContext
            An SSL context configured for client authentication using the node's
            signed certificate and private key.

        Notes
        -----
        The context requires a Certificate Authority (CA) certificate to verify clients.
        The verification mode is set to require SSL certificates (CERT_REQUIRED).
        """
        # Create a default SSL context for client authentication and load the necessary certificates
        ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        # Load the node's signed certificate and private key into the SSL context for establishing secure communication channels
        ssl_context.load_cert_chain(
            certfile=self.certificate_signed_path, keyfile=self.private_key_path
        )
        # Load the CA's certificate to verify client connections in the SSL context
        ssl_context.load_verify_locations(cafile=self.ca_certificate_path)
        # Set the context to require SSL certificate verification
        ssl_context.verify_mode = ssl.CERT_REQUIRED
        return ssl_context
