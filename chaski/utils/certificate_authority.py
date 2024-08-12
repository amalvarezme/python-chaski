import os
import ssl
import datetime

# Importing cryptography modules for X509 certificates, private key generation,
# and serialization, including RSA key generation and PEM encoding without encryption.
from cryptography import x509
from cryptography.x509.oid import NameOID
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa


########################################################################
class CertificateAuthority:
    """"""

    # ----------------------------------------------------------------------
    def __init__(
        self,
        id: str,
        ip_address: str,
        ssl_certificates_location: str = '.',
        ssl_certificate_attributes: dict = {},
    ):
        """
        Initialize the Certificate Authority (CA).

        This constructor initializes the CA instance with the provided ID,
        the directory where SSL certificates are stored, and a dictionary
        of SSL certificate attributes. These attributes are used for generating
        new certificates.
        """
        self.id = id
        self.ssl_certificates_location = ssl_certificates_location
        self.ssl_certificate_attributes = ssl_certificate_attributes
        self.ip_address = ip_address

    # ----------------------------------------------------------------------
    def setup_certificate_authority(self) -> None:
        """
        Set up the Certificate Authority (CA).

        This method sets up the CA by either generating and saving a new CA private key
        and certificate or loading existing ones. It ensures that the CA's private key and
        certificate are available for certificate signing operations. If the key and certificate
        files are missing, it generates them and saves them in the specified location.

        The following files are checked or generated:
        - CA private key: 'ca.pk'
        - CA certificate: 'ca_certificate.pem'

        Raises
        ------
        IOError
            If there is an error reading or writing the key and certificate files.
        """
        self.ca_key_path_ = os.path.join(
            self.ssl_certificates_location, "ca.key"
        )
        self.ca_cert_path_ = os.path.join(
            self.ssl_certificates_location, "ca.cert"
        )

        # Generate CA key
        ca_key = rsa.generate_private_key(
            public_exponent=65537, key_size=2048
        )

        # Write CA key to file
        with open(self.ca_key_path_, "wb") as f:
            f.write(
                ca_key.private_bytes(
                    encoding=serialization.Encoding.PEM,
                    format=serialization.PrivateFormat.TraditionalOpenSSL,
                    encryption_algorithm=serialization.NoEncryption(),
                )
            )

        # Generate CA certificate
        subject = issuer = x509.Name(
            [
                x509.NameAttribute(
                    NameOID.COUNTRY_NAME,
                    self.ssl_certificate_attributes['Country Name'],
                ),
                x509.NameAttribute(
                    NameOID.STATE_OR_PROVINCE_NAME,
                    self.ssl_certificate_attributes[
                        'State or Province Name'
                    ],
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
                    self.ssl_certificate_attributes['Common Name'] + 'CA',
                ),
            ]
        )
        # Generate a CA (Certificate Authority) certificate using the given CA key.
        # The certificate includes identifying information such as the subject name, issuer name,
        # public key, serial number, and validity period. It also includes a BasicConstraints extension
        # indicating the certificate is for a CA. Finally, it is signed using the CA's private key.
        ca_certificate = (
            x509.CertificateBuilder()
            .subject_name(subject)
            .issuer_name(issuer)
            .public_key(ca_key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(datetime.datetime.utcnow())
            .not_valid_after(
                datetime.datetime.utcnow()
                + datetime.timedelta(days=365 * 10)
            )
            .add_extension(
                x509.BasicConstraints(ca=True, path_length=None),
                critical=True,
            )
            .sign(ca_key, hashes.SHA256())
        )

        # Write CA certificate to file
        with open(self.ca_cert_path_, "wb") as f:
            f.write(ca_certificate.public_bytes(serialization.Encoding.PEM))

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
    @ca_private_key_path.setter
    def ca_private_key_path(self, path: str) -> None:
        """"""
        self.ca_key_path_ = path

    # ----------------------------------------------------------------------
    def load_ca(self, ca_key_path, ca_cert_path):
        """"""
        self.ca_private_key_path = ca_key_path
        self.ca_certificate_path = ca_cert_path

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
    @ca_certificate_path.setter
    def ca_certificate_path(self, path: str) -> None:
        """
        Set the path to the Certificate Authority (CA) certificate.

        This setter method allows you to update the file path to the CA\'s certificate.
        The specified path will be assigned to the `ca_cert_path_` attribute.

        Parameters
        ----------
        path : str
            The new file path to the CA's certificate.
        """
        self.ca_cert_path_ = path

    # ----------------------------------------------------------------------
    def sign_csr(self, csr_data: bytes) -> bytes:
        """
        Sign a Certificate Signing Request (CSR) with the Certificate Authority (CA) key.

        This method signs a given CSR using the CA's private key and generates a certificate
        that is valid for one year. The signed certificate is returned in PEM format.

        Parameters
        ----------
        csr_data : bytes
            The certificate signing request data in PEM format.

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

        # Load CA key
        ca_key = serialization.load_pem_private_key(
            self.load_certificate(self.ca_private_key_path), password=None
        )

        # Load CA certificate
        ca_certificate = x509.load_pem_x509_certificate(
            self.load_certificate(self.ca_certificate_path)
        )

        # Load client CSR
        csr = x509.load_pem_x509_csr(csr_data)

        # Generate client certificate
        certificate = (
            x509.CertificateBuilder()
            # Set the subject name for the certificate to the subject specified in the CSR (Certificate Signing Request)
            .subject_name(csr.subject)
            # Set the issuer's name to the subject of the CA certificate, essentially making the CA the issuer of this certificate.
            .issuer_name(ca_certificate.subject)
            # Add the public key from the Certificate Signing Request (CSR) to the certificate.
            .public_key(csr.public_key())
            # Generate a random serial number for the certificate to ensure its uniqueness.
            .serial_number(x509.random_serial_number())
            .not_valid_before(
                datetime.datetime.utcnow()
            )  # Set certificate's validity start time to current time
            .not_valid_after(
                datetime.datetime.utcnow()
                + datetime.timedelta(
                    days=365
                )  # Set certificate's expiration time to one year from now
            )
            # Add an extension to specify that this certificate is not for a Certificate Authority (CA)
            .add_extension(
                x509.BasicConstraints(ca=False, path_length=None),
                critical=True,
            )
            # Add the subject alternative name extension with the IP address
            .add_extension(
                x509.SubjectAlternativeName(
                    [x509.IPAddress(self.ip_address)]
                ),
                critical=False,
            )
            # Sign the certificate using the CA's private key with SHA256 as the hashing algorithm
            .sign(ca_key, hashes.SHA256())
        )

        return certificate.public_bytes(serialization.Encoding.PEM)

    # ----------------------------------------------------------------------
    def _key_and_csr(self, name='client') -> tuple:
        """
        Generate a private key and Certificate Signing Request (CSR).

        This method generates a private key and a corresponding CSR for the specified name
        (either 'client' or 'server'). The private key and CSR are saved to files in the
        designated SSL certificates location. The paths to the generated key and CSR files
        are returned.

        Parameters
        ----------
        name : str, optional
            The name to use for generating the key and CSR filenames (default is 'client').

        Returns
        -------
        tuple
            A tuple containing the paths to the generated private key file and CSR file.

        Raises
        ------
        IOError
            If there is an error writing the private key or CSR files to the filesystem.
        """
        private_key_path_ = os.path.join(
            self.ssl_certificates_location, f'{name}_{self.id}.key'
        )

        certificate_path_ = os.path.join(
            self.ssl_certificates_location, f'{name}_{self.id}.csr'
        )

        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)

        # Write client key to file
        with open(private_key_path_, "wb") as f:
            f.write(
                key.private_bytes(
                    encoding=serialization.Encoding.PEM,
                    format=serialization.PrivateFormat.TraditionalOpenSSL,
                    encryption_algorithm=serialization.NoEncryption(),
                )
            )

        # Generate client CSR
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
                            self.ssl_certificate_attributes[
                                'State or Province Name'
                            ],
                        ),
                        x509.NameAttribute(
                            NameOID.LOCALITY_NAME,
                            self.ssl_certificate_attributes['Locality Name'],
                        ),
                        x509.NameAttribute(
                            NameOID.ORGANIZATION_NAME,
                            self.ssl_certificate_attributes[
                                'Organization Name'
                            ],
                        ),
                        x509.NameAttribute(
                            NameOID.COMMON_NAME,
                            self.ssl_certificate_attributes['Common Name'],
                        ),
                    ]
                )
            )
            .sign(key, hashes.SHA256())
        )

        # Write client CSR to file
        with open(certificate_path_, "wb") as f:
            f.write(csr.public_bytes(serialization.Encoding.PEM))

        return private_key_path_, certificate_path_

    # ----------------------------------------------------------------------
    def generate_key_and_csr(self) -> None:
        """
        Generate and store the private keys and Certificate Signing Requests (CSRs).

        This method generates private keys and Certificate Signing Requests (CSRs) for both
        'client' and 'server' entities. The generated keys and CSRs are saved to the filesystem
        in the specified SSL certificates location, and their paths are stored in the instance
        attributes.

        Raises
        ------
        IOError
            If there is an error writing the private key or CSR files to the filesystem.
        """
        self.private_key_client_path_, self.certificate_client_path_ = (
            self._key_and_csr(name='client')
        )
        self.private_key_server_path_, self.certificate_server_path_ = (
            self._key_and_csr(name='server')
        )

    # ----------------------------------------------------------------------
    def load_key_and_csr(
        self,
        private_key_client_path: str,
        certificate_client_path: str,
        private_key_server_path: str,
        certificate_server_path: str,
    ) -> None:
        """
        Load the client's and server's private key and CSR from the specified file paths.

        This method assigns the provided file paths to the corresponding attributes for
        the client's and server's private keys and Certificate Signing Requests (CSRs).
        These files are essential for cryptographic operations such as signing and verifying
        certificates.

        Parameters
        ----------
        private_key_client_path : str
            The file path to the client's private key.
        certificate_client_path : str
            The file path to the client's Certificate Signing Request (CSR).
        private_key_server_path : str
            The file path to the server's private key.
        certificate_server_path : str
            The file path to the server's Certificate Signing Request (CSR).

        Raises
        ------
        FileNotFoundError
            If any of the provided file paths do not point to existing files.
        IOError
            If there is an error reading any of the provided files.
        """
        self.private_key_client_path_ = private_key_client_path
        self.certificate_client_path_ = certificate_client_path
        self.private_key_server_path_ = private_key_server_path
        self.certificate_server_path_ = certificate_server_path

    # ----------------------------------------------------------------------
    @property
    def private_key_paths(self) -> str:
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
        if hasattr(self, 'private_key_client_path_'):
            return {
                'client': self.private_key_client_path_,
                'server': self.private_key_server_path_,
            }
        else:
            raise Exception("Private key path not set")

    # ----------------------------------------------------------------------
    @property
    def certificate_paths(self) -> str:
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
        if hasattr(self, 'certificate_client_path_'):
            return {
                'client': self.certificate_client_path_,
                'server': self.certificate_server_path_,
            }
        else:
            raise Exception("CA certificate path not set")

    # ----------------------------------------------------------------------
    @property
    def certificate_signed_paths(self) -> str:
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
        return {
            'client': self.certificate_paths['client'].replace(
                '.csr', '.cert'
            ),
            'server': self.certificate_paths['server'].replace(
                '.csr', '.cert'
            ),
        }

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
        # Create a default SSL context for the client, specifying that it's intended for server authentication
        ssl_context_client = ssl.create_default_context(
            ssl.Purpose.SERVER_AUTH
        )
        # Load and set the client's certificate and private key for the SSL context
        ssl_context_client.load_cert_chain(
            certfile=self.certificate_signed_paths['client'],
            keyfile=self.private_key_paths['client'],
        )
        # Load and set the Certificate Authority (CA) certificate to verify the client's server certificate
        ssl_context_client.load_verify_locations(
            cafile=self.ca_certificate_path
        )
        # Set the verification mode of the SSL context to require SSL certificates (CERT_REQUIRED).
        ssl_context_client.verify_mode = ssl.CERT_REQUIRED

        # Create a default SSL context for the server, specifying that it's intended for client authentication
        ssl_context_server = ssl.create_default_context(
            ssl.Purpose.CLIENT_AUTH
        )
        # Load and set the server's certificate and private key for the SSL context
        ssl_context_server.load_cert_chain(
            certfile=self.certificate_signed_paths['server'],
            keyfile=self.private_key_paths['server'],
        )
        # Load and set the Certificate Authority (CA) certificate to verify the server's client certificate
        ssl_context_server.load_verify_locations(
            cafile=self.ca_certificate_path
        )
        # Set the verification mode of the SSL context to require SSL certificates (CERT_REQUIRED).
        ssl_context_server.verify_mode = ssl.CERT_REQUIRED

        return ssl_context_client, ssl_context_server
