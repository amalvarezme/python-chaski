import re
import logging
import ipaddress
import asyncio

from chaski.node import ChaskiNode
from chaski.utils.certificate_authority import CertificateAuthority
from chaski.utils.debug import styled_logger

logger_ca = styled_logger(logging.getLogger("ChaskiCA"))


########################################################################
class ChaskiCA(ChaskiNode):
    """"""

    # ----------------------------------------------------------------------
    def __init__(self, *args: tuple, **kwargs: dict):
        """
        Initialize a new instance of ChaskiCA.

        This constructor initializes the ChaskiCA instance by calling the superclass
        initializer with the provided arguments. It then creates a CertificateAuthority
        instance, passing the necessary parameters like the node ID, SSL certificates
        location, and SSL certificate attributes. The setup_certificate_authority method
        is invoked to ensure that the CA's private key and certificate are properly set up.
        """
        super().__init__(reconnections=0, *args, **kwargs)

        pattern = r"(?:(?:\*?\w+@)?(\d{1,3}(?:\.\d{1,3}){3})|(?:\*?\w+@)?\[((?:[0-9a-fA-F]{1,4}:){1,7}[0-9a-fA-F]{1,4})\]):(\d+)"
        ipv4, ipv6, port = re.findall(pattern, f"{self.ip}:{self.port}")[0]

        if ipv4:
            ip_address = ipaddress.IPv4Address(self.ip)
        elif ipv6:
            ip_address = ipaddress.IPv6Address(self.ip)

        self.ca = CertificateAuthority(
            self.id,
            ip_address,
            ssl_certificates_location=self.ssl_certificates_location,
            ssl_certificate_attributes=self.ssl_certificate_attributes,
        )
        # Ensure that the CA's private key and certificate are properly set up
        self.ca.setup_certificate_authority()

    # ----------------------------------------------------------------------
    @property
    def address(self) -> str:
        """
        Get the address of the ChaskiCA instance.

        This property returns the address of the Certificate Authority (CA) node in the format
        "ChaskiCA@ip:port".

        Returns
        -------
        str
            A string representation of the ChaskiCA address.
        """
        return f"ChaskiCA@{self.ip}:{self.port}"

    # ----------------------------------------------------------------------
    async def sign_csr(
        self, csr_data_client: bytes, csr_data_server: bytes, node_id: str
    ) -> dict:
        """
        Sign a Certificate Signing Request (CSR) asynchronously.

        This method signs the provided CSR data for a node identified by `node_id`.
        The signing process involves using the Certificate Authority (CA) capabilities
        to generate a signed certificate which can be used for secure communications.

        Parameters
        ----------
        csr_data : bytes
            CSR data in PEM format to be signed by the Certificate Authority.
        node_id : str
            An identifier for the node for which the CSR is being signed.

        Returns
        -------
        dict
            A dictionary containing the signed certificate and the CA certificate path.
            The dictionary has the following format:
            {
                'signed_csr': bytes,
                'ca_certificate_path': bytes,
            }
        """
        await asyncio.sleep(0)

        logger_ca.debug(
            f"{self.name}: Starting the signing process for CSR data for node '{node_id}'"
        )
        # Sign the provided Certificate Signing Request (CSR) using the Certificate Authority (CA)
        signed_csr_client = self.ca.sign_csr(csr_data_client)
        signed_csr_server = self.ca.sign_csr(csr_data_server)

        # Prepare the dictionary containing the signed certificates for client and server,
        # and the path to the CA certificate. This dictionary will be returned as the result
        # of the CSR signing process.
        data = {
            'signed_csr_client': signed_csr_client,
            'signed_csr_server': signed_csr_server,
            'ca_certificate_path': self.ca.load_certificate(
                self.ca.ca_certificate_path
            ),
        }
        return data
