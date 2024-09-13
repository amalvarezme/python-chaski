import asyncio
import os
import logging
from chaski.ca import ChaskiCA

logging.basicConfig(level=logging.DEBUG)

# Create the main directory for Chaski CA configuration in the user's home directory if it doesn't already exist
chaski_dir = os.path.join(os.path.expanduser("~"), '.chaski_confluent')
if not os.path.exists(chaski_dir):
    os.mkdir(chaski_dir)

# Define the directory for storing Chaski CA certificates and create it if it does not exist
chaski_ca_dir = os.path.join(chaski_dir, 'ca')
if not os.path.exists(chaski_ca_dir):
    os.mkdir(chaski_ca_dir)


# ----------------------------------------------------------------------
async def run(ip, port, name):
    """"""
    ca = ChaskiCA(
        ip=ip,
        port=port,
        ssl_certificates_location=chaski_ca_dir,
        name=name,
        run=False,
        ssl_certificate_attributes={
            'Country Name': "CO",
            'Locality Name': "Manizales",
            'Organization Name': "DunderLab",
            'State or Province Name': "Caldas",
            'Common Name': "Chaski-Confluent",
        },
    )
    print(f"CA Address: {ca.address}")
    await ca.run()


# ----------------------------------------------------------------------
def main(ip=None, port=65432, name='ChaskiCA'):
    """"""
    asyncio.run(run(ip, port, name))


if __name__ == '__main__':
    main()
