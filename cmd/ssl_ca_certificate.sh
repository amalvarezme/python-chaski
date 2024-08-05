#!/bin/bash

# Generate a private key for the Certificate Authority (CA) using the RSA algorithm.
# The private key will be saved in a file named 'ca_private_key.pem'.
openssl genpkey -algorithm RSA -out ca_private_key.pem

# Create a root certificate for the Certificate Authority (CA).
# This command generates a certificate request and signs it with the private key.
# The resulting root certificate will be saved in a file named 'ca_certificate.pem' and will be valid for 365 days.
openssl req -x509 -new -nodes -key ca_private_key.pem -sha256 -days 365 -out ca_certificate.pem
