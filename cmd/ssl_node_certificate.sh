#!/bin/bash

# Generate a private key for the client node using RSA algorithm and save it to 'client_private_key.pem'
openssl genpkey -algorithm RSA -out client_private_key.pem

# Create a Certificate Signing Request (CSR) using the generated private key and save it to 'client_csr.pem'
openssl req -new -key client_private_key.pem -out client_csr.pem
