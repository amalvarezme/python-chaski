#!/bin/bash

# Generate a private key using RSA algorithm and save it to 'private_key.pem' file
openssl genpkey -algorithm RSA -out private_key.pem

# Generate a self-signed certificate valid for 365 days using the previously created private key
openssl req -new -x509 -key private_key.pem -out certificate.pem -days 365
