#!/bin/sh

export SECRETS=$(dd if=/dev/urandom bs=32 count=1 2>/dev/null | openssl base64)
export TZ=UTC
export REDIS_URL=redis://localhost:6379

