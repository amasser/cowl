#!/bin/bash
set -e

echo "linting... "
make lint
echo "lint OK"
echo "testing... "
make test
echo "tests OK"
echo "building... "
make build
echo "build OK"
