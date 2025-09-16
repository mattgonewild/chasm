#!/usr/bin/env bash
set -euo pipefail
protoc --go_out=. --go_opt=paths=source_relative *.proto