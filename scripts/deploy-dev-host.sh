#!/bin/bash

# -----------------------------------------------------------------------------
# Deploy a locally built Hasura GraphQL Engine to a BigBlueButton development
# host over SSH. This script is intended for use on a development machine.
#
# It copies the newly built graphql-engine binary to the target host (first
# argument, user@host), stops the bbb-graphql-server service, replaces the
# binary, and restarts the service. The remote user needs passwordless sudo.
# -----------------------------------------------------------------------------

set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <user@host>" >&2
  exit 1
fi
TARGET="$1"

# Color helpers
GREEN="\e[32m"
BLUE="\e[34m"
RED="\e[31m"
RESET="\e[0m"

ENGINE_BIN=$(find dist-newstyle/build -type f -name graphql-engine -path '*/x/graphql-engine/*' | head -n1)
if [[ -z "$ENGINE_BIN" ]]; then
  echo -e "${RED}graphql-engine binary not found under dist-newstyle/build. Run ./scripts/build-v2.sh first.${RESET}" >&2
  exit 1
fi

echo -e "${BLUE}==> Copying new graphql-engine binary (${ENGINE_BIN})...${RESET}"

scp "$ENGINE_BIN" "${TARGET}:/tmp/graphql-engine"

echo -e "${BLUE}==> Connecting to the target host...${RESET}"

ssh "$TARGET" "
  GREEN='\e[32m'
  BLUE='\e[34m'
  RESET='\e[0m'

  echo -e \"\${BLUE}==> Stopping all bbb-graphql-server instances ...\${RESET}\"
  sudo systemctl stop 'bbb-graphql-server@*.service' || true
  sudo systemctl stop bbb-graphql-server.service || true

  echo -e \"\${BLUE}==> Deploying new graphql-engine binary...\${RESET}\"
  sudo cp /tmp/graphql-engine /usr/bin/hasura-graphql-engine
  sudo chmod +x /usr/bin/hasura-graphql-engine

  echo -e \"\${BLUE}==> Starting all bbb-graphql-server instances ...\${RESET}\"
  sudo systemctl start bbb-graphql-server.service
  sudo systemctl start 'bbb-graphql-server@*.service' || true

  echo -e \"\${GREEN}Deployment completed successfully.\${RESET}\"
"

echo -e "${GREEN}All done!${RESET}"
