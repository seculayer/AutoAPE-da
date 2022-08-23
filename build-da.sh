#!/bin/bash

VERSION="1.0.0"
MODULE_NAME="da"
REGISTRY_URL="registry.seculayer.com:31500"
GITCONFIG="../gitconfig"
SSL_CERT="../slroot.crt"

if [ -x "$(command -v podman)" ]; then
  function docker { podman "$@"; }
fi

# docker build
DOCKER_BUILDKIT=1 docker build --secret id=gitconfig,src="$GITCONFIG" \
  --secret id=cert,src="${SSL_CERT}" \
  -t $REGISTRY_URL/ape/automl-$MODULE_NAME:$VERSION .
docker push $REGISTRY_URL/ape/automl-$MODULE_NAME:$VERSION
