# docker tag naming
export DOCKER_REGISTRY := env_var_or_default("DOCKER_REGISTRY", "test.l5d.io" )

_docker_tag := ```
    name=${USER//[^[:alnum:].-]/}
    echo "dev-$(git rev-parse --short=8 HEAD)-$name"
```
_image-name := DOCKER_REGISTRY + "/client-policy-controller"

cluster := "l5d-test"

# linkerd CLI stuff
_ctx := "k3d-" + cluster
_rootdir := `pwd`
export INSTALLROOT := _rootdir + "/.linkerd2"
_l5d := INSTALLROOT + "/bin/linkerd" + " --context " + _ctx
_kubectl := "kubectl --context " + _ctx

# lists available recipes
default:
    @just --list

# build a client-policy-controller docker image
docker:
    docker buildx build . \
        --tag="{{ _image-name }}:latest" \
        --load

# load the client-policy-controller image into a k3d cluster
k3d-load: docker
    k3d image import --mode=direct --cluster={{ cluster }} \
        {{ _image-name }}:latest

# run a Linkerd command
linkerd *args: _install-cli
    {{ _l5d }} {{ args }}

# run a kubectl command
kubectl *args: _install-cli
    {{ _kubectl }} {{ args }}

# install the client-policy-controller prototype
install: k3d-load _install-cli install-crds
    {{ _l5d }} inject --manual client-policy-controller.yml \
        | {{ _kubectl }} apply -n linkerd -f -


# set up a k3d cluster
k3d-create:
    #!/usr/bin/env bash
    set -euo pipefail
    set -x

    # make sure a k3d cluster exists
    if ! k3d cluster list {{ cluster }} >/dev/null 2>/dev/null; then
        k3d cluster create {{ cluster }}
    else
        k3d kubeconfig merge {{ cluster }}
    fi

# tear down the k3d cluster
k3d-delete:
    k3d cluster delete {{ cluster }}

# create a k3d cluster and install linkerd
setup: k3d-create && install-linkerd

# install the ClientPolicy CRDs
install-crds:
    {{ _kubectl }} apply -f crds/

# install linkerd in the k3d cluster
install-linkerd: _install-cli
    {{ _l5d }} check --pre
    {{ _l5d }} install  --crds | {{ _kubectl }} apply -f -
    {{ _l5d }} install | {{ _kubectl }} apply -f -
    {{ _l5d }} check

# install the linkerd-viz extension in the k3d cluster
install-viz: _install-cli
    {{ _l5d }} viz install | {{ _kubectl  }} apply -f -
    {{ _l5d }} viz check

# install the booksapp example in the k3d cluster
install-booksapp:
    {{ _kubectl }} create ns booksapp
    {{ _l5d }} inject examples/booksapp/ | {{ _kubectl }} -n booksapp apply -f -
    {{ _kubectl }} -n booksapp rollout status deploy/webapp
    {{ _kubectl }} -n booksapp get po

# install the linkerd cli
_install-cli:
    #!/usr/bin/env bash
    set -euo pipefail

    mkdir -p "$INSTALLROOT"

    if [[ ! -x "${INSTALLROOT}/bin/linkerd" ]]; then
        curl --proto '=https' --tlsv1.2 -sSfL https://run.linkerd.io/install | sh
    fi
