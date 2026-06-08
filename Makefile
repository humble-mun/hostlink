.DEFAULT_GOAL := build
.PHONY: tag controller agent build clean push proto

BASE_IMAGE ?= gcr.io/distroless/base-debian13:latest
ARCH ?= amd64
VARIANT ?=
VERSION ?= v1.0
TIMESTAMP := $(shell date '+%Y%m%d%H%M')
TAG ?= $(VERSION)$(shell test "$(ARCH)" != amd64 && echo "-$(ARCH)" || true)$(shell test ! -z $(VARIANT) && echo "-$(VARIANT)" || true)-$(TIMESTAMP)
DEBUG ?= false
REPO ?= harbor-0afe11c0.nip.io/humble-mun/hostlink-controller
DOCKERFILE ?= Dockerfile
GO_VERSION ?= 1.26.3-trixie
BASE_PROJECT ?= github.com/humble-mun/chassis
PROJECT ?= github.com/humble-mun/hostlink
VERSION_PACKAGE ?= pkg/version
GOCACHE_HOST := $(shell go env GOCACHE)
GOPATH_HOST := $(shell go env GOPATH)

ifeq "$(DEBUG)" "false"
LD_FLAGS ?= -w -s
else
GC_FLAGS ?= -gcflags \"all=-N -l\"
endif

tag:
	@echo "TAG=$(TAG)"

clean:
	docker images '$(REPO)*' --format='{{ .Repository }}:{{ .Tag }}' | xargs docker rmi

push:
	docker images --filter=reference="$(REPO)*" --format="{{ .Repository }}:{{ .Tag }}" | xargs -L1 docker push

proto:
	# Regenerate Go stubs from the .proto sources via buf. Requires buf,
	# protoc-gen-go and protoc-gen-go-grpc on PATH; install the plugins with:
	#   go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	#   go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	buf generate

controller:
	docker --debug build --no-cache --platform linux/$(ARCH) \
	--build-arg GO_VERSION="$(GO_VERSION)" \
	--build-arg BASE_IMAGE="$(BASE_IMAGE)" \
	--build-arg BASE_PROJECT="$(BASE_PROJECT)" \
	--build-arg PROJECT="$(PROJECT)" \
	--build-arg VERSION_PACKAGE="$(VERSION_PACKAGE)" \
	--build-arg LD_FLAGS="$(LD_FLAGS)" \
	--build-arg GC_FLAGS="$(GC_FLAGS)" \
	--build-arg ARCH="$(ARCH)" \
	--build-arg VARIANT="$(VARIANT)" \
	-t $(REPO):$(TAG) -f $(DOCKERFILE) .

build: controller

agent:
	# Build the Linux agent binary inside a throwaway golang container.
	# The recipe is interpreted by a POSIX shell (Git Bash sh.exe on Windows,
	# /bin/sh on Linux), so the quoting below is portable across both.
	# NOTE: the container workdir uses a leading // (//go/...) so that MSYS/Git
	# Bash on Windows does not rewrite the absolute path; on Linux // collapses
	# to / and is harmless.
	docker run --rm -i --name builder-$(TIMESTAMP) \
		-e CGO_ENABLED=0 -e GOOS=linux -e GOARCH=$(ARCH) \
		-v "$(GOCACHE_HOST):/root/.cache/go-build" \
		-v "$(GOPATH_HOST)/pkg:/go/pkg" \
		-v "$(CURDIR):/go/src/$(PROJECT)" \
		-w //go/src/$(PROJECT) \
		golang:$(GO_VERSION) \
		sh -c "go build -v -mod=vendor $(GC_FLAGS) \
			-ldflags \"$(LD_FLAGS) \
			-X '$(BASE_PROJECT)/$(VERSION_PACKAGE).CommitID=\`git rev-parse HEAD 2>/dev/null\`' \
			-X '$(BASE_PROJECT)/$(VERSION_PACKAGE).BuiltAt=\`date -u +%Y-%m-%dT%H:%M:%SZ\`' \
			-X '$(BASE_PROJECT)/$(VERSION_PACKAGE).Name=$@' \
			-X '$(BASE_PROJECT)/$(VERSION_PACKAGE).Architecture=$(ARCH)' \
			-X '$(BASE_PROJECT)/$(VERSION_PACKAGE).Variant=$(VARIANT)' \
			-X '$(BASE_PROJECT)/$(VERSION_PACKAGE).RecentCommits=\`git log -n 20 --oneline 2>/dev/null | tr \"\\n\" \";\"\`'\" \
			-o $@.elf $(PROJECT)/cmd/$@"
