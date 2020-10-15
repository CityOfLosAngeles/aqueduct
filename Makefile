.PHONY: build publish clean test

BUILDLOG := buildlog.txt
DOCKER := $(if $(DOCKER),$(DOCKER),docker)
DOCKERHUB_REPO := cityofla/ita-data-civis-lab

build: Dockerfile
	$(DOCKER) build . > $(BUILDLOG)

publish: build
	$(eval $@_TAG := $(shell cat $(BUILDLOG) | grep "Successfully built" | tail -n 1 | awk '{ print $$3 }'))
	$(DOCKER) tag $($@_TAG) ${DOCKERHUB_REPO}:$($@_TAG)
	$(DOCKER) push ${DOCKERHUB_REPO}:$($@_TAG)

clean:
	rm -f $(BUILDLOG)
