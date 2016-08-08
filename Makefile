project = fastlane
projectpath = ${PWD}
glidepath = ${PWD}/vendor/github.com/Masterminds/glide
keyspath = ${PWD}/files/docker

target:
	@go build

test:
	@go test

integration: test
	@go test -tags=integration

$(glidepath)/glide:
	git clone https://github.com/Masterminds/glide.git $(glidepath)
	cd $(glidepath);make build
	cp $(glidepath)/glide .

libs: $(glidepath)/glide
	$(glidepath)/glide install

deps: libs
