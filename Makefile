all: grunt build

grunt:
	grunt

build:
	env GOOS=linux GOARCH=amd64 go build -o ./dist/druid-plugin_linux_amd64 .
