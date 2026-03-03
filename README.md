# framework-codegen-typescript

TypeScript ACTR protoc plugin for Actor-RTC.

This plugin now focuses on generating the ACTR layer only. Generate protobuf
message code with `protoc-gen-es`, then run this plugin to generate remote
`*_client.ts` files plus a unified `local_actor.ts` dispatcher for local
services and remote forwarding.

## Build

```bash
npm install
npm run build
npm run bundle
```

## Usage

```bash
protoc \
  --plugin=protoc-gen-es=./node_modules/.bin/protoc-gen-es \
  --es_out=generated \
  --es_opt=target=ts \
  local.proto remote/echo.proto

protoc \
  --plugin=protoc-gen-actrframework-typescript=./scripts/protoc-gen-actrframework-typescript \
  --actrframework-typescript_out=generated \
  --actrframework-typescript_opt=target=ts,LocalFiles=local.proto,RemoteFiles=remote/echo.proto,RemoteFileMapping=remote/echo.proto=acme+EchoService \
  local.proto remote/echo.proto
```
